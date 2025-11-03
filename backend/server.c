/* server.c — Reader–Writer TCP server (MongoDB, client-pool)
 *
 * Compile:
 *   gcc server.c -o server -pthread $(pkg-config --cflags --libs libmongoc-1.0)
 *
 * Notes:
 * - Expects persistent writer TCP connections to hold the writer lock.
 * - After inserting a message the server sends back an acknowledgement "OK: ..." or "ERROR: ...".
 */

 #define _POSIX_C_SOURCE 200809L
 #include <stdio.h>
 #include <stdlib.h>
 #include <string.h>
 #include <unistd.h>
 #include <arpa/inet.h>
 #include <pthread.h>
 #include <semaphore.h>
 #include <bson/bson.h>
 #include <mongoc/mongoc.h>
 #include <time.h>
 #include <signal.h>
 #include <errno.h>
 
 #define PORT 8080
 #define MAX_CLIENTS 64
 #define BUFFER_SIZE 4096
 
 /* Synchronization */
 static sem_t mutex; /* protects reader_count */
 static sem_t wrt;   /* writer lock */
 static int reader_count = 0;
 
 /* MongoDB pool */
 static mongoc_client_pool_t *mongo_pool = NULL;
 
 static volatile sig_atomic_t running = 1;
 void handle_sigint(int signo) { (void)signo; running = 0; fprintf(stderr, "\n[SERVER] SIGINT received\n"); }
 
 /* Trim trailing newline(s) */
 static void rtrim(char *s) {
     size_t n = strlen(s);
     while (n > 0 && (s[n-1] == '\n' || s[n-1] == '\r')) { s[n-1] = '\0'; n--; }
 }
 
 /* Insert using pool; return dynamically allocated result string (caller must free) */
 char *insert_message_to_db_pool(const char *message) {
     if (!mongo_pool) {
         char *res = strdup("ERROR: no DB pool\n");
         return res;
     }
 
     mongoc_client_t *client = mongoc_client_pool_pop(mongo_pool);
     if (!client) return strdup("ERROR: could not pop client\n");
 
     mongoc_collection_t *coll = mongoc_client_get_collection(client, "chatdb", "chat");
     if (!coll) {
         mongoc_client_pool_push(mongo_pool, client);
         return strdup("ERROR: no collection\n");
     }
 
     bson_t *doc = bson_new();
     BSON_APPEND_UTF8(doc, "message", message);
     BSON_APPEND_DATE_TIME(doc, "timestamp", (int64_t)time(NULL) * 1000);
 
     bson_error_t error;
     if (!mongoc_collection_insert_one(coll, doc, NULL, NULL, &error)) {
         char buf[512];
         snprintf(buf, sizeof(buf), "ERROR: insert failed: %s\n", error.message);
         bson_destroy(doc);
         mongoc_collection_destroy(coll);
         mongoc_client_pool_push(mongo_pool, client);
         return strdup(buf);
     }
 
     bson_destroy(doc);
     mongoc_collection_destroy(coll);
     mongoc_client_pool_push(mongo_pool, client);
 
     char okmsg[256];
     snprintf(okmsg, sizeof(okmsg), "OK: message stored\n");
     return strdup(okmsg);
 }
 
 /* Fetch using client pool (fills provided buffer) */
 void fetch_messages_from_db_pool(char *buffer, size_t buffer_size) {
     if (!mongo_pool) {
         strncpy(buffer, "No DB pool\n", buffer_size - 1);
         buffer[buffer_size - 1] = '\0';
         return;
     }
 
     mongoc_client_t *client = mongoc_client_pool_pop(mongo_pool);
     if (!client) { strncpy(buffer, "DB client unavailable\n", buffer_size - 1); buffer[buffer_size-1]=0; return; }
 
     mongoc_collection_t *coll = mongoc_client_get_collection(client, "chatdb", "chat");
     if (!coll) { strncpy(buffer, "DB collection unavailable\n", buffer_size - 1); buffer[buffer_size-1]=0; mongoc_client_pool_push(mongo_pool, client); return; }
 
     bson_t *query = bson_new();
     bson_t *opts = BCON_NEW("sort", "{", "timestamp", BCON_INT32(1), "}");
     mongoc_cursor_t *cursor = mongoc_collection_find_with_opts(coll, query, opts, NULL);
 
     buffer[0] = '\0';
     const bson_t *doc;
     bson_iter_t iter;
     while (mongoc_cursor_next(cursor, &doc)) {
         const char *msg = "(null)";
         int64_t millis = 0;
         char timestr[64] = {0};
 
         if (bson_iter_init_find(&iter, doc, "message") && BSON_ITER_HOLDS_UTF8(&iter))
             msg = bson_iter_utf8(&iter, NULL);
         if (bson_iter_init_find(&iter, doc, "timestamp") && BSON_ITER_HOLDS_DATE_TIME(&iter))
             millis = bson_iter_date_time(&iter);
 
         time_t sec = millis / 1000;
         struct tm tm;
         localtime_r(&sec, &tm);
         strftime(timestr, sizeof(timestr), "%Y-%m-%d %H:%M:%S", &tm);
 
         char line[1024];
         snprintf(line, sizeof(line), "[%s] %s\n", timestr, msg);
         strncat(buffer, line, buffer_size - strlen(buffer) - 1);
     }
 
     mongoc_cursor_destroy(cursor);
     bson_destroy(query);
     bson_destroy(opts);
     mongoc_collection_destroy(coll);
     mongoc_client_pool_push(mongo_pool, client);
 }
 
 /* Client handler */
 void *handle_client(void *arg) {
     int sock = *(int *)arg;
     free(arg);
     char initial[BUFFER_SIZE];
     ssize_t r = recv(sock, initial, sizeof(initial)-1, 0);
     if (r <= 0) { close(sock); return NULL; }
     initial[r] = '\0';
     rtrim(initial);
 
     /* Determine role robustly */
     char mode[16] = {0};
     if (strncmp(initial, "writer", 6) == 0) strcpy(mode, "writer");
     else if (strncmp(initial, "reader", 6) == 0) strcpy(mode, "reader");
     else {
         // If input had both role and payload in one frame, extract role token
         if (strstr(initial, "writer") != NULL) strcpy(mode, "writer");
         else if (strstr(initial, "reader") != NULL) strcpy(mode, "reader");
     }
 
     if (strcmp(mode, "writer") == 0) {
         printf("[SERVER] Writer connected (sock=%d)\n", sock);
         char buf[BUFFER_SIZE];
         int n;
         int has_lock = 0;
 
         /* If the initial frame contained a payload after 'writer', handle it:
            e.g., "writer\nstart" or "writerstart" etc. */
         char *p_after = NULL;
         if (strlen(initial) > 6) { p_after = initial + 6; while (*p_after==' '||*p_after=='\n'||*p_after=='\r') p_after++; if (*p_after) rtrim(p_after); }
 
         if (p_after && strlen(p_after) > 0) {
             if (strcmp(p_after, "start") == 0) {
                 sem_wait(&wrt);
                 has_lock = 1;
                 send(sock, "OK: writer session started\n", 26, 0);
                 printf("[SERVER] Writer STARTED (sock=%d)\n", sock);
             } else if (strcmp(p_after, "stop") == 0) {
                 // stop without lock => no-op
                 send(sock, "OK: writer session stopped\n", 26, 0);
             } else {
                 // treat as message only if has_lock (it won't yet)
                 if (!has_lock) {
                     send(sock, "ERROR: start writing first\n", 27, 0);
                 } else {
                     char *res = insert_message_to_db_pool(p_after);
                     send(sock, res, strlen(res), 0);
                     free(res);
                 }
             }
         }
 
         /* Persist connection: receive controls and messages while conn open */
         while ((n = recv(sock, buf, sizeof(buf)-1, 0)) > 0) {
             buf[n] = '\0';
             rtrim(buf);
             if (strlen(buf) == 0) continue;
 
             if (strcmp(buf, "start") == 0) {
                 sem_wait(&wrt);
                 has_lock = 1;
                 send(sock, "OK: writer session started\n", 26, 0);
                 printf("[SERVER] Writer STARTED (sock=%d)\n", sock);
             } else if (strcmp(buf, "stop") == 0) {
                 if (has_lock) {
                     has_lock = 0;
                     sem_post(&wrt);
                     send(sock, "OK: writer session stopped\n", 26, 0);
                     printf("[SERVER] Writer STOPPED (sock=%d)\n", sock);
                 } else {
                     send(sock, "ERROR: no active writer session\n", 32, 0);
                 }
             } else if (strcmp(buf, "exit") == 0) {
                 break;
             } else {
                 if (!has_lock) {
                     send(sock, "ERROR: You must start writing first\n", 36, 0);
                     printf("[SERVER] Rejected write (sock=%d, no lock)\n", sock);
                     continue;
                 }
                 /* insert and reply */
                 char *res = insert_message_to_db_pool(buf);
                 send(sock, res, strlen(res), 0);
                 free(res);
             }
         }
 
         if (has_lock) {
             sem_post(&wrt);
             printf("[SERVER] Writer lock auto-released (sock=%d)\n", sock);
         }
 
         printf("[SERVER] Writer disconnected (sock=%d)\n", sock);
         close(sock);
         return NULL;
     }
     else if (strcmp(mode, "reader") == 0) {
         printf("[SERVER] Reader connected (sock=%d)\n", sock);
         sem_wait(&mutex);
         reader_count++;
         if (reader_count == 1) sem_wait(&wrt);
         sem_post(&mutex);
 
         printf("[SERVER] Reader entered critical section (reading messages)...\n");
         char out[BUFFER_SIZE * 8];
         fetch_messages_from_db_pool(out, sizeof(out));
         send(sock, out, strlen(out), 0);
 
         sem_wait(&mutex);
         reader_count--;
         if (reader_count == 0) sem_post(&wrt);
         sem_post(&mutex);
 
         printf("[SERVER] Reader finished and disconnected (sock=%d)\n", sock);
         close(sock);
         return NULL;
     }
     else {
         printf("[SERVER] Unknown role received: %s\n", initial);
         close(sock);
         return NULL;
     }
 }
 
 /* Main */
 int main(void) {
     signal(SIGINT, handle_sigint);
     mongoc_init();
     const char *mongo_uri_env = getenv("MONGO_URI");
     if (!mongo_uri_env) mongo_uri_env = "mongodb://127.0.0.1:27017";
 
     mongoc_uri_t *uri = mongoc_uri_new(mongo_uri_env);
     if (!uri) { fprintf(stderr, "[MongoDB] invalid URI\n"); return EXIT_FAILURE; }
 
     mongo_pool = mongoc_client_pool_new(uri);
     mongoc_uri_destroy(uri);
     if (!mongo_pool) { fprintf(stderr, "[MongoDB] client pool creation failed\n"); mongoc_cleanup(); return EXIT_FAILURE; }
     printf("[MongoDB] Client pool created for %s\n", mongo_uri_env);
 
     if (sem_init(&mutex, 0, 1) != 0) { perror("sem_init mutex"); return EXIT_FAILURE; }
     if (sem_init(&wrt, 0, 1) != 0) { perror("sem_init wrt"); return EXIT_FAILURE; }
 
     int server_fd = socket(AF_INET, SOCK_STREAM, 0);
     if (server_fd < 0) { perror("socket"); return EXIT_FAILURE; }
 
     int opt = 1;
     setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
     struct sockaddr_in addr = {0};
     addr.sin_family = AF_INET; addr.sin_port = htons(PORT); addr.sin_addr.s_addr = INADDR_ANY;
 
     if (bind(server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) { perror("bind"); return EXIT_FAILURE; }
     if (listen(server_fd, MAX_CLIENTS) < 0) { perror("listen"); return EXIT_FAILURE; }
 
     printf("=========================================\n");
     printf(" Reader–Writer Server with MongoDB Ready\n");
     printf(" Listening on port %d\n", PORT);
     printf("=========================================\n");
 
     while (running) {
         struct sockaddr_in client_addr; socklen_t client_len = sizeof(client_addr);
         int client = accept(server_fd, (struct sockaddr *)&client_addr, &client_len);
         if (client < 0) {
             if (errno == EINTR && !running) break;
             perror("accept"); continue;
         }
         int *pclient = malloc(sizeof(int));
         if (!pclient) { close(client); continue; }
         *pclient = client;
         pthread_t tid;
         if (pthread_create(&tid, NULL, handle_client, pclient) != 0) {
             perror("pthread_create"); close(client); free(pclient); continue;
         }
         pthread_detach(tid);
     }
 
     close(server_fd);
     if (mongo_pool) mongoc_client_pool_destroy(mongo_pool);
     mongoc_cleanup();
     sem_destroy(&mutex); sem_destroy(&wrt);
     printf("[SERVER] Shutdown complete.\n");
     return 0;
 }
 