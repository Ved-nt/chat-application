#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>

#define PORT 8080
#define BUFFER_SIZE 1024

int main() {
    int sock = 0;
    struct sockaddr_in serv_addr;
    char buffer[BUFFER_SIZE] = {0};
    char mode[10];

    printf("Enter mode (reader/writer): ");
    scanf("%s", mode);
    getchar(); // clear newline

    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        printf("\nSocket creation error\n");
        return -1;
    }

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(PORT);

    if (inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr) <= 0) {
        printf("\nInvalid address\n");
        return -1;
    }

    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        printf("\nConnection Failed\n");
        return -1;
    }

    send(sock, mode, sizeof(mode), 0);

    if (strcmp(mode, "writer") == 0) {
        printf("You are Writer. Type messages (type 'exit' to quit)\n");
        while (1) {
            printf("Enter message: ");
            fgets(buffer, sizeof(buffer), stdin);
            buffer[strcspn(buffer, "\n")] = 0;
            send(sock, buffer, strlen(buffer), 0);
            if (strcmp(buffer, "exit") == 0)
                break;
        }
    } else {
        printf("You are Reader. Waiting for data...\n");
        int bytes = recv(sock, buffer, sizeof(buffer), 0);
        buffer[bytes] = '\0';
        printf("\n--- Chat Messages ---\n%s\n", buffer);
    }

    close(sock);
    return 0;
}