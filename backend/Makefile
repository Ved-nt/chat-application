CC = gcc
CFLAGS = -Wall -O2 -pthread
PKG = $(shell pkg-config --cflags --libs libmongoc-1.0)
TARGET = server
CLIENT = client

all: $(TARGET) $(CLIENT)

server: server.c
	$(CC) $(CFLAGS) server.c -o server $(PKG)

client: client.c
	$(CC) client.c -o client

clean:
	rm -f server client
