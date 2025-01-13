#ifndef CLIENT_UTIL_H
#define CLIENT_UTIL_H

#include "constants.h"
#include "kvs.h"

typedef struct {
    int max_clients;
    int server_fd;
} pool_args;

typedef struct {
    char req_pipe_path[MAX_PIPE_PATH_LENGTH];
    char resp_pipe_path[MAX_PIPE_PATH_LENGTH];
    char notif_pipe_path[MAX_PIPE_PATH_LENGTH];
    int req_pipe_fd;
    int resp_pipe_fd;
    int notif_pipe_fd;
    char keys[MAX_NUMBER_SUB][MAX_STRING_SIZE];
} client_args;

///  Reads the client pipe arguments from the server pipe.
/// @param fd The file descriptor of the server pipe.
/// @return The client arguments.
client_args produce(int fd);

void * client_threads();

// Client Queue Functions Prototypes
void client_pool_manager(pool_args * args);
void set_stop_server(int stop);

void sig_handler(int sig);

#endif