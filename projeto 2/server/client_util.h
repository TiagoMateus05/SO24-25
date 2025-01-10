#ifndef CLIENT_UTIL_H
#define CLIENT_UTIL_H

#include "constants.h"
#include "kvs.h"

typedef struct {
    int max_clients;
    int server_fd;
} pool_args;

typedef struct {
    int id;
    char req_pipe_path[MAX_PIPE_PATH_LENGTH];
    char resp_pipe_path[MAX_PIPE_PATH_LENGTH];
    char notif_pipe_path[MAX_PIPE_PATH_LENGTH];
} client_args;

typedef struct{
    KeyNode *notif_table[MAX_NUMBER_SUB * MAX_SESSION_COUNT];
} NotifTable;


///  Reads the client pipe arguments from the server pipe.
/// @param fd The file descriptor of the server pipe.
/// @return The client arguments.
client_args produce(int fd);

void * client_threads();

//Client Queue Functions Prototypes
void client_pool_manager(pool_args * args);
void set_stop_server(int stop);


#endif