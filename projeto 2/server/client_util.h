#ifndef CLIENT_UTIL_H
#define CLIENT_UTIL_H

#include "constants.h"

typedef struct {
    char req_pipe_path[MAX_PIPE_PATH_LENGTH];
    char resp_pipe_path[MAX_PIPE_PATH_LENGTH];
    char notif_pipe_path[MAX_PIPE_PATH_LENGTH];
} client_args;

///  Reads the client pipe arguments from the server pipe.
/// @param fd The file descriptor of the server pipe.
/// @return The client arguments.
client_args produce(int fd);

#endif