#include "client_util.h"

#include <stdio.h>
#include <string.h>

#include "../common/io.h"

client_args produce(int fd) {
  printf("Reading client arguments\n");
  client_args ret;

  char code;
  // TODO: change NULL for safe reading when signal handling is implemented
  read_all(fd, &code, sizeof(char), NULL);
  
  char req_pipe_path[MAX_PIPE_PATH_LENGTH];
  char resp_pipe_path[MAX_PIPE_PATH_LENGTH];
  char notif_pipe_path[MAX_PIPE_PATH_LENGTH];

  read_all(fd, req_pipe_path, MAX_PIPE_PATH_LENGTH, NULL);
  read_all(fd, resp_pipe_path, MAX_PIPE_PATH_LENGTH, NULL);
  read_all(fd, notif_pipe_path, MAX_PIPE_PATH_LENGTH, NULL);

  strncpy(ret.req_pipe_path, req_pipe_path, MAX_PIPE_PATH_LENGTH);
  strncpy(ret.resp_pipe_path, resp_pipe_path, MAX_PIPE_PATH_LENGTH);
  strncpy(ret.notif_pipe_path, notif_pipe_path, MAX_PIPE_PATH_LENGTH);

  return ret;
}