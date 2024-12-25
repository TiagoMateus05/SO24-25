#include "api.h"

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "../common/constants.h"
#include "../common/protocol.h"

int req_pipe_fd;
int resp_pipe_fd;
int notif_pipe_fd;
char const* req_pipe;
char const* resp_pipe;
char const* notif_pipe;

int kvs_connect(char const* req_pipe_path, char const* resp_pipe_path, char const* server_pipe_path,
                char const* notif_pipe_path, int* notif_pipe) {
  req_pipe = req_pipe_path;
  resp_pipe = resp_pipe_path;
  notif_pipe = notif_pipe_path;

  open_pipe(req_pipe_path, PIPE_PERMS);
  open_pipe(resp_pipe_path, PIPE_PERMS);
  open_pipe(notif_pipe_path, PIPE_PERMS);

  int server_fd = safe_open(server_pipe_path, O_WRONLY);

  char message[1 + 3 * MAX_PIPE_PATH_LENGTH];
  message[0] = OP_CONNECT;
  strncpy(message + 1, req_pipe_path, MAX_PIPE_PATH_LENGTH);
  strncpy(message + 1 + MAX_PIPE_PATH_LENGTH, resp_pipe_path, MAX_PIPE_PATH_LENGTH);
  strncpy(message + 1 + 2 * MAX_PIPE_PATH_LENGTH, notif_pipe_path, MAX_PIPE_PATH_LENGTH);

  write_all(server_fd, message, 1 + 3 * MAX_PIPE_PATH_LENGTH);

  safe_close(server_fd);

  req_pipe_fd = safe_open(req_pipe_path, O_WRONLY);
  resp_pipe_fd = safe_open(resp_pipe_path, O_RDONLY);
  notif_pipe = safe_open(notif_pipe_path, O_RDONLY);

  char buffer[2];
  read_all(resp_pipe_fd, buffer, 2, NULL);
  if (buffer[0] != OP_CONNECT || buffer[1] != 0) {
    fprintf(stderr, "Error connecting to server\n"); //TODO
    return 1;
  }

  fprintf(stdout, "Connected to server\n"); //TODO

  return 0;
}
 
int kvs_disconnect(void) {
  char code = OP_DISCONNECT;
  write_all(req_pipe_fd, &code, sizeof(code));

  char buffer[2];
  read_all(resp_pipe_fd, buffer, 1, NULL);
  if (buffer[0] != OP_DISCONNECT|| buffer[1] != 0) {
    fprintf(stderr, "Error disconnecting from server\n"); //TODO
    return 1;
  }

  safe_close(req_pipe_fd);
  safe_close(resp_pipe_fd);
  safe_cose(notif_pipe_fd);

  safe_unlink(req_pipe);
  safe_unlink(resp_pipe);
  safe_unlink(notif_pipe);

  return 0;
}

int kvs_subscribe(const char* key) {
  char code = OP_SUBSCRIBE;
  int key_length = strlen(key);
  char message[1 + key_length];
  message[0] = code;
  strncpy(message + 1, key, key_length);

  write_all(req_pipe_fd, message, 1 + key_length);

  char buffer[2];
  read_all(resp_pipe_fd, buffer, 2, NULL);
  if (buffer[0] != OP_SUBSCRIBE || buffer[1] != 0) {
    fprintf(stderr, "Error subscribing to key %s\n", key); //TODO
    return 1;
  }

  fprintf(stdout, "Subscribed to key %s\n", key); //TODO
  
  return 0;
}

int kvs_unsubscribe(const char* key) {
  char code = OP_UNSUBSCRIBE;
  int key_length = strlen(key);
  char message[1 + key_length];
  message[0] = code;
  strncpy(message + 1, key, key_length);

  write_all(req_pipe_fd, message, 1 + key_length);

  char buffer[2];
  read_all(resp_pipe_fd, buffer, 2, NULL);
    if (buffer[0] != OP_UNSUBSCRIBE || buffer[1] != 0) {
    fprintf(stderr, "Error unsubscribing from key %s\n", key); //TODO
    return 1;
  }

  fprintf(stdout, "Unsubscribed from key %s\n", key); //TODO

  return 0;
}


