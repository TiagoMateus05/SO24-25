#include "api.h"

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "../common/constants.h"
#include "../common/io.h"
#include "../common/protocol.h"

int req_pipe_fd;
int resp_pipe_fd;
int notif_pipe_fd;
char const* req_pipe;
char const* resp_pipe;
char const* notif_pipe;

int kvs_connect(char const* req_pipe_path, char const* resp_pipe_path, char const* server_pipe_path,
                char const* notif_pipe_path, int* notifications_fd) {
  req_pipe = req_pipe_path;
  resp_pipe = resp_pipe_path;
  notif_pipe = notif_pipe_path;

  open_fifo(req_pipe_path, PIPE_PERMS);
  open_fifo(resp_pipe_path, PIPE_PERMS);
  open_fifo(notif_pipe_path, PIPE_PERMS);

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
  notif_pipe_fd = safe_open(notif_pipe_path, O_RDONLY);

  *notifications_fd = notif_pipe_fd;

  char code, res;
  read_all(resp_pipe_fd, &code, sizeof(char), NULL);
  read_all(resp_pipe_fd, &res, sizeof(char), NULL);

  if (code != OP_CONNECT || res != 0) {
    fprintf(stderr, "Error connecting to server\n"); 
    return 1;
  }

  fprintf(stdout, "Server returned <%d> for operation: <connect>\n", res);

  return 0;
}
 
int kvs_disconnect(void) {
  char code = OP_DISCONNECT;
  write_all(req_pipe_fd, &code, sizeof(code));

  code = '\0';
  char res;
  read_all(resp_pipe_fd, &code, sizeof(char), NULL);
  read_all(resp_pipe_fd, &res, sizeof(char), NULL);

  fprintf(stdout, "Server returned <%d> for operation: <disconnect>\n", res);

  if (code != OP_DISCONNECT || res != 0) {
    fprintf(stderr, "Error disconnecting from server\n"); 
    return 1;
  }

  safe_close(req_pipe_fd);
  safe_close(resp_pipe_fd);

  safe_unlink(req_pipe);
  safe_unlink(resp_pipe);

  return 0;
}

int kvs_subscribe(const char* key) {
  char code = OP_SUBSCRIBE;
  size_t key_length = strlen(key);
  char message[1 + key_length];
  message[0] = code;
  strncpy(message + 1, key, key_length);

  write_all(req_pipe_fd, message, 1 + key_length);

  code = '\0';
  char res;
  read_all(resp_pipe_fd, &code, sizeof(char), NULL);
  read_all(resp_pipe_fd, &res, sizeof(char), NULL);

  if (code != OP_SUBSCRIBE || res != 0) {
    fprintf(stderr, "Error subscribing to key %s\n", key); 
    return 1;
  }

  fprintf(stdout, "Server returned <%d> for operation: <subscribe>\n", res);
  
  return 0;
}

int kvs_unsubscribe(const char* key) {
  char code = OP_UNSUBSCRIBE;
  size_t key_length = strlen(key);
  char message[1 + key_length];
  message[0] = code;
  strncpy(message + 1, key, key_length);

  write_all(req_pipe_fd, message, 1 + key_length);

  code = '\0';
  char res;
  read_all(resp_pipe_fd, &code, sizeof(char), NULL);
  read_all(resp_pipe_fd, &res, sizeof(char), NULL);
  if (code != OP_UNSUBSCRIBE || res != 0) {
    fprintf(stderr, "Error unsubscribing from key %s\n", key); 
    return 1;
  }

  fprintf(stdout, "Server returned <%d> for operation: <unsubscribe>\n", res);

  return 0;
}


