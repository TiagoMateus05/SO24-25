#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <semaphore.h>

#include "parser.h"
#include "../client/api.h"
#include "../common/constants.h"
#include "../common/io.h"
#include "../common/protocol.h"

int disconnect_flag = 0;
int server_disconnected = 0;

void *notifications_thread(void *arg) {
  int *notif_fd = (int *)arg;

  while (!disconnect_flag && !server_disconnected) {
    char pair[MAX_STRING_SIZE + 1][MAX_STRING_SIZE + 1] = {{'\0'}};

    if (read_all(*notif_fd, pair[0], MAX_STRING_SIZE + 1, NULL) <= 0 && !disconnect_flag) {
        server_disconnected = 1;
        free(notif_fd);
        server_disconnected_gracefully();
        fprintf(stderr, "Disconnected forcefully from server\n");
        // client is disconnected forcefully because the server disconnected
        exit(1);
        break;
    }

    if (read_all(*notif_fd, pair[1], MAX_STRING_SIZE + 1, NULL) <= 0 && !disconnect_flag) {
        server_disconnected = 1;
        free(notif_fd);
        server_disconnected_gracefully();
        fprintf(stderr, "Disconnected forcefully from server\n");
        // client is disconnected forcefully because the server disconnected
        exit(1);
        break;
    }

    fprintf(stdout, "Notification: (%s,%s)\n", pair[0], pair[1]);
  }
  fprintf(stdout, "Notifications thread exiting\n");
  return NULL;
}

int main(int argc, char* argv[]) {
  if (argc < 3) {
    fprintf(stderr, "Usage: %s <client_unique_id> <register_pipe_path>\n", argv[0]);
    return 1;
  }

  char req_pipe_path[256] = "/tmp/al97_req_";
  char resp_pipe_path[256] = "/tmp/al97_resp_";
  char notif_pipe_path[256] = "/tmp/al97_notif_";

  char keys[MAX_NUMBER_SUB][MAX_STRING_SIZE] = {0};
  unsigned int delay_ms;
  size_t num;
  strncat(req_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));
  strncat(resp_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));
  strncat(notif_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));

  printf("Waiting for server access...\n");

  int *notif_fd = malloc(sizeof(int)); 
  
  if (kvs_connect(req_pipe_path, resp_pipe_path, argv[2], notif_pipe_path, notif_fd) != 0) {
    fprintf(stderr, "Failed to connect to the server\n");
    return 1;
  }
  fprintf(stdout, "Connected to server\n");

  pthread_t notif_thread;
  if (pthread_create(&notif_thread, NULL, notifications_thread, notif_fd) != 0) {
    fprintf(stderr, "Failed to create notifications thread\n");
    return 1;
  }

  while (!server_disconnected) {
    switch (get_next(STDIN_FILENO)) {
      case CMD_DISCONNECT:
        pthread_cancel(notif_thread);
        if (kvs_disconnect() != 0) {
          fprintf(stderr, "Failed to disconnect to the server\n");
          return 1;
        }
        disconnect_flag = 1;
        if (pthread_join(notif_thread, NULL) != 0) {
          fprintf(stderr, "Failed to join notifications thread\n");
          safe_close(*notif_fd);
          safe_unlink(notif_pipe_path);
          free(notif_fd);
          return 1;
        }
        safe_close(*notif_fd);
        safe_unlink(notif_pipe_path);
        free(notif_fd);

        //releases the semaphore and closes it
        return 0;

      case CMD_SUBSCRIBE:
        num = parse_list(STDIN_FILENO, keys, 1, MAX_STRING_SIZE);
        if (num == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }
         
        if (kvs_subscribe(keys[0])) {
            fprintf(stderr, "Command subscribe failed\n");
        }

        break;

      case CMD_UNSUBSCRIBE:
        num = parse_list(STDIN_FILENO, keys, 1, MAX_STRING_SIZE);
        if (num == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }
         
        if (kvs_unsubscribe(keys[0])) {
            fprintf(stderr, "Command subscribe failed\n");
        }

        break;

      case CMD_DELAY:
        if (parse_delay(STDIN_FILENO, &delay_ms) == -1) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (delay_ms > 0) {
            printf("Waiting...\n");
            delay(delay_ms);
        }
        break;

      case CMD_INVALID:
        fprintf(stderr, "Invalid command. See HELP for usage\n");
        break;

      case CMD_EMPTY:
        break;

      case EOC:
        // input should end in a disconnect, or it will loop here forever
        break;
    }
  }
  return 0;
}