#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <semaphore.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "../common/constants.h"
#include "../common/io.h"
#include "client_util.h"
#include "operations.h"

int max_clients;
int stop_server = 0;

pthread_t *client_worker_threads;
sem_t *sem_empty;
sem_t *sem_full;
pthread_mutex_t mutex;

client_args BUFFER_PC;

client_args active_clients[MAX_SESSION_COUNT];
pthread_mutex_t active_clients_mutex;

int sigusr1_flag = false;

void producer(client_args args) {
  if (sigusr1_flag) {
    return;
  }
  sem_wait(sem_empty);
  pthread_mutex_lock(&mutex);

  BUFFER_PC = args;

  pthread_mutex_unlock(&mutex);
  sem_post(sem_full);
}

client_args consumer() {
  sem_wait(sem_full);
  pthread_mutex_lock(&mutex);

  client_args args = BUFFER_PC;

  pthread_mutex_unlock(&mutex);
  sem_post(sem_empty);

  return args;
}

void init_worker_threads() {
  client_worker_threads = malloc((unsigned long)max_clients * sizeof(pthread_t));

  sem_empty = sem_open("/sem_empty", O_CREAT, 0644, 1);
  sem_full = sem_open("/sem_full", O_CREAT, 0644, 0);

  pthread_mutex_init(&mutex, NULL);
  pthread_mutex_init(&active_clients_mutex, NULL);

  for (int i = 0; i < MAX_SESSION_COUNT; i++) {
    memset(active_clients[i].req_pipe_path, 0, sizeof(active_clients[i].req_pipe_path));
    memset(active_clients[i].resp_pipe_path, 0, sizeof(active_clients[i].resp_pipe_path));
    memset(active_clients[i].notif_pipe_path, 0, sizeof(active_clients[i].notif_pipe_path));
    active_clients[i].resp_pipe_fd = -1;
    active_clients[i].req_pipe_fd = -1;
    active_clients[i].notif_pipe_fd = -1;
  }

  for (int i = 0; i < max_clients; i++) {
    pthread_create(&client_worker_threads[i], NULL, client_threads, NULL);
  }
}

void close_worker_threads() {
  for (int i = 0; i < max_clients; i++) {
    pthread_join(client_worker_threads[i], NULL);
  }

  free(client_worker_threads);
  pthread_mutex_destroy(&mutex);

  sem_close(sem_empty);
  sem_close(sem_full);

  sem_unlink("/sem_empty");
  sem_unlink("/sem_full");
}

client_args produce(int fd) {
  printf("Reading client arguments\n");
  client_args ret;
  int res = 0;

  char code;
  res = read_all(fd, &code, sizeof(char), &sigusr1_flag);
  if (res <= 1 && sigusr1_flag) {
    return ret;
  }
  else if (res == -1) {
    fprintf(stderr, "Failed to read from server pipe\n");
    exit(1);
  }
  
  char req_pipe_path[MAX_PIPE_PATH_LENGTH];
  char resp_pipe_path[MAX_PIPE_PATH_LENGTH];
  char notif_pipe_path[MAX_PIPE_PATH_LENGTH];

  res = read_all(fd, req_pipe_path, MAX_PIPE_PATH_LENGTH, &sigusr1_flag);
  if (res == -1) {
    fprintf(stderr, "Failed to read from server pipe\n");
    exit(1);
  }

  res = read_all(fd, resp_pipe_path, MAX_PIPE_PATH_LENGTH, &sigusr1_flag);
  if (res == -1) {
    fprintf(stderr, "Failed to read from server pipe\n");
    exit(1);
  }

  res = read_all(fd, notif_pipe_path, MAX_PIPE_PATH_LENGTH, &sigusr1_flag);
  if (res == -1) {
    fprintf(stderr, "Failed to read from server pipe\n");
    exit(1);
  }

  strncpy(ret.req_pipe_path, req_pipe_path, MAX_PIPE_PATH_LENGTH);
  strncpy(ret.resp_pipe_path, resp_pipe_path, MAX_PIPE_PATH_LENGTH);
  strncpy(ret.notif_pipe_path, notif_pipe_path, MAX_PIPE_PATH_LENGTH);

  return ret;
}

void *client_threads() {
  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGUSR1);
  if (pthread_sigmask(SIG_BLOCK, &set, NULL) != 0) {
    fprintf(stderr, "Failed to block SIGUSR1\n");
    exit(1);
  }

  while (!stop_server) {
    int index = 0;
    client_args client = consumer();
    fprintf(stderr, "Client\n");

    int req_fd = safe_open(client.req_pipe_path, O_RDONLY);
    int resp_fd = safe_open(client.resp_pipe_path, O_WRONLY);
    int notif_fd = safe_open(client.notif_pipe_path, O_WRONLY);

    pthread_mutex_lock(&active_clients_mutex);
    for (int i = 0; i < MAX_SESSION_COUNT; i++) {
      fprintf(stderr, "Checking client %d\n", active_clients[i].req_pipe_fd);
      if (active_clients[i].req_pipe_fd < 0) {
        strcpy(active_clients[i].req_pipe_path, client.req_pipe_path);
        strcpy(active_clients[i].resp_pipe_path, client.resp_pipe_path);
        strcpy(active_clients[i].notif_pipe_path, client.notif_pipe_path);
        active_clients[i].req_pipe_fd = req_fd;
        active_clients[i].resp_pipe_fd = resp_fd;
        active_clients[i].notif_pipe_fd = notif_fd;
        memset(active_clients[i].keys, 0, sizeof(active_clients[i].keys));
        index = i;
        break;
      }
    }
    fprintf(stderr, "Client %d connected\n", index);
    pthread_mutex_unlock(&active_clients_mutex);

    char code = OP_CONNECT;
    char res = 0;
    if (write_all(active_clients[index].resp_pipe_fd, &code, sizeof(char)) == -1) {
      fprintf(stderr, "Failed to write to client\n");
      exit(1);
    }
    if (write_all(active_clients[index].resp_pipe_fd, &res, sizeof(char)) == -1) {
      fprintf(stderr, "Failed to write to client\n");
      exit(1);
    }

    char key[MAX_STRING_SIZE + 1] = {'\0'};
    int ret = 0;

    while (!stop_server) {
      fprintf(stderr, "Client %d waiting for request\n", req_fd);
      ret = read_all(active_clients[index].req_pipe_fd, &code, sizeof(char), NULL);
      fprintf(stderr, "Client %d : %d read code %d\n", active_clients[index].req_pipe_fd, active_clients[index].req_pipe_fd, code);
      if (ret == -1) {
        fprintf(stderr, "Failed to read from client\n");
        exit(1);
      } else if (ret == -2) {
        printf("Client %d disconnected\n", index);
        break;
      }

      if (code == OP_DISCONNECT) {
        break;
      }

      ret = read_all(active_clients[index].req_pipe_fd, key, MAX_STRING_SIZE + 1, NULL);
      if (ret == -1) {
        fprintf(stderr, "Failed to read from client\n");
        exit(1);
      } else if (ret == -2) {
        printf("Client %d disconnected\n", index);
        break;
      }
      

      switch (code) {
        case OP_SUBSCRIBE:
          for (int i = 0; i < MAX_NUMBER_SUB; i++) {
            if (active_clients[index].keys[i][0] == '\0') {
              strncpy(active_clients[index].keys[i], key, MAX_STRING_SIZE);
              break;
            }
          }
          res = kvs_subscribe(key, notif_fd);
          break;
        case OP_UNSUBSCRIBE:
          for (int i = 0; i < MAX_NUMBER_SUB; i++) {
            if (strcmp(active_clients[index].keys[i], key) == 0) {
              memset(active_clients[index].keys[i], '\0', MAX_STRING_SIZE);
              break;
            }
          }
          res = kvs_unsubscribe(key, notif_fd);
          break;
        default:
          fprintf(stderr, "Unknown operation code: %d\n", code);
          res = 1;
          break;
      }

      ret = write_all(active_clients[index].resp_pipe_fd, &code, sizeof(char));
      if (ret == -1) {
        fprintf(stderr, "Failed to write to client\n");
        exit(1);
      }

      ret = write_all(active_clients[index].resp_pipe_fd, &res, sizeof(char));
      if (ret == -1) {
        fprintf(stderr, "Failed to write to client\n");
        exit(1);
      }
      printf("Code: %d\n", code);
      memset(key, '\0', MAX_STRING_SIZE + 1);
    }

    fprintf(stderr, "Client %d ret\n", ret);
    if (ret >= 0) {
      kvs_disconnect_client(active_clients[index].keys, notif_fd);
      code = OP_DISCONNECT;
      res = 0;  
      write_all(active_clients[index].resp_pipe_fd, &code, sizeof(char));
      write_all(active_clients[index].resp_pipe_fd, &res, sizeof(char));

      safe_close(active_clients[index].req_pipe_fd);
      safe_close(active_clients[index].resp_pipe_fd);
      safe_close(active_clients[index].notif_pipe_fd);

      pthread_mutex_lock(&active_clients_mutex);
      memset(active_clients[index].req_pipe_path, 0, sizeof(active_clients[index].req_pipe_path));
      memset(active_clients[index].resp_pipe_path, 0, sizeof(active_clients[index].resp_pipe_path));
      memset(active_clients[index].notif_pipe_path, 0, sizeof(active_clients[index].notif_pipe_path));
      active_clients[index].req_pipe_fd = -1;
      active_clients[index].resp_pipe_fd = -1;
      active_clients[index].notif_pipe_fd = -1;
      pthread_mutex_unlock(&active_clients_mutex);   
    }
  }
  return NULL;
}


void client_pool_manager(pool_args * args) {
  pool_args *pool = (pool_args *) args;
  max_clients = (int)pool->max_clients;
  int server_reg_path = pool->server_fd;

  init_worker_threads();

  while (!stop_server) {
    if (sigusr1_flag) {
      fprintf(stderr, "Received SIGUSR1 and Handling\n");
      for (int i = 0; i < MAX_SESSION_COUNT; i++) {
        if (active_clients[i].req_pipe_fd != -1) {
          fprintf(stderr, "Disconnecting client %d\n", i);
          kvs_disconnect_client(active_clients[i].keys, 
                                active_clients[i].notif_pipe_fd);
          close(active_clients[i].req_pipe_fd);
          close(active_clients[i].resp_pipe_fd);
          close(active_clients[i].notif_pipe_fd);

          fprintf(stderr, "Close FD %d\n", active_clients[i].req_pipe_fd);

          safe_unlink(active_clients[i].req_pipe_path);
          safe_unlink(active_clients[i].resp_pipe_path);
          safe_unlink(active_clients[i].notif_pipe_path);
          memset(active_clients[i].req_pipe_path, 0, sizeof(active_clients[i].req_pipe_path));
          memset(active_clients[i].resp_pipe_path, 0, sizeof(active_clients[i].resp_pipe_path));
          memset(active_clients[i].notif_pipe_path, 0, sizeof(active_clients[i].notif_pipe_path));
          active_clients[i].req_pipe_fd = -1;
          active_clients[i].resp_pipe_fd = -1;
          active_clients[i].notif_pipe_fd = -1;
        }
      }
      sigusr1_flag = 0;
    }
    if (!sigusr1_flag){
      client_args client = produce(server_reg_path);
      producer(client);
    }
  }
}

void set_stop_server(int stop) {
  stop_server = stop;
}

void sig_handler(int sig) {
  fprintf(stderr, "Received SIGUSR1\n");
  sigusr1_flag = true;
  if (signal(sig, sig_handler) == SIG_ERR) {
    fprintf(stderr, "Failed to set signal handler\n");
  }

  return;
}