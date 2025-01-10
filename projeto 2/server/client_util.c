#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <semaphore.h>
#include <pthread.h>
#include <stdlib.h>
#include <errno.h>

#include "client_util.h"
#include "../common/constants.h"
#include "operations.h"


#include "../common/io.h"

int max_clients;

int stop_server = 0;

pthread_t *client_worker_threads;
sem_t *sem_empty;
sem_t *sem_full;
pthread_mutex_t mutex;

client_args BUFFER_PC;

void producer(client_args args) {
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

  char code;
  // TODO: change NULL for safe reading when signal handling is implemented
  read_all(fd, &code, sizeof(char), NULL);
  
  char req_pipe_path[MAX_PIPE_PATH_LENGTH];
  char resp_pipe_path[MAX_PIPE_PATH_LENGTH];
  char notif_pipe_path[MAX_PIPE_PATH_LENGTH];

  read_all(fd, req_pipe_path, MAX_PIPE_PATH_LENGTH, NULL);
  read_all(fd, resp_pipe_path, MAX_PIPE_PATH_LENGTH, NULL);
  read_all(fd, notif_pipe_path, MAX_PIPE_PATH_LENGTH, NULL);
  ret.id = (int) req_pipe_path[strlen(req_pipe_path) - 1];

  strncpy(ret.req_pipe_path, req_pipe_path, MAX_PIPE_PATH_LENGTH);
  strncpy(ret.resp_pipe_path, resp_pipe_path, MAX_PIPE_PATH_LENGTH);
  strncpy(ret.notif_pipe_path, notif_pipe_path, MAX_PIPE_PATH_LENGTH);


  return ret;
}

void * client_threads() {
  char keys[MAX_NUMBER_SUB][MAX_STRING_SIZE] = {0};
  while (!stop_server) {
    client_args client = consumer();


    int req_fd = safe_open(client.req_pipe_path, O_RDONLY);
    int resp_fd = safe_open(client.resp_pipe_path, O_WRONLY);
    int notif_fd = safe_open(client.notif_pipe_path, O_WRONLY);

    char code = OP_CONNECT;
    char res = 0;
    write_all(resp_fd, &code, sizeof(char));
    write_all(resp_fd, &res, sizeof(char));

    char key[MAX_STRING_SIZE + 1] = {'\0'};

    while (!stop_server) {
      read_all(req_fd, &code, sizeof(char), NULL);

      if (code == OP_DISCONNECT) {
        break;
      }

      read_all(req_fd, key, MAX_STRING_SIZE + 1, NULL);

      switch (code) {
        case OP_SUBSCRIBE:
          for (int i = 0; i < MAX_NUMBER_SUB; i++) {
            if (keys[i][0] == '\0') {
              strncpy(keys[i], key, MAX_STRING_SIZE);
              break;
            }
          }
          res = kvs_subscribe(key, notif_fd);
          break;
        case OP_UNSUBSCRIBE:
          for (int i = 0; i < MAX_NUMBER_SUB; i++) {
            if (strcmp(keys[i], key) == 0) {
              memset(keys[i], '\0', MAX_STRING_SIZE);
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

      write_all(resp_fd, &code, sizeof(char));
      write_all(resp_fd, &res, sizeof(char));

      memset(key, '\0', MAX_STRING_SIZE + 1);
    }

    kvs_disconnect_client(keys, notif_fd);

    code = OP_DISCONNECT;
    res = 0;
    write_all(resp_fd, &code, sizeof(char));
    write_all(resp_fd, &res, sizeof(char));

    safe_close(req_fd);
    safe_close(resp_fd);
    safe_close(notif_fd);

  }
  return NULL;
}


void client_pool_manager(pool_args * args) {
  pool_args *pool = (pool_args *) args;
  max_clients = (int)pool->max_clients;
  int server_reg_path = pool->server_fd;

  init_worker_threads();

  while (!stop_server) {
    client_args client = produce(server_reg_path);
    producer(client);

    fprintf(stdout, "Client %d produced\n", client.id);
  }
}

void set_stop_server(int stop) {
  stop_server = stop;
}