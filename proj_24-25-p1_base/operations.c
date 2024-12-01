#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "kvs.h"
#include "constants.h"
#include "operations.h"

static struct HashTable* kvs_table = NULL;


/// Calculates a timespec from a delay in milliseconds.
/// @param delay_ms Delay in milliseconds.
/// @return Timespec with the given delay.
static struct timespec delay_to_timespec(unsigned int delay_ms) {
  return (struct timespec){delay_ms / 1000, (delay_ms % 1000) * 1000000};
}

int kvs_init() {
  if (kvs_table != NULL) {
    fprintf(stderr, "KVS state has already been initialized\n");
    return 1;
  }

  kvs_table = create_hash_table();
  return kvs_table == NULL;
}

int kvs_terminate() {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  free_table(kvs_table);
  return 0;
}

int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE]) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  for (size_t i = 0; i < num_pairs; i++) {
    if (write_pair(kvs_table, keys[i], values[i]) != 0) {
      fprintf(stderr, "Failed to write keypair (%s,%s)\n", keys[i], values[i]);
    }
  }

  return 0;
}

int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE], int out_fd) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }
  size_t pair_size = MAX_STRING_SIZE * 2 + 3;

  char *buffer = (char *)safe_malloc(MAX_STRING_SIZE * pair_size * num_pairs + 2); 
  buffer[0] = '\0';

  strcat(buffer, "[");
  for (size_t i = 0; i < num_pairs; i++) {
    char* result = read_pair(kvs_table, keys[i]);
    if (result == NULL) {
      strcat(buffer, "(");
      strcat(buffer, keys[i]);
      strcat(buffer, ",KVSERROR)");
    } else {
      
      strcat(buffer, "(");
      strcat(buffer, keys[i]);
      strcat(buffer, ",");
      strcat(buffer, result);
      strcat(buffer, ")");
    }
    free(result);
  }
  strcat(buffer, "]\n");

  if (write_to_file(out_fd, buffer) != 0) {
    free(buffer);
    return 1;
  }

  free(buffer);
  return 0;
}

int kvs_delete(size_t num_pairs, char keys[][MAX_STRING_SIZE], int out_fd) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  size_t pair_size = MAX_STRING_SIZE * 2 + 3;
  char *buffer = (char *)safe_malloc(MAX_STRING_SIZE * pair_size * num_pairs + 2);
  buffer[0] = '\0';
  int aux = 0;

  for (size_t i = 0; i < num_pairs; i++) {
    if (delete_pair(kvs_table, keys[i]) != 0) {
      if (!aux) {
        strcat(buffer, "[");
        aux = 1;
      }
      strcat(buffer, "(");
      strcat(buffer, keys[i]);
      strcat(buffer, ",KVSMISSING)");
    }
  }
  if (aux) {
    strcat(buffer, "]\n");
  }

  if (write_to_file(out_fd, buffer) != 0) {
    free(buffer);
    return 1;
  }

  free(buffer);
  return 0;
}

int kvs_show(int out_fd) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  } 
  
  size_t pair_size = MAX_STRING_SIZE * 2 + 3;
  // TODO: verify if malloc size is correct
  char *buffer = (char *)safe_malloc(pair_size * TABLE_SIZE);
  buffer[0] = '\0';

  for (int i = 0; i < TABLE_SIZE; i++) {
    KeyNode *keyNode = kvs_table->table[i];
    while (keyNode != NULL) {
      strcat(buffer, "(");
      strcat(buffer, keyNode->key);
      strcat(buffer, ",");
      strcat(buffer, keyNode->value);
      strcat(buffer, ")\n");

      keyNode = keyNode->next; // Move to the next node
    }
  }

  if (write_to_file(out_fd, buffer) != 0) {
    free(buffer);
    return 1;
  }

  free(buffer);
  return 0;
}

int kvs_backup() {
  return 0;
}

void kvs_wait(unsigned int delay_ms) {
  struct timespec delay = delay_to_timespec(delay_ms);
  nanosleep(&delay, NULL);
}

// Auxiliary functions 
int is_jobs_file(const char *filename) {
  return strlen(filename) > 4 && !strcmp(filename + strlen(filename) - 4, ".job");
}

int write_to_file(int fd, char *buffer) {
  ssize_t n = 0;
  size_t done = 0;
  size_t len = strlen(buffer);

  while (n = write(fd, buffer + done, len), n > 0) {
    done += (size_t)n;
    len -= (size_t)n;
  }

  if (n == -1) {
    fprintf(stderr, "Failed to write to output file\n");
    return 1;
  }

  return 0;
}

void *safe_malloc(size_t size) {
  void *ptr = malloc(size);
  if (ptr == NULL) {
    fprintf(stderr, "Failed to allocate memory\n");
    exit(1);
  }
  return ptr;
}