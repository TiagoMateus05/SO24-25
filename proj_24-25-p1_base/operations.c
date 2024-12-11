#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "kvs.h"
#include "operations.h"
#include "parser.h"
#include "constants.h"

int MAX_BACKUPS;
int CURRENT_BACKUPS = 0;
pthread_rwlock_t global_lock = PTHREAD_RWLOCK_INITIALIZER;

static struct HashTable* kvs_table = NULL;

/// Calculates a timespec from a delay in milliseconds.
/// @param delay_ms Delay in milliseconds.
/// @return Timespec with the given delay.
static struct timespec delay_to_timespec(unsigned int delay_ms) {
  return (struct timespec){delay_ms / 1000, (delay_ms % 1000) * 1000000};
}

void setMaxBackups(int max) {
  MAX_BACKUPS = max;
}

int kvs_init() {
  if (kvs_table != NULL) {
    fprintf(stderr, "KVS state has already been initialized\n");
    return 1;
  }

  kvs_table = create_hash_table();
  safe_rwlock_init(&(kvs_table->table_lock));
  for (int i = 0; i < TABLE_SIZE; i++)
    safe_rwlock_init(&(kvs_table->cell_locks[i]));
  return kvs_table == NULL;
}

int kvs_terminate() {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }
  safe_rwlock_destroy(&(kvs_table->table_lock));
  for (int i = 0; i < TABLE_SIZE; i++)
    safe_rwlock_destroy(&(kvs_table->cell_locks[i]));
  return 0;
}

int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE]) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  safe_rwlock_rdlock(&kvs_table->table_lock);
  for (size_t i = 0; i < num_pairs; i++) {
    if (write_pair(kvs_table, keys[i], values[i]) != 0) {
      fprintf(stderr, "Failed to write keypair (%s,%s)\n", keys[i], values[i]);
    }
  }
  safe_rwlock_unlock(&kvs_table->table_lock);
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
  safe_rwlock_rdlock(&kvs_table->table_lock);
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
  safe_rwlock_unlock(&kvs_table->table_lock);

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

  safe_rwlock_rdlock(&kvs_table->table_lock);
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
  safe_rwlock_unlock(&kvs_table->table_lock);
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

  safe_rwlock_wrlock(&kvs_table->table_lock);

  size_t buffer_size = 0;
  for (int i = 0; i < TABLE_SIZE; i++) {
    KeyNode *keyNode = kvs_table->table[i];
    while (keyNode != NULL) {
      buffer_size += strlen(keyNode->key) + strlen(keyNode->value) + 5; // 5 for "(", ",", ")\n" and null terminator
      keyNode = keyNode->next;
    }
  }
  char *buffer = (char *)safe_malloc(buffer_size + 1);
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

  safe_rwlock_unlock(&kvs_table->table_lock);

  if (write_to_file(out_fd, buffer) != 0) {
    free(buffer);
    return 1;
  }

  free(buffer);
  return 0;
}

int kvs_backup(int fd) {
  if (kvs_table == NULL) {
      fprintf(stderr, "KVS state must be initialized\n");
      return 1;
  }
  if (fd == -1) {
      fprintf(stderr, "Failed to open backup file\n");
      return 1;
  }

  size_t buffer_size = 0;
  for (int i = 0; i < TABLE_SIZE; i++) {
    KeyNode *keyNode = kvs_table->table[i];
    while (keyNode != NULL) {
        buffer_size += strlen(keyNode->key) + strlen(keyNode->value) + 5; // 5 for "(", ",", ")\n" and null terminator
        keyNode = keyNode->next;
    }
  }
  fprintf(stderr, "Started Seeing Table\n");
  char *buffer = (char *)safe_malloc(buffer_size + 1);
  buffer[0] = '\0';
  fprintf(stderr, "A\n");
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
  fprintf(stderr, "Finished See Table\n");
  if (write_to_file(fd, buffer) != 0) {
    free(buffer);
    return 1;
  }
  fprintf(stderr, "Finished Writting\n");
  free(buffer);
  fprintf(stderr, "Finish Backup\n");
  return 0;
}

void kvs_wait(unsigned int delay_ms) {
  struct timespec delay = delay_to_timespec(delay_ms);
  nanosleep(&delay, NULL);
}

void *thread_function(void *args) {
  struct thread_args *t_args = (struct thread_args *)args;
  char jobs_path[PATH_MAX];
  char out_path[PATH_MAX];
  int exit_flag = 0;
  int *ret = (int*)safe_malloc(sizeof(int));

  strncpy(jobs_path, t_args->jobs_path, PATH_MAX);

  int open_flags = O_WRONLY | O_CREAT | O_TRUNC;
  mode_t file_perms = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH | S_IWGRP | S_IWOTH;
  int num_backups = 1;
  size_t len = strlen(jobs_path);

  int jobs_fd = open(jobs_path, O_RDONLY);
  if (jobs_fd == -1) {
    fprintf(stderr, "Failed to open job file\n");
    free(t_args);
    return ret;
  }

  strncpy(out_path, jobs_path, PATH_MAX - 1);
  out_path[PATH_MAX - 1] = '\0'; // Ensure null termination
  out_path[t_args->path_len - 5] = '\0';
  strcat(out_path, ".out");

  int out_fd = open(out_path, open_flags, file_perms);
  if (out_fd == -1) {
    fprintf(stderr, "Failed to open output file\n");
    free(t_args);
    return ret;
  }

  while (!exit_flag) {
    char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    unsigned int delay;
    size_t num_pairs;
    char backup_path[PATH_MAX];

    switch (get_next(jobs_fd)) {
      case CMD_WRITE:
        num_pairs = parse_write(jobs_fd, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
        if (num_pairs == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (kvs_write(num_pairs, keys, values)) {
          fprintf(stderr, "Failed to write pair\n");
        }

        break;

      case CMD_READ:
        num_pairs = parse_read_delete(jobs_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

        if (num_pairs == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (kvs_read(num_pairs, keys, out_fd)) {
          fprintf(stderr, "Failed to read pair\n");
        }
        break;

      case CMD_DELETE:
        num_pairs = parse_read_delete(jobs_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

        if (num_pairs == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (kvs_delete(num_pairs, keys, out_fd)) {
          fprintf(stderr, "Failed to delete pair\n");
        }
        break;

      case CMD_SHOW:
        if (kvs_show(out_fd)) {
          fprintf(stderr, "Failed to show KVS\n");
        }
        break;

      case CMD_WAIT:
        if (parse_wait(jobs_fd, &delay, NULL) == -1) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (delay > 0) {
          write_to_file(out_fd, "Waiting...\n");
          kvs_wait(delay);
        }
        break;

      case CMD_BACKUP:
      fprintf(stderr, "start backup\n");
        strncpy(backup_path, jobs_path, len - 4);
        backup_path[len - 4] = '\0'; // Ensure null termination

        char suffix[20];
        snprintf(suffix, sizeof(suffix), "-%d.bck", num_backups);
        strcat(backup_path, suffix);
        num_backups++;
	
	      fprintf(stderr, "Making New Process of %d of %s\n", num_backups, jobs_path);
        pid_t pid;
	      safe_rwlock_wrlock(&global_lock);
	      fprintf(stderr, "Checking current backups with current backups: %d\n", CURRENT_BACKUPS);
        if (MAX_BACKUPS == CURRENT_BACKUPS) {
	      fprintf(stderr, "Waiting for Child to fork\n");
          wait(NULL);
          pid = fork();
        } else {
          CURRENT_BACKUPS++;
          pid = fork();
        }

        if (pid == 0) {
          fprintf(stderr, "Open .bck\n");
          int bck_fd = open(backup_path, open_flags, file_perms);
          fprintf(stderr, "Backup Function\n");
          kvs_backup(bck_fd);
          fprintf(stderr, "Finished Backing up\n");
          close(bck_fd);
	        fprintf(stderr, "Quiting Child\n");
          exit(0);
        } else if (pid < 0) {
          fprintf(stderr, "Failed to fork\n");
          free(t_args);
          return NULL;
        } else {
          break;
        }
	      safe_rwlock_unlock(&global_lock);
        break;
      case CMD_INVALID:
        fprintf(stderr, "Invalid command. See HELP for usage\n");
        break;

      case CMD_HELP:
        printf( 
            "Available commands:\n"
            "  WRITE [(key,value)(key2,value2),...]\n"
            "  READ [key,key2,...]\n"
            "  DELETE [key,key2,...]\n"
            "  SHOW\n"
            "  WAIT <delay_ms>\n"
            "  BACKUP\n" // Not implemented
            "  HELP\n"
        );

        break;
        
      case CMD_EMPTY:
        break;

      case EOC:
        *ret = 0;
        exit_flag = 1;

    }
    if (exit_flag) {
      break;
    }
  }  
  free(t_args);
  return ret;
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
    exit(EXIT_FAILURE);
  }
  return ptr;
}

void safe_mutex_init(pthread_mutex_t *mutex) {
  if (pthread_mutex_init(mutex, NULL) != 0) {
    fprintf(stderr, "Failed to initialize mutex\n");
    exit(EXIT_FAILURE);
  }
}

void safe_mutex_lock(pthread_mutex_t *mutex) {
  if (pthread_mutex_lock(mutex) != 0) {
    fprintf(stderr, "Failed to lock mutex\n");
    exit(EXIT_FAILURE);
  }
}

void safe_mutex_unlock(pthread_mutex_t *mutex) {
  if (pthread_mutex_unlock(mutex) != 0) {
    fprintf(stderr, "Failed to unlock mutex\n");
    exit(EXIT_FAILURE);
  }
}

void safe_mutex_destroy(pthread_mutex_t *mutex) {
  if (pthread_mutex_destroy(mutex) != 0) {
    fprintf(stderr, "Failed to destroy mutex\n");
    exit(EXIT_FAILURE);
  }
}

void safe_rwlock_init(pthread_rwlock_t *rwlock) {
  if (pthread_rwlock_init(rwlock, NULL) != 0) {
    fprintf(stderr, "Failed to initialize rwlock\n");
    exit(EXIT_FAILURE);
  }
}

void safe_rwlock_rdlock(pthread_rwlock_t *rwlock) {
  if (pthread_rwlock_rdlock(rwlock) != 0) {
    fprintf(stderr, "Failed to lock rw_rdlock\n");
    exit(EXIT_FAILURE);
  }
}

void safe_rwlock_wrlock(pthread_rwlock_t *rwlock) {
  if (pthread_rwlock_wrlock(rwlock) != 0) {
    fprintf(stderr, "Failed to lock rw_wrlock\n");
    exit(EXIT_FAILURE);
  }
}

void safe_rwlock_unlock(pthread_rwlock_t *rwlock) {
  if (pthread_rwlock_unlock(rwlock) != 0) {
    fprintf(stderr, "Failed to unlock rwlock\n");
    exit(EXIT_FAILURE);
  }
}

void safe_rwlock_destroy(pthread_rwlock_t *rwlock) {
  if (pthread_rwlock_destroy(rwlock) != 0) {
    fprintf(stderr, "Failed to destroy rwlock\n");
    exit(EXIT_FAILURE);
  }
}

/*
  WRITE - Bucket only (read table, write bucket) DONE
  READ - Bucket only (read table, read bucket) DONE
  DELETE - Bucket only (read table, write bucket) DONE
  SHOW - Whole table (write table, -----) DONE
  BACKUP - Nothing (-----, -----) DONE
*/
