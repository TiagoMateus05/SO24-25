#ifndef KVS_OPERATIONS_H
#define KVS_OPERATIONS_H

#include <limits.h>
#include <pthread.h>
#include <stddef.h>
#include <sys/types.h>

#include "constants.h"

struct thread_args {
  int id;
  int MAX_THREADS;
  char jobs_path[PATH_MAX];
  size_t path_len;
};

/// Initializes the KVS state.
/// @return 0 if the KVS state was initialized successfully, 1 otherwise.
int kvs_init();

/// Destroys the KVS state.
/// @return 0 if the KVS state was terminated successfully, 1 otherwise.
int kvs_terminate();

/// Writes a key value pair to the KVS. If key already exists it is updated.
/// @param num_pairs Number of pairs being written.
/// @param keys Array of keys' strings.
/// @param values Array of values' strings.
/// @return 0 if the pairs were written successfully, 1 otherwise.
int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE]);

/// Reads values from the KVS.
/// @param num_pairs Number of pairs to read.
/// @param keys Array of keys' strings.
/// @param fd File descriptor to write the output.
/// @return 0 if the key reading, 1 otherwise.
int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fd);

/// Deletes key value pairs from the KVS.
/// @param num_pairs Number of pairs to read.
/// @param keys Array of keys' strings.
/// @param fd File descriptor to write the output.
/// @return 0 if the pairs were deleted successfully, 1 otherwise.
int kvs_delete(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fd);

/// Writes the state of the KVS.
/// @param fd File descriptor to write the output.
/// @return 0 if the KVS state was written successfully, 1 otherwise.
int kvs_show(int fd);

/// Creates a backup of the KVS state and stores it in the correspondent
/// backup file
/// @return 0 if the backup was successful, 1 otherwise.
int kvs_backup(int fd);

/// Handles the backup of the KVS state.
/// @param num_backups Number of backups.
/// @param jobs_path Path to the .jobs file.
/// @param path_len Length of the backup path.
/// @return 0 if the backup was successful, 1 otherwise.
int backup_handler(int num_backups, char *jobs_path, size_t len);

/// Waits for a given amount of time.
/// @param delay_us Delay in milliseconds.
void kvs_wait(unsigned int delay_ms);

/// Thread function that processes the .jobs.
/// @param args Arguments for the thread.
void *thread_function(void *args);

/// Checks if the file is a .jobs file.
/// @param filename Name of the file.
/// @return 1 if the file is a .jobs file, 0 otherwise.
int is_jobs_file(const char *filename);

/// Orders buffer for read and delete operations.
/// @param buffer Buffer to be ordered.
void order_read_delete(char buffer[][MAX_STRING_SIZE], size_t num_pairs);

/// Orders buffer for write operations.
/// @param buffer Buffer to be ordered.
void order_write(char buffer[][MAX_STRING_SIZE], size_t num_pairs);

/// Sets the maximum number of backups.
/// @param max Maximum number of backups.
void set_max_backups(int max);

/// Writes the buffer to the file descriptor.
/// @param fd File descriptor of the file.
/// @param buffer Buffer to be written.
/// @return 0 if the buffer was written successfully, 1 otherwise.
int write_to_file(int fd, char *buffer);

/// Allocates memory safely.
/// @param size 
/// @return Pointer to the allocated memory.
void *safe_malloc(size_t size);

/// Initializes a mutex safely.
/// @param mutex 
void safe_mutex_init(pthread_mutex_t *mutex);

/// Locks a mutex safely.
/// @param mutex
void safe_mutex_lock(pthread_mutex_t *mutex);

/// Unlocks a mutex safely.
/// @param mutex
void safe_mutex_unlock(pthread_mutex_t *mutex);

/// Destroys a mutex safely.
/// @param mutex
void safe_mutex_destroy(pthread_mutex_t *mutex);

/// Initializes a rwlock safely.
/// @param rwlock
void safe_rwlock_init(pthread_rwlock_t *rwlock);

/// Locks a rw_rdlock safely.
/// @param rwlock
void safe_rwlock_rdlock(pthread_rwlock_t *rwlock);

/// Locks a rw_wrlock safely.
/// @param rwlock
void safe_rwlock_wrlock(pthread_rwlock_t *rwlock);

/// Unlocks a rwlock safely.
/// @param rwlock
void safe_rwlock_unlock(pthread_rwlock_t *rwlock);

/// Destroys a rwlock safely.
/// @param rwlock
void safe_rwlock_destroy(pthread_rwlock_t *rwlock);

#endif  // KVS_OPERATIONS_H
