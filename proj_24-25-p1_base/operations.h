#ifndef KVS_OPERATIONS_H
#define KVS_OPERATIONS_H

#include <stddef.h>

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

/// Waits for the last backup to be called.
void kvs_wait_backup();

/// Waits for a given amount of time.
/// @param delay_us Delay in milliseconds.
void kvs_wait(unsigned int delay_ms);

/// Checks if the file is a .jobs file.
/// @param filename Name of the file.
/// @return 1 if the file is a .jobs file, 0 otherwise.
int is_jobs_file(const char *filename);

/// Writes the buffer to the file descriptor.
/// @param fd File descriptor of the file.
/// @param buffer Buffer to be written.
/// @return 0 if the buffer was written successfully, 1 otherwise.
int write_to_file(int fd, char *buffer);

/// Allocates memory safely.
/// @param size 
/// @return Pointer to the allocated memory.
void *safe_malloc(size_t size);

#endif  // KVS_OPERATIONS_H
