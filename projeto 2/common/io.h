#ifndef COMMON_IO_H
#define COMMON_IO_H

#include <stddef.h>

/// Reads a given number of bytes from a file descriptor. Will block until all
/// bytes are read, or fail if not all bytes could be read.
/// @param fd File descriptor to read from.
/// @param buffer Buffer to read into.
/// @param size Number of bytes to read.
/// @param intr Pointer to a variable that will be set to 1 if the read was interrupted.
/// @return On success, returns 1, on end of file, returns 0, on error, returns -1
int read_all(int fd, void *buffer, size_t size, int *intr);

int read_string(int fd, char *str);

/// Writes a given number of bytes to a file descriptor. Will block until all
/// bytes are written, or fail if not all bytes could be written.
/// @param fd File descriptor to write to.
/// @param buffer Buffer to write from.
/// @param size Number of bytes to write.
/// @return On success, returns 1, on error, returns -1
int write_all(int fd, const void *buffer, size_t size);

void delay(unsigned int time_ms);

/// Safely allocates memory using malloc.
/// @param size Size of the memory to allocate.
/// @return Pointer to the allocated memory.
void *safe_malloc(size_t size);

/// Safely opens a file.
/// @param pathname Path to the file to open.
/// @param flags Flags to pass to open.
/// @return File descriptor on success.
int safe_open(const char *pathname, int flags);

/// Safely closes a file descriptor.
/// @param fd File descriptor to close.
void safe_close(int fd);

/// Safely unlinks a file.
/// @param pathname Path to the file to unlink.
void safe_unlink(const char *pathname);

// Opens a FIFO with the given pathname and flags.
/// @param pathname Path to the FIFO to open.
/// @param flags Flags to pass to open.
void open_fifo(const char *pathname, int flags);

#endif  // COMMON_IO_H
