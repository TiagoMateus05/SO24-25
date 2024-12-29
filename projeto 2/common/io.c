#include "io.h"
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "constants.h"
 
int read_all(int fd, void *buffer, size_t size, int *intr) {
  if (intr != NULL && *intr) {
    return -1;
  }
  size_t bytes_read = 0;
  while (bytes_read < size) {
    ssize_t result = read(fd, buffer + bytes_read, size - bytes_read);
    if (result == -1) {
      if (errno == EINTR) {
        if (intr != NULL) {
          *intr = 1;
          if (bytes_read == 0) {
            return -1;
          }
        }
        continue;
      }
      perror("Failed to read from pipe");
      return -1;
    } else if (result == 0) {
      return 0;
    }
    bytes_read += (size_t)result;
  }
  return 1;
}
 
int read_string(int fd, char *str) {
  ssize_t bytes_read = 0;
  char ch;
  while (bytes_read < MAX_STRING_SIZE - 1) {
    if (read(fd, &ch, 1) != 1) {
      return -1;
    }
    if (ch == '\0' || ch == '\n') {
      break;
    }
    str[bytes_read++] = ch;
  }
  str[bytes_read] = '\0';
  return (int)bytes_read;
}
 
int write_all(int fd, const void *buffer, size_t size) {
  size_t bytes_written = 0;
  while (bytes_written < size) {
    ssize_t result = write(fd, buffer + bytes_written, size - bytes_written);
    if (result == -1) {
      if (errno == EINTR) {
        // error for broken PIPE (error associated with writting to the closed PIPE)
        continue;
      }
      perror("Failed to write to pipe");
      return -1;
    }
    bytes_written += (size_t)result;
  }
  return 1;
}

static struct timespec delay_to_timespec(unsigned int delay_ms) {
    return (struct timespec){delay_ms / 1000, (delay_ms % 1000) * 1000000};
}

void delay(unsigned int time_ms) {
    struct timespec delay = delay_to_timespec(time_ms);
    nanosleep(&delay, NULL);
}

void *safe_malloc(size_t size) {
    void *ptr = malloc(size);
    if (ptr == NULL) {
        fprintf(stderr, "Failed to allocate memory\n");
        exit(EXIT_FAILURE);
    }
    return ptr;
}

int safe_open(const char *path, int flags) {
    int fd = open(path, flags);
    if (fd == -1) {
        fprintf(stderr, "Failed to open file\n");
        exit(EXIT_FAILURE);
    }
    return fd;
}

void safe_close(int fd) {
    if (close(fd) == -1) {
        fprintf(stderr, "Failed to close file\n");
    }
}

void safe_unlink(const char *path) {
    if (unlink(path) == -1 && errno != ENOENT) {
        fprintf(stderr, "Failed to unlink %s: %s\n", path, strerror(errno));
    }
}

void open_fifo(const char *path, mode_t mode) {
  safe_unlink(path);

  if (mkfifo(path, mode) != 0) {
    fprintf(stderr, "Failed to create FIFO %s: %s\n", path, strerror(errno));
    exit(EXIT_FAILURE);
  }

  fprintf(stdout, "Created FIFO %s\n", path);
}
