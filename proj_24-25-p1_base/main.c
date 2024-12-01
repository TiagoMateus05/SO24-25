#include <dirent.h>    
#include <errno.h>  
#include <fcntl.h>  
#include <limits.h>    
#include <stdio.h>     
#include <stdlib.h>    
#include <string.h>  
#include <sys/stat.h>   
#include <unistd.h>  

#include "constants.h"
#include "parser.h"
#include "operations.h"

// Hack to solve the issue of DT_REG not being recognized

int main(int argc, char *argv[]) {
  if (argc != 3) {
    fprintf(stderr, "Usage: %s <dir_path> <MAX_BACKUPS>\n", argv[0]);
    return 1;
  }

  if (kvs_init()) {
    fprintf(stderr, "Failed to initialize KVS\n");
    return 1;
  }

  char *dir_path = argv[1];
  DIR *dir = opendir(dir_path);

  if (dir == NULL) {
    fprintf(stderr, "Failed to open directory\n");
    return 1;
  }

  struct dirent *dp; 
  errno = 0;

  //int MAX_BACKUPS = atoi(argv[2]); not implemented

  //Structure to verify file type
  struct stat sb;
  char path[PATH_MAX]; //For the build of the path of file

  while ((dp = readdir(dir)) != NULL) {
    snprintf(path, PATH_MAX, "%s/%s", dir_path, dp->d_name); //Builds the path to be used in stat(path, &sb)
    if (stat(path, &sb) == 0 && S_ISREG(sb.st_mode) && is_jobs_file(dp->d_name)) {
      size_t len = strlen(dir_path) + strlen(dp->d_name) + 2; // +2 for '/' and '\0'
      char *jobs_path = (char*)safe_malloc(len);
      strcpy(jobs_path, dir_path);
      strcat(jobs_path, "/");
      strcat(jobs_path, dp->d_name);

      int jobs_fd = open(jobs_path, O_RDONLY);
      if (jobs_fd == -1) {
        fprintf(stderr, "Failed to open job file\n");
        return 1;
      }

      int open_flags = O_WRONLY | O_CREAT | O_TRUNC;
      mode_t file_perms = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH | S_IWGRP | S_IWOTH;

      char *out_path = (char*)safe_malloc(len);
      memset(out_path, 0, len);
      strncpy(out_path, jobs_path, len - 5);
      strcat(out_path, ".out");

      int out_fd = open(out_path, open_flags, file_perms);
      if (out_fd == -1) {
        fprintf(stderr, "Failed to open output file\n");
        return 1;
      }

      while (1) {
        char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
        char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
        unsigned int delay;
        size_t num_pairs;

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
            if (kvs_backup()) {
              fprintf(stderr, "Failed to perform backup.\n");
            }
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
            // TODO: make it less repetitive
            // Might solve itself when implementing threads
            if (close(jobs_fd) == -1) {
              fprintf(stderr, "Failed to close job file\n");
              return 1;
            }

            if (close(out_fd) == -1) {
              fprintf(stderr, "Failed to close output file\n");
              return 1;
            }

            free(jobs_path);
            free(out_path);

            if (closedir(dir) == -1) {
              fprintf(stderr, "Failed to close directory\n");
              return 1;
            }

            kvs_terminate();
            return 0;
        }
      }
    }
  }
  if (closedir(dir) == -1) {
    fprintf(stderr, "Failed to close directory\n");
    return 1;
  }

  kvs_terminate();
}
