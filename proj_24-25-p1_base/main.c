#include <dirent.h>    
#include <errno.h>  
#include <fcntl.h>
#include <limits.h>    
#include <pthread.h>
#include <stdio.h>     
#include <stdlib.h>    
#include <string.h>  
#include <sys/wait.h>
#include <sys/stat.h>   
#include <unistd.h>  

#include "parser.h"
#include "operations.h"
#include "constants.h"


int MAX_THREADS;
int CURRENT_THREADS = 0;

int main(int argc, char *argv[]) {

  if (argc != 4) {
    fprintf(stderr, "Usage: %s <dir_path> <MAX_BACKUPS> <MAX_THREADS>\n", argv[0]);
    return 1;
  }

  if (kvs_init()) {
    fprintf(stderr, "Failed to initialize KVS\n");
    return 1;
  }

  //Get the arguments
  char *dir_path = argv[1];
  int max;
  if (sscanf(argv[2], "%d", &max) != 1) {
    fprintf(stderr, "Invalid MAX_BACKUPS value\n");
    return 1;
  }

  setMaxBackups(max);

  if (sscanf(argv[3], "%d", &MAX_THREADS) != 1) {
    fprintf(stderr, "Invalid MAX_THREADS value\n");
    return 1;
  }
  //Open the dir Path
  DIR *dir = opendir(dir_path);

  if (dir == NULL) {
    fprintf(stderr, "Failed to open directory\n");
    return 1;
  }

  struct dirent *dp; 
  errno = 0;
  void *ret;
  int threads_id[MAX_THREADS];
  pthread_t threads[MAX_THREADS];

  for (int i = 0; i < MAX_THREADS; i++) {
    threads_id[i] = i;
    threads[i] = 0;
  }

  while ((dp = readdir(dir)) != NULL) {
    char jobs_path[PATH_MAX];
    snprintf(jobs_path, PATH_MAX, "%s/%s", dir_path, dp->d_name);


    struct stat sb;
    if (stat(jobs_path, &sb) == 0 && S_ISREG(sb.st_mode) && is_jobs_file(dp->d_name)) {
      while (CURRENT_THREADS >= MAX_THREADS) {
        for (int i = 0; i < MAX_THREADS; i++) {
          if (threads[i] == 0) {
            continue;
          }
          if (pthread_join(threads[i], &ret) != 0) {
            fprintf(stderr, "Failed to join thread\n");
            free(ret);
            return 1;
          }
          free(ret);
          threads[i] = 0;
          CURRENT_THREADS--;
        }
      }
      size_t len = strlen(dir_path) + strlen(dp->d_name) + 2; // +2 for '/' and '\0'
      struct thread_args *args = safe_malloc(sizeof(struct thread_args));
      args->id = threads_id[CURRENT_THREADS];
      strncpy(args->jobs_path, jobs_path, PATH_MAX);
      args->path_len = len;
      if (pthread_create(&threads[CURRENT_THREADS], NULL, thread_function, 
      args) != 0) {
        fprintf(stderr, "Failed to create thread\n");
        return 1;
      }

      CURRENT_THREADS++;
    }  
  }

  while(wait(NULL) != -1 || errno != ECHILD) {}
  
  for (int i = 0; i < CURRENT_THREADS; i++) {
    if (threads[i] == 0) {
      continue;
    }
    if (pthread_join(threads[i], &ret) != 0) {
      fprintf(stderr, "Failed to join thread\n");
      free(ret);
      return 1;
    } 
    free(ret);
  }

  if (closedir(dir) == -1) {
    fprintf(stderr, "Failed to close directory\n");
    return 1;
  }

  kvs_terminate();
}
