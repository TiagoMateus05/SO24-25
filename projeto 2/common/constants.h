// constantes partilhadas entre cliente e servidor
#define MAX_SESSION_COUNT 4  // num max de sessoes no server, 1 default, necessario alterar mais tarde
#define STATE_ACCESS_DELAY_US  // delay a aplicar no server

#define MAX_PIPE_PATH_LENGTH 40 // tamanho max do caminho do pipe
#define PIPE_PERMS 0640 

#define MAX_STRING_SIZE 40
#define MAX_NUMBER_SUB 10

#define OP_CONNECT 1
#define OP_DISCONNECT 2
#define OP_SUBSCRIBE 3
#define OP_UNSUBSCRIBE 4