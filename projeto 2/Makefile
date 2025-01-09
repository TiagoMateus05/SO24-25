CC = gcc

# Compilation flags
CFLAGS = -g -std=c17 -D_POSIX_C_SOURCE=200809L -I. \
         -Wall -Wextra \
         -Wcast-align -Wconversion -Wfloat-equal -Wformat=2 -Wnull-dereference -Wshadow -Wsign-conversion -Wswitch-enum -Wundef -Wunreachable-code -Wunused \
         -pthread
# -fsanitize=address -fsanitize=undefined 

ifneq ($(shell uname -s),Darwin) # if not MacOS
    CFLAGS += -fmax-errors=5
endif

# Main targets
all: server/kvs client/client

# Server binary
server/kvs: server/main.o server/operations.o server/kvs.o server/io.o server/parser.o common/io.o server/client_util.o
	$(CC) $(CFLAGS) -o $@ $^

# Client binary
client/client: client/main.o client/api.o client/parser.o common/io.o
	$(CC) $(CFLAGS) -o $@ $^

# Pattern rule for object files
%.o: %.c %.h
	$(CC) $(CFLAGS) -c $< -o $@

# Clean up
clean:
	rm -f common/*.o client/*.o server/*.o server/kvs client/client client/client_write

# Format code
format:
	@which clang-format >/dev/null 2>&1 || echo "Please install clang-format to run this command"
	clang-format -i common/*.c common/*.h client/*.c client/*.h server/*.c server/*.h
