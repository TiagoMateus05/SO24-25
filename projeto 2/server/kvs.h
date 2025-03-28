#ifndef KEY_VALUE_STORE_H
#define KEY_VALUE_STORE_H
#define TABLE_SIZE 26

#include <stddef.h>
#include <pthread.h>

#include "../common/constants.h"

typedef struct KeyNode {
    char *key;
    char *value;
    int subscribers[MAX_SESSION_COUNT];
    struct KeyNode *next;
} KeyNode;

typedef struct HashTable {
    KeyNode *table[TABLE_SIZE];
    pthread_rwlock_t tablelock;
} HashTable;

/// Creates a new KVS hash table.
/// @return Newly created hash table, NULL on failure
struct HashTable *create_hash_table();

int hash(const char *key); 

// Writes a key value pair in the hash table.
// @param ht The hash table.
// @param key The key.
// @param value The value.
// @return 0 if successful.
int write_pair(HashTable *ht, const char *key, const char *value);

// Reads the value of a given key.
// @param ht The hash table.
// @param key The key.
// return the value if found, NULL otherwise.
char* read_pair(HashTable *ht, const char *key);

/// Deletes a pair from the table.
/// @param ht Hash table to read from.
/// @param key Key of the pair to be deleted.
/// @return 0 if the node was deleted successfully, 1 otherwise.
int delete_pair(HashTable *ht, const char *key);

/// Frees the hashtable.
/// @param ht Hash table to be deleted.
void free_table(HashTable *ht);

/// Adds a subscriber to a key.
/// @param ht Hash table to add the subscriber.
/// @param key Key to add the subscriber.
/// @param notif_fd File descriptor to notify the subscriber.
int add_subscriber(HashTable *ht, const char *key, int notif_fd);

/// Removes a subscriber from a key.
/// @param ht Hash table to remove the subscriber.
/// @param key Key to remove the subscriber.
int remove_subscriber(HashTable *ht, const char *key , int fd);

/// Notifies all subscribers of a key.
/// @param keyNode Node of the key to notify the subscribers.
/// @param value Value of updated key or "DELETED" if the key was deleted.
void notify_subscribers(KeyNode *keyNode, const char *value);

#endif  // KVS_H
