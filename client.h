#ifndef CLIENT_H
#define CLIENT_H

#include <sys/stat.h>

// VFD (Virtual File Descriptor) constants
#define VFD_BASE 1000
#define MAX_VFDS 1024

// Function to send function name to server
void send_to_server(const char *func_name);

// Function to set the original close pointer (called from mylib.c)
// This allows client.c to close internal sockets without logging
void set_orig_close(int (*close_func)(int fd));

// VFD management functions
int allocate_vfd(int server_fd);
void free_vfd(int client_vfd);
int get_server_fd(int client_vfd);
int is_vfd(int fd);

// RPC functions
int rpc_open(const char *pathname, int flags, mode_t mode);
int rpc_vfd_register(int client_vfd, int server_fd);
ssize_t rpc_write(int fd, const void *buf, size_t count);
ssize_t rpc_read(int fd, void *buf, size_t count);
off_t rpc_lseek(int fd, off_t offset, int whence);
int rpc_close(int fd);
int rpc_stat(const char *pathname, struct stat *statbuf);
int rpc_xstat(int version, const char *pathname, struct stat *statbuf);
int rpc_unlink(const char *pathname);
ssize_t rpc_getdirentries(int fd, char *buf, size_t nbytes, off_t *basep);
struct dirtreenode* rpc_getdirtree(const char *pathname);

#endif // CLIENT_H
