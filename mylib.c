/*
 * Client interposition library: intercepts libc file calls and forwards them
 * to the remote server over RPC (via LD_PRELOAD).
 */

#define _GNU_SOURCE

#include <dlfcn.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
 
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdarg.h>
#include <errno.h>
#include <dirent.h>
#include "../include/dirtree.h"
#include "client.h"

// Function pointers for original functions
int (*orig_open)(const char *pathname, int flags, ...);
int (*orig_close)(int fd);
ssize_t (*orig_read)(int fd, void *buf, size_t count);
ssize_t (*orig_write)(int fd, const void *buf, size_t count);
off_t (*orig_lseek)(int fd, off_t offset, int whence);
int (*orig_stat)(const char *pathname, struct stat *statbuf);
int (*orig___xstat)(int vers, const char *pathname, struct stat *statbuf);
int (*orig_unlink)(const char *pathname);
ssize_t (*orig_getdirentries)(int fd, char *buf, size_t nbytes, off_t *basep);
struct dirtreenode* (*orig_getdirtree)(const char *path);
void (*orig_freedirtree)(struct dirtreenode *dt);

// Interposition functions
int open(const char *pathname, int flags, ...) {
	mode_t m = 0;
	if (flags & O_CREAT) {
		va_list a;
		va_start(a, flags);
		m = va_arg(a, mode_t);
		va_end(a);
	}
	
	// Call RPC
	int vfd = rpc_open(pathname, flags, m);
	if (vfd >= 0) {
		// Register VFD with server
		if (rpc_vfd_register(vfd, get_server_fd(vfd)) < 0) {
			// Registration failed, free VFD and return error
			free_vfd(vfd);
			return -1;
		}
	}
	return vfd;
}

int close(int fd) {
	// Check if FD is a VFD
	if (is_vfd(fd)) {
		// VFD: use RPC
		return rpc_close(fd);
	} else {
		// System FD: pass through to original function
		return orig_close(fd);
	}
}

ssize_t read(int fd, void *buf, size_t count) {
	// Check if FD is a VFD
	if (is_vfd(fd)) {
		// VFD: use RPC
		return rpc_read(fd, buf, count);
	} else {
		// System FD: pass through to original function
		return orig_read(fd, buf, count);
	}
}

ssize_t write(int fd, const void *buf, size_t count) {
	// Check if FD is a VFD
	if (is_vfd(fd)) {
		// VFD: use RPC
		return rpc_write(fd, buf, count);
	} else {
		// System FD: pass through to original function
		return orig_write(fd, buf, count);
	}
}

off_t lseek(int fd, off_t offset, int whence) {
	// Check if FD is a VFD
	if (is_vfd(fd)) {
		// VFD: use RPC
		return rpc_lseek(fd, offset, whence);
	} else {
		// System FD: pass through to original function
		return orig_lseek(fd, offset, whence);
	}
}

int stat(const char *pathname, struct stat *statbuf) {
	return rpc_stat(pathname, statbuf);
}

// Also interpose on __xstat for compatibility with older Linux systems
int __xstat(int vers, const char *pathname, struct stat *statbuf) {
	return rpc_xstat(vers, pathname, statbuf);
}

int unlink(const char *pathname) {
	return rpc_unlink(pathname);
}

ssize_t getdirentries(int fd, char *buf, size_t nbytes, off_t *basep) {
	// Check if FD is a VFD
	if (is_vfd(fd)) {
		// VFD: use RPC
		return rpc_getdirentries(fd, buf, nbytes, basep);
	} else {
		// System FD: pass through to original function
		return orig_getdirentries(fd, buf, nbytes, basep);
	}
}

struct dirtreenode* getdirtree(const char *path) {
	return rpc_getdirtree(path);
}

void freedirtree(struct dirtreenode *dt) {
	// Pass through to original function - no RPC needed
	// Tree structure is allocated on client side, so freeing is local
	orig_freedirtree(dt);
}

// This function is automatically called when program is started
void _init(void) {
	// Initialize all function pointers to original functions
	orig_open = dlsym(RTLD_NEXT, "open");
	orig_close = dlsym(RTLD_NEXT, "close");
	orig_read = dlsym(RTLD_NEXT, "read");
	orig_write = dlsym(RTLD_NEXT, "write");
	orig_lseek = dlsym(RTLD_NEXT, "lseek");
	orig_stat = dlsym(RTLD_NEXT, "stat");
	orig___xstat = dlsym(RTLD_NEXT, "__xstat");
	orig_unlink = dlsym(RTLD_NEXT, "unlink");
	orig_getdirentries = dlsym(RTLD_NEXT, "getdirentries");
	orig_getdirtree = dlsym(RTLD_NEXT, "getdirtree");
	orig_freedirtree = dlsym(RTLD_NEXT, "freedirtree");
	
	// Give client.c access to orig_close so it can close internal sockets
	set_orig_close(orig_close);
	
	// Note: We'll connect to server on first function call, not here
	// This avoids issues if server isn't running yet
}


