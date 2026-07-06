/*
 * RPC server: executes remote file operations on behalf of clients.
 */

#include <stdio.h>
#include <stdlib.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <string.h>
#include <unistd.h>
#include <err.h>
#include <stdint.h>
#include <endian.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <errno.h>
#include <time.h>
#include <netinet/tcp.h>
#include <sys/wait.h>
#include <dirent.h>
#include "../include/dirtree.h"

#define MAXMSGLEN (10 * 1024 * 1024)  // 10MB to handle large write operations
#define MAX_CLIENT_VFDS 1024
#define VFD_BASE 1000  // Same as client.h - VFDs start at 1000

// Declare __xstat if not available from headers
// On Linux, __xstat is available but may need explicit declaration
extern int __xstat(int version, const char *pathname, struct stat *statbuf);

// Per-client VFD mapping structure
struct client_vfd_map {
	int client_vfd;
	int server_fd;
	int in_use;  // 1 if allocated, 0 if free
};

// Per-client VFD mapping array (local to each child process after fork)
// This is set at the start of handle_client_session() for each client
static struct client_vfd_map *client_vfds = NULL;

// Helper function to initialize/reset VFD map for a new client session
static void init_vfd_map(struct client_vfd_map *vfd_map) {
	for (int i = 0; i < MAX_CLIENT_VFDS; i++) {
		vfd_map[i].client_vfd = 0;
		vfd_map[i].server_fd = 0;
		vfd_map[i].in_use = 0;
	}
}

// Helper function to get server_fd from client_vfd
// Uses O(1) direct indexing since VFDs are allocated sequentially from VFD_BASE
static int get_server_fd_from_vfd(int client_vfd) {
	if (!client_vfds) return -1;
	
	// Check if client_vfd is in valid range
	if (client_vfd < VFD_BASE || client_vfd >= VFD_BASE + MAX_CLIENT_VFDS) {
		return -1;  // Out of range
	}
	
	// Calculate index directly (O(1) instead of O(n))
	int index = client_vfd - VFD_BASE;
	if (index >= 0 && index < MAX_CLIENT_VFDS && 
	    client_vfds[index].in_use && 
	    client_vfds[index].client_vfd == client_vfd) {
		return client_vfds[index].server_fd;
	}
	return -1;  // Not found or not in use
}

// Helper function to remove VFD mapping
// Uses O(1) direct indexing since VFDs are allocated sequentially from VFD_BASE
static void remove_vfd_mapping(int client_vfd) {
	if (!client_vfds) return;
	
	// Check if client_vfd is in valid range
	if (client_vfd < VFD_BASE || client_vfd >= VFD_BASE + MAX_CLIENT_VFDS) {
		return;  // Out of range
	}
	
	// Calculate index directly (O(1) instead of O(n))
	int index = client_vfd - VFD_BASE;
	if (index >= 0 && index < MAX_CLIENT_VFDS && 
	    client_vfds[index].in_use && 
	    client_vfds[index].client_vfd == client_vfd) {
		client_vfds[index].in_use = 0;
		client_vfds[index].client_vfd = 0;
		client_vfds[index].server_fd = 0;
	}
}

// Forward declaration for handle_client_session
void handle_client_session(int sessfd);

// Helper function to send all bytes, ensuring complete transmission
static int send_all(int sockfd, const void *buf, size_t len) {
	const char *ptr = (const char *)buf;
	size_t bytes_sent = 0;
	
	while (bytes_sent < len) {
		ssize_t sent = send(sockfd, ptr + bytes_sent, len - bytes_sent, 0);
		if (sent < 0) {
			return -1;  // Error
		}
		if (sent == 0) {
			return -1;  // Connection closed
		}
		bytes_sent += sent;
	}
	
	return 0;  // Success
}

// Helper function to receive all bytes, handling short counts
static int recv_all(int sockfd, void *buf, size_t len) {
	char *ptr = (char *)buf;
	size_t bytes_received = 0;
	
	while (bytes_received < len) {
		ssize_t received = recv(sockfd, ptr + bytes_received, 
		                        len - bytes_received, 0);
		if (received < 0) return -1;  // Error
		if (received == 0) return -1;  // Connection closed
		bytes_received += received;
	}
	return 0;  // Success
}

// Helper function to receive complete RPC requests
// Returns 0 on success with opcode and msg_len set, -1 on error
static int recv_rpc_request(int sockfd, char *opcode, uint32_t *msg_len) {
	// Receive length prefix (4 bytes)
	uint32_t msg_len_network;
	if (recv_all(sockfd, &msg_len_network, sizeof(msg_len_network)) < 0) {
		return -1;  // Connection closed or error
	}
	
	// Convert from network byte order
	uint32_t total_len = ntohl(msg_len_network);
	
	// Sanity check: request length should be reasonable
	if (total_len > MAXMSGLEN) {
		return -1;  // Request too long
	}
	
	// Receive operation type byte (1 byte)
	if (recv_all(sockfd, opcode, 1) < 0) {
		return -1;  // Connection closed or error
	}
	
	// Calculate message length (length - 1, since length includes opcode)
	*msg_len = total_len - 1;
	
	return 0;  // Success
}

// Helper function to send complete RPC responses
static int send_rpc_response(int sockfd, int64_t ret_val, int32_t errno_val) {
	// Calculate response length: 8 bytes (ret_val) + 4 bytes (errno)
	// Note: The 4-byte length field itself is NOT included in the length value
	uint32_t resp_len = 8 + 4;
	uint32_t resp_len_network = htonl(resp_len);
	
	// Send response length (4 bytes, network byte order)
	if (send_all(sockfd, &resp_len_network, sizeof(resp_len_network)) < 0) {
		return -1;  // Error
	}
	
	// Convert return value to network byte order and send (8 bytes)
	int64_t ret_val_network = htobe64(ret_val);
	if (send_all(sockfd, &ret_val_network, sizeof(ret_val_network)) < 0) {
		return -1;  // Error
	}
	
	// Convert errno to network byte order and send (4 bytes)
	int32_t errno_val_network = htonl(errno_val);
	if (send_all(sockfd, &errno_val_network, sizeof(errno_val_network)) < 0) {
		return -1;  // Error
	}
	
	return 0;  // Success
}

// Forward declarations for RPC handlers (to be implemented in later steps)
void handle_open(int sessfd);
void handle_vfd_register(int sessfd);
void handle_write(int sessfd);
void handle_close(int sessfd);
void handle_read(int sessfd);
void handle_lseek(int sessfd);
void handle_stat(int sessfd);
void handle_xstat(int sessfd);
void handle_unlink(int sessfd);
void handle_getdirentries(int sessfd);
void handle_getdirtree(int sessfd);
void handle_client_session(int sessfd);

// Forward declaration for serialize_dirtree
static void serialize_dirtree(int sessfd, struct dirtreenode *node);

// Helper function to calculate serialized tree size
static size_t calculate_tree_size(struct dirtreenode *node) {
	if (node == NULL) {
		return 4 + 4;  // name_len (0) + num_subdirs (0)
	}
	size_t size = 4;  // name_len
	size += node->name ? strlen(node->name) : 0;  // name
	size += 4;  // num_subdirs
	if (node->subdirs != NULL && node->num_subdirs > 0) {
		for (int i = 0; i < node->num_subdirs; i++) {
			size += calculate_tree_size(node->subdirs[i]);
		}
	}
	return size;
}

// Implement OPEN Handler (Server Side)
void handle_open(int sessfd) {
	// Unmarshal request
	uint32_t pathname_len_network;
	uint32_t pathname_len;
	
	// Receive pathname length (4 bytes)
	if (recv_all(sessfd, &pathname_len_network, sizeof(pathname_len_network)) < 0) {
		// Connection closed or error
		return;
	}
	pathname_len = ntohl(pathname_len_network);
	
	// Allocate buffer for pathname (add 1 for null terminator)
	char *pathname = malloc(pathname_len + 1);
	if (!pathname) {
		// Send error response
		send_rpc_response(sessfd, -1, ENOMEM);
		return;
	}
	
	// Receive pathname (raw bytes, no null terminator)
	if (recv_all(sessfd, pathname, pathname_len) < 0) {
		free(pathname);
		return;  // Connection closed or error
	}
	pathname[pathname_len] = '\0';  // Add null terminator
	
	// Receive flags (4 bytes)
	uint32_t flags_network;
	int flags;
	if (recv_all(sessfd, &flags_network, sizeof(flags_network)) < 0) {
		free(pathname);
		return;  // Connection closed or error
	}
	flags = (int)ntohl(flags_network);
	
	// Receive mode (4 bytes)
	uint32_t mode_network;
	mode_t mode;
	if (recv_all(sessfd, &mode_network, sizeof(mode_network)) < 0) {
		free(pathname);
		return;  // Connection closed or error
	}
	mode = (mode_t)ntohl(mode_network);
	
	// Call actual open() on server
	int server_fd = open(pathname, flags, mode);
	int saved_errno;
	if (server_fd >= 0) {
		// Success: errno should be 0 (or clear any stale value)
		saved_errno = 0;
	} else {
		// Failure: save the actual errno
		saved_errno = errno;
	}
	
	// Send response
	// Response length (4 bytes, network byte order)
	// Return value: server_fd as int64_t (8 bytes, network byte order)
	// Errno: saved_errno as int32_t (4 bytes, network byte order)
	send_rpc_response(sessfd, (int64_t)server_fd, (int32_t)saved_errno);
	
	// Free allocated pathname buffer
	free(pathname);
}

void handle_vfd_register(int sessfd) {
	// Unmarshal request
	uint32_t client_vfd_network;
	uint32_t server_fd_network;
	int client_vfd;
	int server_fd;
	
	// Receive client_vfd (4 bytes)
	if (recv_all(sessfd, &client_vfd_network, sizeof(client_vfd_network)) < 0) {
		// Connection closed or error
		return;
	}
	client_vfd = (int)ntohl(client_vfd_network);
	
	// Receive server_fd (4 bytes)
	if (recv_all(sessfd, &server_fd_network, sizeof(server_fd_network)) < 0) {
		// Connection closed or error
		return;
	}
	server_fd = (int)ntohl(server_fd_network);
	
	// Store mapping in client's VFD map
	// Use direct indexing (O(1)) since VFDs are allocated sequentially from VFD_BASE
	// Check if client_vfd is in valid range
	if (client_vfd < VFD_BASE || client_vfd >= VFD_BASE + MAX_CLIENT_VFDS) {
		send_rpc_response(sessfd, -1, EBADF);  // Bad file descriptor
		return;
	}
	
	// Calculate index directly
	int index = client_vfd - VFD_BASE;
	if (index < 0 || index >= MAX_CLIENT_VFDS) {
		send_rpc_response(sessfd, -1, EBADF);
		return;
	}
	
	// Check if slot is already in use
	if (!client_vfds || client_vfds[index].in_use) {
		send_rpc_response(sessfd, -1, EBADF);
		return;
	}
	
	// Store at the calculated index
	client_vfds[index].client_vfd = client_vfd;
	client_vfds[index].server_fd = server_fd;
	client_vfds[index].in_use = 1;
		// VFD registered successfully
	
	// Send response: success
	// Response length (4 bytes)
	// Return value: 0 (8 bytes)
	// Errno: 0 (4 bytes)
	send_rpc_response(sessfd, 0, 0);
}

void handle_write(int sessfd) {
	// Unmarshal request
	uint32_t client_vfd_network;
	uint32_t data_len_network;
	int client_vfd;
	uint32_t data_len;
	
	// Receive client_vfd (4 bytes)
	if (recv_all(sessfd, &client_vfd_network, sizeof(client_vfd_network)) < 0) {
		// Connection closed or error
		return;
	}
	client_vfd = (int)ntohl(client_vfd_network);
	
	// Receive data length (4 bytes)
	if (recv_all(sessfd, &data_len_network, sizeof(data_len_network)) < 0) {
		// Connection closed or error
		return;
	}
	data_len = ntohl(data_len_network);
	
	// Allocate buffer for data
	char *data_buf = malloc(data_len);
	if (!data_buf) {
		// Send error response
		send_rpc_response(sessfd, -1, ENOMEM);
		return;
	}
	
	// Receive data buffer (raw bytes)
	if (recv_all(sessfd, data_buf, data_len) < 0) {
		free(data_buf);
		return;  // Connection closed or error
	}
	
	// Map client_vfd to server_fd using client's VFD map
	int server_fd = get_server_fd_from_vfd(client_vfd);
	if (server_fd < 0) {
		// VFD not found
		free(data_buf);
		send_rpc_response(sessfd, -1, EBADF);
		return;
	}
	
	// Call actual write() on server
	ssize_t bytes_written = write(server_fd, data_buf, data_len);
	int saved_errno;
	if (bytes_written >= 0) {
		// Success: errno should be 0
		saved_errno = 0;
	} else {
		// Failure: save the actual errno
		saved_errno = errno;
	}
	
	// Send response
	// Response length (4 bytes)
	// Return value: bytes_written as int64_t (8 bytes)
	// Errno: saved_errno as int32_t (4 bytes)
	int send_result = send_rpc_response(sessfd, (int64_t)bytes_written, (int32_t)saved_errno);
	
	// If send failed, connection is likely closed - return to let main loop handle it
	if (send_result < 0) {
		free(data_buf);
		return;  // Connection closed or error
	}
	
	// Free allocated data buffer
	free(data_buf);
}

void handle_close(int sessfd) {
	// Unmarshal request
	uint32_t client_vfd_network;
	int client_vfd;
	
	// Receive client_vfd (4 bytes)
	if (recv_all(sessfd, &client_vfd_network, sizeof(client_vfd_network)) < 0) {
		// Connection closed or error
		return;
	}
	client_vfd = (int)ntohl(client_vfd_network);
	
	// Map client_vfd to server_fd using client's VFD map
	int server_fd = get_server_fd_from_vfd(client_vfd);
	if (server_fd < 0) {
		// VFD not found
		send_rpc_response(sessfd, -1, EBADF);
		return;
	}
	
	// Call actual close() on server
	int result = close(server_fd);
	int saved_errno;
	if (result == 0) {
		// Success: errno should be 0
		saved_errno = 0;
	} else {
		// Failure: save the actual errno
		saved_errno = errno;
	}
	
	// Remove VFD mapping from client's map
	remove_vfd_mapping(client_vfd);
	
	// Send response
	// Response length (4 bytes)
	// Return value: result as int64_t (8 bytes)
	// Errno: saved_errno as int32_t (4 bytes)
	send_rpc_response(sessfd, (int64_t)result, (int32_t)saved_errno);
}

// Implement READ Handler (Server Side)
void handle_read(int sessfd) {
	// Unmarshal request
	uint32_t client_vfd_network;
	uint32_t count_network;
	int client_vfd;
	uint32_t count;
	
	// Receive client_vfd (4 bytes)
	if (recv_all(sessfd, &client_vfd_network, sizeof(client_vfd_network)) < 0) {
		// Connection closed or error
		return;
	}
	client_vfd = (int)ntohl(client_vfd_network);
	
	// Receive count (4 bytes)
	if (recv_all(sessfd, &count_network, sizeof(count_network)) < 0) {
		// Connection closed or error
		return;
	}
	count = ntohl(count_network);
	
	// Map client_vfd to server_fd using client's VFD map
	int server_fd = get_server_fd_from_vfd(client_vfd);
	if (server_fd < 0) {
		// VFD not found
		send_rpc_response(sessfd, -1, EBADF);
		return;
	}
	
	// Allocate buffer for reading
	char *data_buf = malloc(count);
	if (!data_buf) {
		// Send error response
		send_rpc_response(sessfd, -1, ENOMEM);
		return;
	}
	
	// Call actual read() on server
	ssize_t bytes_read = read(server_fd, data_buf, count);
	int saved_errno = errno;
	
	// Send response
	if (bytes_read > 0) {
		// Success with data: response length = 8 (ret_val) + 4 (errno) + 4 (data_length) + bytes_read
		uint32_t resp_len = 8 + 4 + 4 + (uint32_t)bytes_read;
		uint32_t resp_len_network = htonl(resp_len);
		
		// Send response length (4 bytes, network byte order)
		if (send_all(sessfd, &resp_len_network, sizeof(resp_len_network)) < 0) {
			free(data_buf);
			return;  // Connection closed or error
		}
		
		// Send return value: bytes_read as int64_t (8 bytes, network byte order)
		int64_t bytes_read_network = htobe64((int64_t)bytes_read);
		if (send_all(sessfd, &bytes_read_network, sizeof(bytes_read_network)) < 0) {
			free(data_buf);
			return;  // Connection closed or error
		}
		
		// Send errno: saved_errno as int32_t (4 bytes, network byte order)
		int32_t errno_network = htonl((int32_t)saved_errno);
		if (send_all(sessfd, &errno_network, sizeof(errno_network)) < 0) {
			free(data_buf);
			return;  // Connection closed or error
		}
		
		// Send data_length: bytes_read as uint32_t (4 bytes, network byte order)
		uint32_t data_length_network = htonl((uint32_t)bytes_read);
		if (send_all(sessfd, &data_length_network, sizeof(data_length_network)) < 0) {
			free(data_buf);
			return;  // Connection closed or error
		}
		
		// Send data buffer: raw bytes (bytes_read bytes)
		if (send_all(sessfd, data_buf, (size_t)bytes_read) < 0) {
			free(data_buf);
			return;  // Connection closed or error
		}
	} else {
		// EOF or error: use standard response format (no data)
		send_rpc_response(sessfd, (int64_t)bytes_read, (int32_t)saved_errno);
	}
	
	// Free allocated buffer
	free(data_buf);
}

// Implement LSEEK Handler (Server Side)
void handle_lseek(int sessfd) {
	// Unmarshal request
	uint32_t client_vfd_network;
	int64_t offset_network;
	int32_t whence_network;
	int client_vfd;
	off_t offset;
	int whence;
	
	// Receive client_vfd (4 bytes)
	if (recv_all(sessfd, &client_vfd_network, sizeof(client_vfd_network)) < 0) {
		// Connection closed or error
		return;
	}
	client_vfd = (int)ntohl(client_vfd_network);
	
	// Receive offset (8 bytes as int64_t)
	if (recv_all(sessfd, &offset_network, sizeof(offset_network)) < 0) {
		// Connection closed or error
		return;
	}
	offset = (off_t)be64toh(offset_network);  // Convert from network byte order (big-endian)
	
	// Receive whence (4 bytes as int32_t)
	if (recv_all(sessfd, &whence_network, sizeof(whence_network)) < 0) {
		// Connection closed or error
		return;
	}
	whence = (int)ntohl(whence_network);
	
	// Map client_vfd to server_fd using client's VFD map
	int server_fd = get_server_fd_from_vfd(client_vfd);
	if (server_fd < 0) {
		// VFD not found
		send_rpc_response(sessfd, -1, EBADF);
		return;
	}
	
	// Call actual lseek() on server
	off_t new_pos = lseek(server_fd, offset, whence);
	int saved_errno = errno;
	
	// Send response
	// Response length (4 bytes): 12 bytes (8 + 4)
	// Return value: new_pos as int64_t (8 bytes)
	// Errno: saved_errno as int32_t (4 bytes)
	send_rpc_response(sessfd, (int64_t)new_pos, (int32_t)saved_errno);
}

// Implement STAT Handler (Server Side)
void handle_stat(int sessfd) {
	// Unmarshal request
	uint32_t pathname_len_network;
	uint32_t pathname_len;
	
	// Receive pathname length (4 bytes)
	if (recv_all(sessfd, &pathname_len_network, sizeof(pathname_len_network)) < 0) {
		// Connection closed or error
		return;
	}
	pathname_len = ntohl(pathname_len_network);
	
	// Allocate buffer for pathname (add 1 for null terminator)
	char *pathname = malloc(pathname_len + 1);
	if (!pathname) {
		// Send error response
		send_rpc_response(sessfd, -1, ENOMEM);
		return;
	}
	
	// Receive pathname (raw bytes, no null terminator)
	if (recv_all(sessfd, pathname, pathname_len) < 0) {
		free(pathname);
		return;  // Connection closed or error
	}
	pathname[pathname_len] = '\0';  // Add null terminator
	
	// Call actual stat() on server
	struct stat st;
	int result = stat(pathname, &st);
	int saved_errno = errno;
	
	// Send response
	if (result == 0) {
		// Success: response length = 8 (ret_val) + 4 (errno) + 144 (struct stat)
		uint32_t resp_len = 8 + 4 + 144;
		uint32_t resp_len_network = htonl(resp_len);
		
		// Send response length (4 bytes, network byte order)
		if (send_all(sessfd, &resp_len_network, sizeof(resp_len_network)) < 0) {
			free(pathname);
			return;  // Connection closed or error
		}
		
		// Send return value: result as int64_t (8 bytes, network byte order)
		int64_t result_network = htobe64((int64_t)result);
		if (send_all(sessfd, &result_network, sizeof(result_network)) < 0) {
			free(pathname);
			return;  // Connection closed or error
		}
		
		// Send errno: saved_errno as int32_t (4 bytes, network byte order)
		int32_t errno_network = htonl((int32_t)saved_errno);
		if (send_all(sessfd, &errno_network, sizeof(errno_network)) < 0) {
			free(pathname);
			return;  // Connection closed or error
		}
		
		// Send struct stat: raw bytes (144 bytes)
		if (send_all(sessfd, &st, 144) < 0) {
			free(pathname);
			return;  // Connection closed or error
		}
	} else {
		// Error: use standard response format (no struct stat)
		send_rpc_response(sessfd, (int64_t)result, (int32_t)saved_errno);
	}
	
	// Free allocated pathname buffer
	free(pathname);
}

// Implement __XSTAT Handler (Server Side)
void handle_xstat(int sessfd) {
	// Unmarshal request
	int32_t version_network;
	int32_t version;
	uint32_t pathname_len_network;
	uint32_t pathname_len;
	
	// Receive version (4 bytes as int32_t)
	if (recv_all(sessfd, &version_network, sizeof(version_network)) < 0) {
		// Connection closed or error
		return;
	}
	version = (int32_t)ntohl(version_network);
	
	// Receive pathname length (4 bytes)
	if (recv_all(sessfd, &pathname_len_network, sizeof(pathname_len_network)) < 0) {
		// Connection closed or error
		return;
	}
	pathname_len = ntohl(pathname_len_network);
	
	// Allocate buffer for pathname (add 1 for null terminator)
	char *pathname = malloc(pathname_len + 1);
	if (!pathname) {
		// Send error response
		send_rpc_response(sessfd, -1, ENOMEM);
		return;
	}
	
	// Receive pathname (raw bytes, no null terminator)
	if (recv_all(sessfd, pathname, pathname_len) < 0) {
		free(pathname);
		return;  // Connection closed or error
	}
	pathname[pathname_len] = '\0';  // Add null terminator
	
	// Call actual __xstat() on server (or stat() if __xstat not available)
	struct stat st;
	int result;
	
	// Try to use __xstat() to match the API semantics - the client sends a version parameter
	// On some Linux systems, __xstat might not be directly callable or might reject
	// certain version parameters. Fall back to stat() if __xstat fails.
	result = __xstat(version, pathname, &st);
	if (result < 0 && errno == EINVAL) {
		// __xstat() rejected the version parameter or is not available
		// Fall back to stat() which is equivalent on modern systems
		result = stat(pathname, &st);
	}
	int saved_errno = errno;
	
	// Send response (same format as STAT)
	if (result == 0) {
		// Success: response length = 8 (ret_val) + 4 (errno) + 144 (struct stat)
		uint32_t resp_len = 8 + 4 + 144;
		uint32_t resp_len_network = htonl(resp_len);
		
		// Send response length (4 bytes, network byte order)
		if (send_all(sessfd, &resp_len_network, sizeof(resp_len_network)) < 0) {
			free(pathname);
			return;  // Connection closed or error
		}
		
		// Send return value: result as int64_t (8 bytes, network byte order)
		int64_t result_network = htobe64((int64_t)result);
		if (send_all(sessfd, &result_network, sizeof(result_network)) < 0) {
			free(pathname);
			return;  // Connection closed or error
		}
		
		// Send errno: saved_errno as int32_t (4 bytes, network byte order)
		int32_t errno_network = htonl((int32_t)saved_errno);
		if (send_all(sessfd, &errno_network, sizeof(errno_network)) < 0) {
			free(pathname);
			return;  // Connection closed or error
		}
		
		// Send struct stat: raw bytes (144 bytes)
		if (send_all(sessfd, &st, 144) < 0) {
			free(pathname);
			return;  // Connection closed or error
		}
	} else {
		// Error: use standard response format (no struct stat)
		send_rpc_response(sessfd, (int64_t)result, (int32_t)saved_errno);
	}
	
	// Free allocated pathname buffer
	free(pathname);
}

// Implement UNLINK Handler (Server Side)
void handle_unlink(int sessfd) {
	// Unmarshal request
	uint32_t pathname_len_network;
	uint32_t pathname_len;
	
	// Receive pathname length (4 bytes)
	if (recv_all(sessfd, &pathname_len_network, sizeof(pathname_len_network)) < 0) {
		// Connection closed or error
		return;
	}
	pathname_len = ntohl(pathname_len_network);
	
	// Allocate buffer for pathname (add 1 for null terminator)
	char *pathname = malloc(pathname_len + 1);
	if (!pathname) {
		// Send error response
		send_rpc_response(sessfd, -1, ENOMEM);
		return;
	}
	
	// Receive pathname (raw bytes, no null terminator)
	if (recv_all(sessfd, pathname, pathname_len) < 0) {
		free(pathname);
		return;  // Connection closed or error
	}
	pathname[pathname_len] = '\0';  // Add null terminator
	
	// Call actual unlink() on server
	int result = unlink(pathname);
	int saved_errno = errno;
	
	// Send response
	// Response length (4 bytes): 12 bytes (8 + 4)
	// Return value: result as int64_t (8 bytes)
	// Errno: saved_errno as int32_t (4 bytes)
	send_rpc_response(sessfd, (int64_t)result, (int32_t)saved_errno);
	
	// Free allocated pathname buffer
	free(pathname);
}

// Implement GETDIRENTRIES Handler (Server Side)
void handle_getdirentries(int sessfd) {
	// Unmarshal request
	uint32_t client_vfd_network;
	uint32_t nbytes_network;
	int client_vfd;
	uint32_t nbytes;
	
	// Receive client_vfd (4 bytes)
	if (recv_all(sessfd, &client_vfd_network, sizeof(client_vfd_network)) < 0) {
		// Connection closed or error
		return;
	}
	client_vfd = (int)ntohl(client_vfd_network);
	
	// Receive nbytes (4 bytes)
	if (recv_all(sessfd, &nbytes_network, sizeof(nbytes_network)) < 0) {
		// Connection closed or error
		return;
	}
	nbytes = ntohl(nbytes_network);
	
	// Map client_vfd to server_fd using client's VFD map
	int server_fd = get_server_fd_from_vfd(client_vfd);
	if (server_fd < 0) {
		// VFD not found
		send_rpc_response(sessfd, -1, EBADF);
		return;
	}
	
	// Allocate buffer for reading directory entries
	char *entries_buf = malloc(nbytes);
	if (!entries_buf) {
		// Send error response
		send_rpc_response(sessfd, -1, ENOMEM);
		return;
	}
	
	// Call actual getdirentries() on server
	off_t base = 0;
	ssize_t bytes_read = getdirentries(server_fd, entries_buf, nbytes, &base);
	int saved_errno = errno;
	
	// Send response
	if (bytes_read > 0) {
		// Success with data: response length = 8 (ret_val) + 4 (errno) + 4 (data_length) + 8 (basep) + bytes_read
		uint32_t resp_len = 8 + 4 + 4 + 8 + (uint32_t)bytes_read;
		uint32_t resp_len_network = htonl(resp_len);
		
		// Send response length (4 bytes, network byte order)
		if (send_all(sessfd, &resp_len_network, sizeof(resp_len_network)) < 0) {
			free(entries_buf);
			return;  // Connection closed or error
		}
		
		// Send return value: bytes_read as int64_t (8 bytes, network byte order)
		int64_t bytes_read_network = htobe64((int64_t)bytes_read);
		if (send_all(sessfd, &bytes_read_network, sizeof(bytes_read_network)) < 0) {
			free(entries_buf);
			return;  // Connection closed or error
		}
		
		// Send errno: saved_errno as int32_t (4 bytes, network byte order)
		int32_t errno_network = htonl((int32_t)saved_errno);
		if (send_all(sessfd, &errno_network, sizeof(errno_network)) < 0) {
			free(entries_buf);
			return;  // Connection closed or error
		}
		
		// Send data_length: bytes_read as uint32_t (4 bytes, network byte order)
		uint32_t data_length_network = htonl((uint32_t)bytes_read);
		if (send_all(sessfd, &data_length_network, sizeof(data_length_network)) < 0) {
			free(entries_buf);
			return;  // Connection closed or error
		}
		
		// Send basep: base as int64_t (8 bytes, network byte order)
		int64_t base_network = htobe64((int64_t)base);
		if (send_all(sessfd, &base_network, sizeof(base_network)) < 0) {
			free(entries_buf);
			return;  // Connection closed or error
		}
		
		// Send directory entries buffer: raw bytes (bytes_read bytes)
		if (send_all(sessfd, entries_buf, (size_t)bytes_read) < 0) {
			free(entries_buf);
			return;  // Connection closed or error
		}
	} else {
		// EOF or error: use standard response format (no data)
		send_rpc_response(sessfd, (int64_t)bytes_read, (int32_t)saved_errno);
	}
	
	// Free allocated buffer
	free(entries_buf);
}

// Implement GETDIRTREE Handler (Server Side)
void handle_getdirtree(int sessfd) {
	// Unmarshal request
	uint32_t pathname_len_network;
	uint32_t pathname_len;
	
	// Receive pathname length (4 bytes)
	if (recv_all(sessfd, &pathname_len_network, sizeof(pathname_len_network)) < 0) {
		// Connection closed or error
		return;
	}
	pathname_len = ntohl(pathname_len_network);
	
	// Allocate buffer for pathname (add 1 for null terminator)
	char *pathname = malloc(pathname_len + 1);
	if (!pathname) {
		// Send error response
		send_rpc_response(sessfd, -1, ENOMEM);
		return;
	}
	
	// Receive pathname (raw bytes, no null terminator)
	if (recv_all(sessfd, pathname, pathname_len) < 0) {
		free(pathname);
		return;  // Connection closed or error
	}
	pathname[pathname_len] = '\0';  // Add null terminator
	
	// Call actual getdirtree() on server
	struct dirtreenode *tree = getdirtree(pathname);
	int saved_errno = errno;
	
	// Send response
	if (tree != NULL) {
		// Success: calculate serialized tree size first
		size_t tree_size = calculate_tree_size(tree);
		uint32_t resp_len = 8 + 4 + (uint32_t)tree_size;  // ret_val + errno + tree
		uint32_t resp_len_network = htonl(resp_len);
		
		// Send response length (4 bytes, network byte order)
		if (send_all(sessfd, &resp_len_network, sizeof(resp_len_network)) < 0) {
			freedirtree(tree);
			free(pathname);
			return;  // Connection closed or error
		}
		
		// Send return value: 0 as int64_t (8 bytes, network byte order)
		int64_t ret_val_network = htobe64(0);
		if (send_all(sessfd, &ret_val_network, sizeof(ret_val_network)) < 0) {
			freedirtree(tree);
			free(pathname);
			return;  // Connection closed or error
		}
		
		// Send errno: saved_errno as int32_t (4 bytes, network byte order)
		int32_t errno_network = htonl((int32_t)saved_errno);
		if (send_all(sessfd, &errno_network, sizeof(errno_network)) < 0) {
			freedirtree(tree);
			free(pathname);
			return;  // Connection closed or error
		}
		
		// Serialize and send tree structure
		serialize_dirtree(sessfd, tree);
		
		// Free tree using freedirtree()
		freedirtree(tree);
	} else {
		// Error: use standard response format (no tree)
		send_rpc_response(sessfd, -1, (int32_t)saved_errno);
	}
	
	// Free allocated pathname buffer
	free(pathname);
}

// Serialize directory tree structure in depth-first order
// This function recursively serializes a dirtreenode tree structure
static void serialize_dirtree(int sessfd, struct dirtreenode *node) {
	if (node == NULL) {
		// Handle NULL node: send name length 0 and num_subdirs 0
		uint32_t name_len = 0;
		uint32_t name_len_network = htonl(name_len);
		if (send_all(sessfd, &name_len_network, sizeof(name_len_network)) < 0) {
			return;  // Connection closed or error
		}
		
		uint32_t num_subdirs = 0;
		uint32_t num_subdirs_network = htonl(num_subdirs);
		if (send_all(sessfd, &num_subdirs_network, sizeof(num_subdirs_network)) < 0) {
			return;  // Connection closed or error
		}
		return;
	}
	
	// Send name length (4 bytes, network byte order)
	uint32_t name_len = node->name ? strlen(node->name) : 0;
	uint32_t name_len_network = htonl(name_len);
	if (send_all(sessfd, &name_len_network, sizeof(name_len_network)) < 0) {
		return;  // Connection closed or error
	}
	
	// Send name (raw bytes, no null terminator)
	if (name_len > 0 && node->name) {
		if (send_all(sessfd, node->name, name_len) < 0) {
			return;  // Connection closed or error
		}
	}
	
	// Send num_subdirs (4 bytes, network byte order)
	uint32_t num_subdirs = (uint32_t)node->num_subdirs;
	uint32_t num_subdirs_network = htonl(num_subdirs);
	if (send_all(sessfd, &num_subdirs_network, sizeof(num_subdirs_network)) < 0) {
		return;  // Connection closed or error
	}
	
	// Recursively serialize each subdirectory
	if (node->subdirs != NULL && num_subdirs > 0) {
		for (int i = 0; i < node->num_subdirs; i++) {
			serialize_dirtree(sessfd, node->subdirs[i]);
		}
	}
}

// Handle a single client session (runs in child process after fork)
void handle_client_session(int sessfd) {
	// Initialize per-client VFD mapping table (local to this child process)
	struct client_vfd_map vfd_map[MAX_CLIENT_VFDS];
	client_vfds = vfd_map;  // Set global pointer for handlers to use
	init_vfd_map(client_vfds);
	
	// Disable Nagle's algorithm for low latency (TCP_NODELAY)
	// This is important for rapid small writes
	int flag = 1;
	setsockopt(sessfd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));
	
	// Increase TCP buffer sizes to reduce system calls
	// Larger buffers allow more data to be sent/received per system call
	int bufsize = 256 * 1024;  // 256KB buffers
	setsockopt(sessfd, SOL_SOCKET, SO_SNDBUF, &bufsize, sizeof(bufsize));
	setsockopt(sessfd, SOL_SOCKET, SO_RCVBUF, &bufsize, sizeof(bufsize));
	
	// Main request handling loop
	while (1) {
		char opcode;
		uint32_t msg_len;
		
		// Receive RPC request (length and opcode)
		if (recv_rpc_request(sessfd, &opcode, &msg_len) < 0) {
			// Connection closed or error
			break;  // Exit loop
		}
		
		// Route to appropriate handler based on opcode
		switch (opcode) {
			case 'O': 
				handle_open(sessfd); 
				break;
			case 'V': 
				handle_vfd_register(sessfd); 
				break;
			case 'W': 
				handle_write(sessfd); 
				break;
			case 'C': 
				handle_close(sessfd); 
				break;
			case 'R': 
				handle_read(sessfd); 
				break;
			case 'L': 
				handle_lseek(sessfd); 
				break;
			case 'S': 
				handle_stat(sessfd); 
				break;
			case 'X': 
				handle_xstat(sessfd); 
				break;
			case 'U': 
				handle_unlink(sessfd); 
				break;
			case 'G': 
				handle_getdirentries(sessfd); 
				break;
			case 'T': 
				handle_getdirtree(sessfd); 
				break;
			default: 
				// Unknown operation - exit session
				break;
		}
	}
	
	// Clean up: close session socket
	close(sessfd);
}

int main(int argc, char**argv) {
	char *serverport;
	unsigned short port;
	int sockfd, sessfd, rv;
	struct sockaddr_in srv, cli;
	socklen_t sa_size;
	
	// Get environment variable indicating the port of the server
	serverport = getenv("SERVER_PORT");
	if (serverport) port = (unsigned short)atoi(serverport);
	else port=9090;
	
	// Create socket
	sockfd = socket(AF_INET, SOCK_STREAM, 0);	// TCP/IP socket
	if (sockfd<0) err(1, 0);			// in case of error
	
	// setup address structure to indicate server port
	memset(&srv, 0, sizeof(srv));			// clear it first
	srv.sin_family = AF_INET;			// IP family
	srv.sin_addr.s_addr = htonl(INADDR_ANY);	// don't care IP address
	srv.sin_port = htons(port);			// server port

	// bind to our port
	rv = bind(sockfd, (struct sockaddr*)&srv, sizeof(struct sockaddr));
	if (rv<0) err(1,0);
	
	// start listening for connections
	rv = listen(sockfd, 5);
	if (rv<0) err(1,0);
	
	// main server loop: handle multiple concurrent clients using fork()
	while (1) {
		// wait for next client, get session socket
		sa_size = sizeof(struct sockaddr_in);
		sessfd = accept(sockfd, (struct sockaddr *)&cli, &sa_size);
		if (sessfd < 0) continue;  // Error accepting, continue to next iteration
		
		// Fork a child process to handle this client
		pid_t pid = fork();
		if (pid == 0) {
			// Child process: handle this client
			close(sockfd);  // Child doesn't need listening socket
			handle_client_session(sessfd);
			close(sessfd);
			exit(0);
		} else if (pid > 0) {
			// Parent process: continue accepting new clients
			close(sessfd);  // Parent doesn't need client socket
			// Reap zombie children (non-blocking)
			while (waitpid(-1, NULL, WNOHANG) > 0) {
				// Reaped a zombie child
			}
		} else {
			// Fork failed
			close(sessfd);
			// Continue accepting other clients
		}
	}
	
	// close socket
	close(sockfd);

	return 0;
}
