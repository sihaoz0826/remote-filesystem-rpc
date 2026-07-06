/*
 * RPC client stub library.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <stdint.h>
#include <endian.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <time.h>
#include <netinet/tcp.h>
#include "client.h"
#include "../include/dirtree.h"

// Global socket for server connection
static int server_socket = -1;  // -1 means not connected yet

// VFD mapping structure
struct vfd_mapping {
	int client_vfd;
	int server_fd;
	int in_use;  // 1 if allocated, 0 if free
};

// VFD mapping array
static struct vfd_mapping vfd_map[MAX_VFDS];

// Function pointer to original close (set by mylib.c)
// This allows us to close internal sockets without logging
int (*orig_close_ptr)(int fd) = NULL;

// Function to set the original close pointer (called from mylib.c)
void set_orig_close(int (*close_func)(int fd)) {
	orig_close_ptr = close_func;
}

// Function to connect to the server
static void connect_to_server(void) {
	if (server_socket >= 0) return;  // Already connected
	
	char *serverip;
	char *serverport;
	unsigned short port;
	int sockfd, rv;
	struct sockaddr_in srv;
	
	// Get environment variable indicating the ip address of the server
	serverip = getenv("SERVER_HOST");
	if (serverip) fprintf(stderr, "Got environment variable SERVER_HOST: %s\n", serverip);
	else {
		fprintf(stderr, "Environment variable SERVER_HOST not found.  Using 127.0.0.1\n");
		serverip = "127.0.0.1";
	}
	
	// Get environment variable indicating the port of the server
	serverport = getenv("SERVER_PORT");
	if (serverport) fprintf(stderr, "Got environment variable SERVER_PORT: %s\n", serverport);
	else {
		fprintf(stderr, "Environment variable SERVER_PORT not found.  Using 9090\n");
		serverport = "9090";
	}
	port = (unsigned short)atoi(serverport);
	
	// Create socket
	sockfd = socket(AF_INET, SOCK_STREAM, 0);
	if (sockfd < 0) {
		fprintf(stderr, "mylib: failed to create socket\n");
		return;
	}
	
	// Disable Nagle's algorithm for low latency (TCP_NODELAY)
	// This is important for rapid small writes
	int flag = 1;
	if (setsockopt(sockfd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag)) < 0) {
		fprintf(stderr, "Warning: Failed to set TCP_NODELAY\n");
	}
	
	// Increase TCP buffer sizes to reduce system calls
	// Larger buffers allow more data to be sent/received per system call
	int bufsize = 256 * 1024;  // 256KB buffers
	if (setsockopt(sockfd, SOL_SOCKET, SO_SNDBUF, &bufsize, sizeof(bufsize)) < 0) {
		fprintf(stderr, "Warning: Failed to set SO_SNDBUF\n");
	}
	if (setsockopt(sockfd, SOL_SOCKET, SO_RCVBUF, &bufsize, sizeof(bufsize)) < 0) {
		fprintf(stderr, "Warning: Failed to set SO_RCVBUF\n");
	}
	
	// Setup address structure to point to server
	memset(&srv, 0, sizeof(srv));
	srv.sin_family = AF_INET;
	srv.sin_addr.s_addr = inet_addr(serverip);
	srv.sin_port = htons(port);
	
	// Connect to the server
	rv = connect(sockfd, (struct sockaddr*)&srv, sizeof(struct sockaddr));
	if (rv < 0) {
		fprintf(stderr, "mylib: failed to connect to server\n");
		// Use original close if available, otherwise use syscall to avoid interposition
		if (orig_close_ptr) {
			orig_close_ptr(sockfd);
		} else {
			syscall(SYS_close, sockfd);
		}
		return;
	}
	
	server_socket = sockfd;
}

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

// Helper function to send complete RPC requests with proper framing
static int send_rpc_request(int sockfd, char opcode, const void *data, size_t data_len) {
	// Calculate total request length: 1 byte (opcode) + data_len
	// Note: The 4-byte length field itself is NOT included in the length value
	uint32_t total_len = 1 + data_len;
	uint32_t len_network = htonl(total_len);
	
	// Send length (4 bytes, network byte order)
	if (send_all(sockfd, &len_network, sizeof(len_network)) < 0) {
		return -1;  // Error
	}
	
	// Send opcode (1 byte)
	if (send_all(sockfd, &opcode, 1) < 0) {
		return -1;  // Error
	}
	
	// Send data using send_all()
	if (data_len > 0 && send_all(sockfd, data, data_len) < 0) {
		return -1;  // Error
	}
	
	return 0;  // Success
}

// Helper function to receive complete RPC responses
// Returns 0 on success, -1 on error
// If remaining_bytes is not NULL, it will be set to the number of additional bytes
// that need to be read after the base response (ret_val + errno)
static int recv_rpc_response(int sockfd, int64_t *ret_val, int32_t *errno_val, uint32_t *remaining_bytes) {
	// Receive response length (4 bytes)
	uint32_t resp_len_network;
	if (recv_all(sockfd, &resp_len_network, sizeof(resp_len_network)) < 0) {
		return -1;  // Error
	}
	
	// Convert from network byte order and validate minimum length
	uint32_t resp_len = ntohl(resp_len_network);
	// Base response format: 8 bytes (ret_val) + 4 bytes (errno) = 12 bytes minimum
	// Note: The 4-byte length field itself is NOT included in the length value
	// Some operations (read, stat, getdirtree) may have additional data after the base response
	if (resp_len < 12) {
		// Invalid response length (too short)
		errno = EPROTO;  // Protocol error
		return -1;
	}
	
	// Receive return value (8 bytes, network byte order)
	int64_t ret_val_network;
	if (recv_all(sockfd, &ret_val_network, sizeof(ret_val_network)) < 0) {
		return -1;  // Error
	}
	*ret_val = be64toh(ret_val_network);  // Convert from network byte order (big-endian)
	
	// Receive errno (4 bytes, network byte order)
	int32_t errno_val_network;
	if (recv_all(sockfd, &errno_val_network, sizeof(errno_val_network)) < 0) {
		return -1;  // Error
	}
	*errno_val = ntohl(errno_val_network);  // Convert from network byte order
	
	// Calculate remaining bytes (if any)
	if (remaining_bytes != NULL) {
		*remaining_bytes = resp_len - 12;  // Total length minus base response (12 bytes)
	}
	
	return 0;  // Success
}

// Function to send function name to server
void send_to_server(const char *func_name) {
	if (server_socket < 0) {
		connect_to_server();
	}
	
	if (server_socket < 0) {
		// Connection failed, skip sending
		return;
	}
	
	// Prepare message: function name + newline
	char msg[100];
	snprintf(msg, sizeof(msg), "%s\n", func_name);
	uint32_t msg_len = strlen(msg);
	
	// Send length prefix (4 bytes, network byte order)
	uint32_t len_network = htonl(msg_len);
	if (send_all(server_socket, &len_network, sizeof(len_network)) < 0) {
		return;  // Send failed
	}
	
	// Send message data
	if (send_all(server_socket, msg, msg_len) < 0) {
		return;  // Send failed
	}
}

// VFD Management Functions

// Allocate a new VFD and store the mapping to server_fd
int allocate_vfd(int server_fd) {
	for (int i = 0; i < MAX_VFDS; i++) {
		if (!vfd_map[i].in_use) {
			vfd_map[i].client_vfd = VFD_BASE + i;
			vfd_map[i].server_fd = server_fd;
			vfd_map[i].in_use = 1;
			return vfd_map[i].client_vfd;
		}
	}
	return -1;  // No free VFD slots
}

// Free a VFD and remove the mapping
void free_vfd(int client_vfd) {
	if (!is_vfd(client_vfd)) {
		return;  // Not a VFD
	}
	
	int index = client_vfd - VFD_BASE;
	if (index >= 0 && index < MAX_VFDS) {
		vfd_map[index].in_use = 0;
		vfd_map[index].client_vfd = 0;
		vfd_map[index].server_fd = 0;
	}
}

// Get the server_fd for a given client_vfd
int get_server_fd(int client_vfd) {
	if (!is_vfd(client_vfd)) {
		return -1;  // Not a VFD
	}
	
	int index = client_vfd - VFD_BASE;
	if (index >= 0 && index < MAX_VFDS && vfd_map[index].in_use) {
		return vfd_map[index].server_fd;
	}
	return -1;  // VFD not found or not in use
}

// Check if a file descriptor is a VFD (and actually allocated)
int is_vfd(int fd) {
	// First check: Is it in the VFD range?
	if (fd < VFD_BASE || fd >= VFD_BASE + MAX_VFDS) {
		return 0;  // Out of range, definitely not a VFD
	}
	
	// Second check: Is it actually allocated?
	int index = fd - VFD_BASE;
	if (index < 0 || index >= MAX_VFDS) {
		return 0;  // Shouldn't happen, but be safe
	}
	
	// Only return true if this VFD is actually in use
	return vfd_map[index].in_use;
}

// RPC Functions

// Implement OPEN RPC (Client Side)
int rpc_open(const char *pathname, int flags, mode_t mode) {
	// Ensure we're connected to the server
	if (server_socket < 0) {
		connect_to_server();
	}
	
	if (server_socket < 0) {
		errno = ECONNREFUSED;
		return -1;  // Connection failed
	}
	
	// Marshal request data
	// Calculate pathname length (without null terminator)
	uint32_t pathname_len = strlen(pathname);
	
	// Prepare marshalled data buffer
	// Format: pathname_len (4 bytes) + pathname (pathname_len bytes) + flags (4 bytes) + mode (4 bytes)
	size_t data_len = 4 + pathname_len + 4 + 4;
	char *data_buf = malloc(data_len);
	if (!data_buf) {
		errno = ENOMEM;
		return -1;
	}
	
	char *ptr = data_buf;
	
	// Pathname length (4 bytes, network byte order)
	uint32_t pathname_len_net = htonl(pathname_len);
	memcpy(ptr, &pathname_len_net, 4);
	ptr += 4;
	
	// Pathname (raw bytes, no null terminator)
	memcpy(ptr, pathname, pathname_len);
	ptr += pathname_len;
	
	// Flags (4 bytes, network byte order)
	uint32_t flags_net = htonl((uint32_t)flags);
	memcpy(ptr, &flags_net, 4);
	ptr += 4;
	
	// Mode (4 bytes, network byte order)
	uint32_t mode_net = htonl((uint32_t)mode);
	memcpy(ptr, &mode_net, 4);
	
	// Send OPEN request ('O') using send_rpc_request()
	if (send_rpc_request(server_socket, 'O', data_buf, data_len) < 0) {
		free(data_buf);
		errno = ECONNRESET;
		return -1;  // Send failed
	}
	
	free(data_buf);
	
	// Receive response
	int64_t ret_val;
	int32_t errno_val;
	if (recv_rpc_response(server_socket, &ret_val, &errno_val, NULL) < 0) {
		errno = ECONNRESET;
		return -1;  // Receive failed
	}
	
	// Check if operation was successful
	if (ret_val >= 0) {
		// Success: allocate VFD and return it
		int server_fd = (int)ret_val;
		int client_vfd = allocate_vfd(server_fd);
		if (client_vfd < 0) {
			errno = EMFILE;  // Too many open files
			return -1;
		}
		return client_vfd;
	} else {
		// Error: set errno from response and return -1
		errno = errno_val;
		return -1;
	}
}

// Implement VFD Registration RPC (Client Side)
int rpc_vfd_register(int client_vfd, int server_fd) {
	// Ensure we're connected to the server
	if (server_socket < 0) {
		connect_to_server();
	}
	
	if (server_socket < 0) {
		errno = ECONNREFUSED;
		return -1;  // Connection failed
	}
	
	// Marshal request data
	// Format: client_vfd (4 bytes) + server_fd (4 bytes)
	size_t data_len = 4 + 4;
	char data_buf[8];
	char *ptr = data_buf;
	
	// client_vfd (4 bytes, network byte order)
	uint32_t client_vfd_net = htonl((uint32_t)client_vfd);
	memcpy(ptr, &client_vfd_net, 4);
	ptr += 4;
	
	// server_fd (4 bytes, network byte order)
	uint32_t server_fd_net = htonl((uint32_t)server_fd);
	memcpy(ptr, &server_fd_net, 4);
	
	// Send VFD registration request ('V') using send_rpc_request()
	if (send_rpc_request(server_socket, 'V', data_buf, data_len) < 0) {
		errno = ECONNRESET;
		return -1;  // Send failed
	}
	
	// Receive response
	int64_t ret_val;
	int32_t errno_val;
	if (recv_rpc_response(server_socket, &ret_val, &errno_val, NULL) < 0) {
		errno = ECONNRESET;
		return -1;  // Receive failed
	}
	
	// Check if operation was successful
	if (ret_val == 0) {
		// Success
		return 0;
	} else {
		// Error: set errno from response and return -1
		errno = errno_val;
		return -1;
	}
}

// Implement CLOSE RPC (Client Side)
int rpc_close(int fd) {
	// Check if FD is a VFD
	if (!is_vfd(fd)) {
		// Not a VFD - this should be handled by caller (mylib.c)
		// Return error to indicate this function doesn't handle non-VFDs
		errno = EBADF;
		return -1;
	}
	
	// Ensure we're connected to the server
	if (server_socket < 0) {
		connect_to_server();
	}
	
	if (server_socket < 0) {
		errno = ECONNREFUSED;
		return -1;  // Connection failed
	}
	
	// Get server_fd using get_server_fd(fd) (for reference, but we send client_vfd)
	int server_fd = get_server_fd(fd);
	if (server_fd < 0) {
		errno = EBADF;
		return -1;  // VFD not found
	}
	
	// Marshal request data
	// Format: client_vfd (4 bytes, network byte order)
	size_t data_len = 4;
	char data_buf[4];
	
	// client_vfd (4 bytes, network byte order)
	uint32_t client_vfd_net = htonl((uint32_t)fd);
	memcpy(data_buf, &client_vfd_net, 4);
	
	// Send CLOSE request ('C') using send_rpc_request()
	if (send_rpc_request(server_socket, 'C', data_buf, data_len) < 0) {
		errno = ECONNRESET;
		return -1;  // Send failed
	}
	
	// Receive response
	int64_t ret_val;
	int32_t errno_val;
	if (recv_rpc_response(server_socket, &ret_val, &errno_val, NULL) < 0) {
		errno = ECONNRESET;
		return -1;  // Receive failed
	}
	
	// Check if operation was successful
	if (ret_val == 0) {
		// Success: free VFD using free_vfd(fd)
		free_vfd(fd);
		return 0;
	} else {
		// Error: set errno from response and return -1
		errno = errno_val;
		return -1;
	}
}

// Implement WRITE RPC (Client Side)
ssize_t rpc_write(int fd, const void *buf, size_t count) {
	// Check if FD is a VFD
	if (!is_vfd(fd)) {
		// Not a VFD - this should be handled by caller (mylib.c)
		// Return error to indicate this function doesn't handle non-VFDs
		errno = EBADF;
		return -1;
	}
	
	// Ensure we're connected to the server
	if (server_socket < 0) {
		connect_to_server();
	}
	
	if (server_socket < 0) {
		errno = ECONNREFUSED;
		return -1;  // Connection failed
	}
	
	// Get server_fd using get_server_fd(fd)
	int server_fd = get_server_fd(fd);
	if (server_fd < 0) {
		errno = EBADF;
		return -1;  // VFD not found
	}
	
	// Marshal request data
	// Format: client_vfd (4 bytes) + data length (4 bytes) + data buffer (count bytes)
	size_t data_len = 4 + 4 + count;
	char *data_buf = malloc(data_len);
	if (!data_buf) {
		errno = ENOMEM;
		return -1;
	}
	
	char *ptr = data_buf;
	
	// client_vfd (4 bytes, network byte order)
	uint32_t client_vfd_net = htonl((uint32_t)fd);
	memcpy(ptr, &client_vfd_net, 4);
	ptr += 4;
	
	// data length (4 bytes, network byte order)
	uint32_t count_net = htonl((uint32_t)count);
	memcpy(ptr, &count_net, 4);
	ptr += 4;
	
	// data buffer (raw bytes, count bytes)
	memcpy(ptr, buf, count);
	
	// Send WRITE request ('W') using send_rpc_request()
	if (send_rpc_request(server_socket, 'W', data_buf, data_len) < 0) {
		free(data_buf);
		errno = ECONNRESET;
		return -1;  // Send failed
	}
	
	free(data_buf);
	
	// Receive response
	int64_t ret_val;
	int32_t errno_val;
	if (recv_rpc_response(server_socket, &ret_val, &errno_val, NULL) < 0) {
		errno = ECONNRESET;
		return -1;  // Receive failed
	}
	
	// Check if operation was successful
	if (ret_val >= 0) {
		// Success: return bytes written
		return (ssize_t)ret_val;
	} else {
		// Error: set errno from response and return -1
		errno = errno_val;
		return -1;
	}
}

// Implement READ RPC (Client Side)
ssize_t rpc_read(int fd, void *buf, size_t count) {
	// Check if FD is a VFD
	if (!is_vfd(fd)) {
		// Not a VFD - this should be handled by caller (mylib.c)
		// Return error to indicate this function doesn't handle non-VFDs
		errno = EBADF;
		return -1;
	}
	
	// Ensure we're connected to the server
	if (server_socket < 0) {
		connect_to_server();
	}
	
	if (server_socket < 0) {
		errno = ECONNREFUSED;
		return -1;  // Connection failed
	}
	
	// Get server_fd using get_server_fd(fd)
	int server_fd = get_server_fd(fd);
	if (server_fd < 0) {
		errno = EBADF;
		return -1;  // VFD not found
	}
	
	// Marshal request data
	// Format: client_vfd (4 bytes) + count (4 bytes)
	size_t data_len = 4 + 4;
	char data_buf[8];
	char *ptr = data_buf;
	
	// client_vfd (4 bytes, network byte order)
	uint32_t client_vfd_net = htonl((uint32_t)fd);
	memcpy(ptr, &client_vfd_net, 4);
	ptr += 4;
	
	// count (4 bytes, network byte order)
	uint32_t count_net = htonl((uint32_t)count);
	memcpy(ptr, &count_net, 4);
	
	// Send READ request ('R') using send_rpc_request()
	if (send_rpc_request(server_socket, 'R', data_buf, data_len) < 0) {
		errno = ECONNRESET;
		return -1;  // Send failed
	}
	
	// Receive response
	int64_t ret_val;
	int32_t errno_val;
	uint32_t remaining_bytes;
	if (recv_rpc_response(server_socket, &ret_val, &errno_val, &remaining_bytes) < 0) {
		errno = ECONNRESET;
		return -1;  // Receive failed
	}
	
	// Check if operation was successful
	if (ret_val > 0) {
		// Success: receive data_length and data buffer
		// First receive data_length (4 bytes)
		uint32_t data_length_network;
		if (recv_all(server_socket, &data_length_network, sizeof(data_length_network)) < 0) {
			errno = ECONNRESET;
			return -1;  // Receive failed
		}
		uint32_t data_length = ntohl(data_length_network);
		
		// Validate: remaining_bytes should be 4 (data_length field) + data_length (actual data)
		if (remaining_bytes != 4 + data_length) {
			errno = EPROTO;  // Protocol error
			return -1;
		}
		
		// Validate: data_length should match ret_val (bytes_read)
		if (data_length != (uint32_t)ret_val) {
			errno = EPROTO;  // Protocol error
			return -1;
		}
		
		// Allocate buffer for data
		char *data_buf = malloc(data_length);
		if (!data_buf) {
			errno = ENOMEM;
			return -1;
		}
		
		// Receive data buffer using recv_all()
		if (recv_all(server_socket, data_buf, data_length) < 0) {
			free(data_buf);
			errno = ECONNRESET;
			return -1;  // Receive failed
		}
		
		// Copy data to user's buffer (up to count bytes)
		size_t bytes_to_copy = (size_t)ret_val < count ? (size_t)ret_val : count;
		memcpy(buf, data_buf, bytes_to_copy);
		
		// Free temporary buffer
		free(data_buf);
		
		// Return bytes read
		return (ssize_t)ret_val;
	} else if (ret_val == 0) {
		// EOF: return 0
		return 0;
	} else {
		// Error: set errno from response and return -1
		errno = errno_val;
		return -1;
	}
}

// Implement LSEEK RPC (Client Side)
off_t rpc_lseek(int fd, off_t offset, int whence) {
	// Check if FD is a VFD
	if (!is_vfd(fd)) {
		// Not a VFD - this should be handled by caller (mylib.c)
		// Return error to indicate this function doesn't handle non-VFDs
		errno = EBADF;
		return -1;
	}
	
	// Ensure we're connected to the server
	if (server_socket < 0) {
		connect_to_server();
	}
	
	if (server_socket < 0) {
		errno = ECONNREFUSED;
		return -1;  // Connection failed
	}
	
	// Get server_fd using get_server_fd(fd)
	int server_fd = get_server_fd(fd);
	if (server_fd < 0) {
		errno = EBADF;
		return -1;  // VFD not found
	}
	
	// Marshal request data
	// Format: client_vfd (4 bytes) + offset (8 bytes as int64_t) + whence (4 bytes as int32_t)
	size_t data_len = 4 + 8 + 4;
	char data_buf[16];
	char *ptr = data_buf;
	
	// client_vfd (4 bytes, network byte order)
	uint32_t client_vfd_net = htonl((uint32_t)fd);
	memcpy(ptr, &client_vfd_net, 4);
	ptr += 4;
	
	// offset (8 bytes as int64_t, network byte order using htobe64())
	int64_t offset_net = htobe64((int64_t)offset);
	memcpy(ptr, &offset_net, 8);
	ptr += 8;
	
	// whence (4 bytes as int32_t, network byte order)
	int32_t whence_net = htonl((int32_t)whence);
	memcpy(ptr, &whence_net, 4);
	
	// Send LSEEK request ('L') using send_rpc_request()
	if (send_rpc_request(server_socket, 'L', data_buf, data_len) < 0) {
		errno = ECONNRESET;
		return -1;  // Send failed
	}
	
	// Receive response
	int64_t ret_val;
	int32_t errno_val;
	if (recv_rpc_response(server_socket, &ret_val, &errno_val, NULL) < 0) {
		errno = ECONNRESET;
		return -1;  // Receive failed
	}
	
	// Check if operation was successful
	if (ret_val >= 0) {
		// Success: return new position
		return (off_t)ret_val;
	} else {
		// Error: set errno from response and return -1
		errno = errno_val;
		return -1;
	}
}

// Implement STAT RPC (Client Side)
int rpc_stat(const char *pathname, struct stat *statbuf) {
	// Ensure we're connected to the server
	if (server_socket < 0) {
		connect_to_server();
	}
	
	if (server_socket < 0) {
		errno = ECONNREFUSED;
		return -1;  // Connection failed
	}
	
	// Marshal request data
	// Calculate pathname length (without null terminator)
	uint32_t pathname_len = strlen(pathname);
	
	// Prepare marshalled data buffer
	// Format: pathname_len (4 bytes) + pathname (pathname_len bytes)
	size_t data_len = 4 + pathname_len;
	char *data_buf = malloc(data_len);
	if (!data_buf) {
		errno = ENOMEM;
		return -1;
	}
	
	char *ptr = data_buf;
	
	// Pathname length (4 bytes, network byte order)
	uint32_t pathname_len_net = htonl(pathname_len);
	memcpy(ptr, &pathname_len_net, 4);
	ptr += 4;
	
	// Pathname (raw bytes, no null terminator)
	memcpy(ptr, pathname, pathname_len);
	
	// Send STAT request ('S') using send_rpc_request()
	if (send_rpc_request(server_socket, 'S', data_buf, data_len) < 0) {
		free(data_buf);
		errno = ECONNRESET;
		return -1;  // Send failed
	}
	
	free(data_buf);
	
	// Receive response
	int64_t ret_val;
	int32_t errno_val;
	uint32_t remaining_bytes;
	if (recv_rpc_response(server_socket, &ret_val, &errno_val, &remaining_bytes) < 0) {
		errno = ECONNRESET;
		return -1;  // Receive failed
	}
	
	// Check if operation was successful
	if (ret_val == 0) {
		// Success: receive struct stat (144 bytes)
		if (remaining_bytes != 144) {
			errno = EPROTO;  // Protocol error
			return -1;
		}
		
		// Receive struct stat using recv_all()
		if (recv_all(server_socket, statbuf, 144) < 0) {
			errno = ECONNRESET;
			return -1;  // Receive failed
		}
		
		// Copy to user's statbuf (already done by recv_all)
		return 0;
	} else {
		// Error: set errno from response and return -1
		errno = errno_val;
		return -1;
	}
}

// Implement __XSTAT RPC (Client Side)
int rpc_xstat(int version, const char *pathname, struct stat *statbuf) {
	// Ensure we're connected to the server
	if (server_socket < 0) {
		connect_to_server();
	}
	
	if (server_socket < 0) {
		errno = ECONNREFUSED;
		return -1;  // Connection failed
	}
	
	// Marshal request data
	// Calculate pathname length (without null terminator)
	uint32_t pathname_len = strlen(pathname);
	
	// Prepare marshalled data buffer
	// Format: version (4 bytes) + pathname_len (4 bytes) + pathname (pathname_len bytes)
	size_t data_len = 4 + 4 + pathname_len;
	char *data_buf = malloc(data_len);
	if (!data_buf) {
		errno = ENOMEM;
		return -1;
	}
	
	char *ptr = data_buf;
	
	// version (4 bytes as int32_t, network byte order)
	int32_t version_net = htonl((int32_t)version);
	memcpy(ptr, &version_net, 4);
	ptr += 4;
	
	// Pathname length (4 bytes, network byte order)
	uint32_t pathname_len_net = htonl(pathname_len);
	memcpy(ptr, &pathname_len_net, 4);
	ptr += 4;
	
	// Pathname (raw bytes, no null terminator)
	memcpy(ptr, pathname, pathname_len);
	
	// Send __XSTAT request ('X') using send_rpc_request()
	if (send_rpc_request(server_socket, 'X', data_buf, data_len) < 0) {
		free(data_buf);
		errno = ECONNRESET;
		return -1;  // Send failed
	}
	
	free(data_buf);
	
	// Receive response
	int64_t ret_val;
	int32_t errno_val;
	uint32_t remaining_bytes;
	if (recv_rpc_response(server_socket, &ret_val, &errno_val, &remaining_bytes) < 0) {
		errno = ECONNRESET;
		return -1;  // Receive failed
	}
	
	// Check if operation was successful
	if (ret_val == 0) {
		// Success: receive struct stat (144 bytes)
		if (remaining_bytes != 144) {
			errno = EPROTO;  // Protocol error
			return -1;
		}
		
		// Receive struct stat using recv_all()
		if (recv_all(server_socket, statbuf, 144) < 0) {
			errno = ECONNRESET;
			return -1;  // Receive failed
		}
		
		// Copy to user's statbuf (already done by recv_all)
		return 0;
	} else {
		// Error: set errno from response and return -1
		errno = errno_val;
		return -1;
	}
}

// Implement UNLINK RPC (Client Side)
int rpc_unlink(const char *pathname) {
	// Ensure we're connected to the server
	if (server_socket < 0) {
		connect_to_server();
	}
	
	if (server_socket < 0) {
		errno = ECONNREFUSED;
		return -1;  // Connection failed
	}
	
	// Marshal request data
	// Calculate pathname length (without null terminator)
	uint32_t pathname_len = strlen(pathname);
	
	// Prepare marshalled data buffer
	// Format: pathname_len (4 bytes) + pathname (pathname_len bytes)
	size_t data_len = 4 + pathname_len;
	char *data_buf = malloc(data_len);
	if (!data_buf) {
		errno = ENOMEM;
		return -1;
	}
	
	char *ptr = data_buf;
	
	// Pathname length (4 bytes, network byte order)
	uint32_t pathname_len_net = htonl(pathname_len);
	memcpy(ptr, &pathname_len_net, 4);
	ptr += 4;
	
	// Pathname (raw bytes, no null terminator)
	memcpy(ptr, pathname, pathname_len);
	
	// Send UNLINK request ('U') using send_rpc_request()
	if (send_rpc_request(server_socket, 'U', data_buf, data_len) < 0) {
		free(data_buf);
		errno = ECONNRESET;
		return -1;  // Send failed
	}
	
	free(data_buf);
	
	// Receive response
	int64_t ret_val;
	int32_t errno_val;
	if (recv_rpc_response(server_socket, &ret_val, &errno_val, NULL) < 0) {
		errno = ECONNRESET;
		return -1;  // Receive failed
	}
	
	// Check if operation was successful
	if (ret_val == 0) {
		// Success
		return 0;
	} else {
		// Error: set errno from response and return -1
		errno = errno_val;
		return -1;
	}
}

// Implement GETDIRENTRIES RPC (Client Side)
ssize_t rpc_getdirentries(int fd, char *buf, size_t nbytes, off_t *basep) {
	// Check if FD is a VFD
	if (!is_vfd(fd)) {
		// Not a VFD - this should be handled by caller (mylib.c)
		// Return error to indicate this function doesn't handle non-VFDs
		errno = EBADF;
		return -1;
	}
	
	// Ensure we're connected to the server
	if (server_socket < 0) {
		connect_to_server();
	}
	
	if (server_socket < 0) {
		errno = ECONNREFUSED;
		return -1;  // Connection failed
	}
	
	// Get server_fd using get_server_fd(fd)
	int server_fd = get_server_fd(fd);
	if (server_fd < 0) {
		errno = EBADF;
		return -1;  // VFD not found
	}
	
	// Marshal request data
	// Format: client_vfd (4 bytes) + nbytes (4 bytes)
	size_t data_len = 4 + 4;
	char data_buf[8];
	char *ptr = data_buf;
	
	// client_vfd (4 bytes, network byte order)
	uint32_t client_vfd_net = htonl((uint32_t)fd);
	memcpy(ptr, &client_vfd_net, 4);
	ptr += 4;
	
	// nbytes (4 bytes, network byte order)
	uint32_t nbytes_net = htonl((uint32_t)nbytes);
	memcpy(ptr, &nbytes_net, 4);
	
	// Send GETDIRENTRIES request ('G') using send_rpc_request()
	if (send_rpc_request(server_socket, 'G', data_buf, data_len) < 0) {
		errno = ECONNRESET;
		return -1;  // Send failed
	}
	
	// Receive response
	int64_t ret_val;
	int32_t errno_val;
	uint32_t remaining_bytes;
	if (recv_rpc_response(server_socket, &ret_val, &errno_val, &remaining_bytes) < 0) {
		errno = ECONNRESET;
		return -1;  // Receive failed
	}
	
	// Check if operation was successful
	if (ret_val > 0) {
		// Success: receive data_length, basep, and directory entries buffer
		// First receive data_length (4 bytes)
		uint32_t data_length_network;
		if (recv_all(server_socket, &data_length_network, sizeof(data_length_network)) < 0) {
			errno = ECONNRESET;
			return -1;  // Receive failed
		}
		uint32_t data_length = ntohl(data_length_network);
		
		// Receive basep (8 bytes as int64_t)
		int64_t basep_network;
		if (recv_all(server_socket, &basep_network, sizeof(basep_network)) < 0) {
			errno = ECONNRESET;
			return -1;  // Receive failed
		}
		off_t base = (off_t)be64toh(basep_network);
		
		// Validate: remaining_bytes should be 4 (data_length) + 8 (basep) + data_length (actual data)
		if (remaining_bytes != 4 + 8 + data_length) {
			errno = EPROTO;  // Protocol error
			return -1;
		}
		
		// Validate: data_length should match ret_val (bytes_read)
		if (data_length != (uint32_t)ret_val) {
			errno = EPROTO;  // Protocol error
			return -1;
		}
		
		// Allocate buffer for directory entries
		char *entries_buf = malloc(data_length);
		if (!entries_buf) {
			errno = ENOMEM;
			return -1;
		}
		
		// Receive directory entries buffer using recv_all()
		if (recv_all(server_socket, entries_buf, data_length) < 0) {
			free(entries_buf);
			errno = ECONNRESET;
			return -1;  // Receive failed
		}
		
		// Copy to user's buffer (up to nbytes bytes)
		size_t bytes_to_copy = (size_t)ret_val < nbytes ? (size_t)ret_val : nbytes;
		memcpy(buf, entries_buf, bytes_to_copy);
		
		// Update basep if provided
		if (basep != NULL) {
			*basep = base;
		}
		
		// Free temporary buffer
		free(entries_buf);
		
		// Return bytes read
		return (ssize_t)ret_val;
	} else if (ret_val == 0) {
		// EOF: return 0
		return 0;
	} else {
		// Error: set errno from response and return -1
		errno = errno_val;
		return -1;
	}
}

// Deserialize directory tree structure in depth-first order
// This function recursively deserializes a dirtreenode tree structure
static struct dirtreenode* deserialize_dirtree(int sockfd) {
	// Receive name length (4 bytes)
	uint32_t name_len_network;
	if (recv_all(sockfd, &name_len_network, sizeof(name_len_network)) < 0) {
		return NULL;  // Connection closed or error
	}
	uint32_t name_len = ntohl(name_len_network);
	
	// Deserialization order must match server's serialization order:
	// Server sends: name_len, name (if name_len > 0), num_subdirs
	// We read: name_len, name, num_subdirs (in that order)
	// Note: We cannot detect NULL nodes (name_len=0 && num_subdirs=0) until after
	// reading both values, so we must read the name first even if name_len is 0.
	
	// Allocate buffer and receive name (add null terminator)
	char *name = NULL;
	if (name_len > 0) {
		name = malloc(name_len + 1);
		if (!name) {
			return NULL;  // Memory allocation failure
		}
		if (recv_all(sockfd, name, name_len) < 0) {
			free(name);
			return NULL;  // Connection closed or error
		}
		name[name_len] = '\0';  // Add null terminator
	} else {
		// Empty name - allocate empty string (name_len == 0 but num_subdirs might be > 0)
		name = malloc(1);
		if (!name) {
			return NULL;  // Memory allocation failure
		}
		name[0] = '\0';
	}
	
	// Receive num_subdirs (4 bytes) - AFTER reading name to match server order
	uint32_t num_subdirs_network;
	if (recv_all(sockfd, &num_subdirs_network, sizeof(num_subdirs_network)) < 0) {
		free(name);
		return NULL;  // Connection closed or error
	}
	uint32_t num_subdirs = ntohl(num_subdirs_network);
	
	// Handle NULL node case: serialize_dirtree sends name_len=0 and num_subdirs=0 for NULL nodes
	if (name_len == 0 && num_subdirs == 0) {
		// This represents a NULL node (not an empty directory name)
		free(name);
		return NULL;
	}
	
	// Allocate node structure
	struct dirtreenode *node = malloc(sizeof(struct dirtreenode));
	if (!node) {
		free(name);
		return NULL;  // Memory allocation failure
	}
	
	node->name = name;
	node->num_subdirs = (int)num_subdirs;
	
	// Allocate subdirs array if needed
	if (num_subdirs > 0) {
		node->subdirs = malloc(num_subdirs * sizeof(struct dirtreenode*));
		if (!node->subdirs) {
			free(name);
			free(node);
			return NULL;  // Memory allocation failure
		}
		
		// Recursively deserialize each subdirectory
		for (int i = 0; i < node->num_subdirs; i++) {
			node->subdirs[i] = deserialize_dirtree(sockfd);
			if (node->subdirs[i] == NULL) {
				// Error during deserialization - clean up what we've allocated so far
				// Free all successfully deserialized subdirectories using freedirtree()
				for (int j = 0; j < i; j++) {
					if (node->subdirs[j] != NULL) {
						freedirtree(node->subdirs[j]);
					}
				}
				free(node->subdirs);
				free(name);
				free(node);
				return NULL;  // Deserialization error
			}
		}
	} else {
		node->subdirs = NULL;
	}
	
	return node;
}

// Implement GETDIRTREE RPC (Client Side)
struct dirtreenode* rpc_getdirtree(const char *pathname) {
	// Ensure we're connected to the server
	if (server_socket < 0) {
		connect_to_server();
	}
	
	if (server_socket < 0) {
		errno = ECONNREFUSED;
		return NULL;  // Connection failed
	}
	
	// Marshal request data
	// Calculate pathname length (without null terminator)
	uint32_t pathname_len = strlen(pathname);
	
	// Prepare marshalled data buffer
	// Format: pathname_len (4 bytes) + pathname (pathname_len bytes)
	size_t data_len = 4 + pathname_len;
	char *data_buf = malloc(data_len);
	if (!data_buf) {
		errno = ENOMEM;
		return NULL;
	}
	
	char *ptr = data_buf;
	
	// Pathname length (4 bytes, network byte order)
	uint32_t pathname_len_net = htonl(pathname_len);
	memcpy(ptr, &pathname_len_net, 4);
	ptr += 4;
	
	// Pathname (raw bytes, no null terminator)
	memcpy(ptr, pathname, pathname_len);
	
	// Send GETDIRTREE request ('T') using send_rpc_request()
	if (send_rpc_request(server_socket, 'T', data_buf, data_len) < 0) {
		free(data_buf);
		errno = ECONNRESET;
		return NULL;  // Send failed
	}
	
	free(data_buf);
	
	// Receive response
	int64_t ret_val;
	int32_t errno_val;
	uint32_t remaining_bytes;
	if (recv_rpc_response(server_socket, &ret_val, &errno_val, &remaining_bytes) < 0) {
		errno = ECONNRESET;
		return NULL;  // Receive failed
	}
	
	// Check if operation was successful
	if (ret_val == 0) {
		// Success: validate that we have tree data to read
		if (remaining_bytes == 0) {
			// No tree data received - this shouldn't happen for a successful getdirtree
			errno = EPROTO;
			return NULL;
		}
		
		// Call deserialize_dirtree() to reconstruct tree
		struct dirtreenode *tree = deserialize_dirtree(server_socket);
		if (tree == NULL) {
			// Deserialization failed - could be protocol error, memory error, or connection issue
			// Check if errno was already set by recv_all() or malloc()
			if (errno == 0) {
				errno = EPROTO;  // Protocol error
			}
			return NULL;
		}
		// Return pointer to root node
		return tree;
	} else {
		// Error: set errno from response and return NULL
		errno = errno_val;
		return NULL;
	}
}
