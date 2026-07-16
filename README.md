# Remote Filesystem over RPC

A transparent remote file-access system in **C**: unmodified programs can read and write files that
actually live on a remote server, with no changes to their source. Achieved by **interposing** on the
C library's file calls and forwarding them over a custom RPC protocol.

> **Deep-dive highlight —** the **custom binary RPC protocol** and the `send_all`/`recv_all`
> streaming loop: requests are length-prefixed and opcode-tagged in network byte order, and every
> socket read/write loops until the full message is transferred to survive partial TCP reads/writes.
> This is the low-level networking + Linux systems core worth walking through line by line.

## How it works

Using `LD_PRELOAD`, a client-side library **intercepts** libc file operations (`open`, `read`,
`write`, `close`, `lseek`, `stat`, `unlink`, `getdirentries`, directory-tree walks, …). Instead of
touching the local disk, each call is packaged into an RPC request, sent to the server, executed
there, and the result is marshalled back — all invisibly to the application.

```
   unmodified app
        │  libc file calls
        ▼
   mylib.c (LD_PRELOAD interposition)
        │  custom binary RPC (length-prefixed, opcode-tagged, network byte order)
        ▼
   server.c  ── executes the real file op on the server's disk ──► response
```

## Highlights

- **Transparent interposition** of a dozen file/directory syscalls via `LD_PRELOAD`
- **Custom binary RPC protocol** — length-prefixed framing, per-operation opcodes, manual
  marshalling/unmarshalling, network byte order (no IDL/XDR)
- **Virtual file-descriptor layer** — client FDs are mapped to server FDs, with local/system FDs
  (sockets, stdout) passing through untouched
- **Concurrent server** — a forked handler per client supports multiple simultaneous clients and files
- **Robust streaming** — `send_all`/`recv_all` handle partial TCP reads/writes; large messages and
  recursive directory-tree serialization supported

## Files

| File | Role |
|---|---|
| `mylib.c` | Client interposition library (the `LD_PRELOAD` shim + client stubs) |
| `client.c` | Client-side RPC helpers / connection handling |
| `server.c` | RPC server that executes file operations |
| `client.h` | Shared client declarations |
| `Makefile` | Build |

## Build & run

```bash
make
# Server:
SERVER_PORT=9090 ./server
# Client (any program):
SERVER_HOST=127.0.0.1 SERVER_PORT=9090 LD_PRELOAD=./mylib.so <your-program>
```

## Key code to look at

The networking + protocol core lives in `client.c`:

| Lines | What it is |
|---|---|
| `119-134` | `send_all()` — loops until every byte is sent (handles short writes) |
| `137-150` | `recv_all()` — loops until the full message arrives (handles short reads / closed conn) |
| `152-175` | `send_rpc_request()` — the framing: 4-byte length prefix + 1-byte opcode + payload |
| `181-219` | `recv_rpc_response()` — parses the frame back, validates length, converts byte order |
| `46-116` | `connect_to_server()` — TCP setup incl. `TCP_NODELAY` and larger socket buffers |
| `1113-1206` | `deserialize_dirtree()` — recursive deserialization of a directory tree over the wire |

## Tech Stack

C · TCP sockets · `LD_PRELOAD` interposition · custom RPC protocol · concurrent (fork-per-client) servers
