# Remote Filesystem over RPC

A transparent remote file-access system in **C**: unmodified programs can read and write files that
actually live on a remote server, with no changes to their source. Achieved by **interposing** on the
C library's file calls and forwarding them over a custom RPC protocol.

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

## Tech Stack

C · TCP sockets · `LD_PRELOAD` interposition · custom RPC protocol · concurrent (fork-per-client) servers
