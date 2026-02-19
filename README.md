# SurfStore

A distributed and decentralized file synchronization system written in Go.

## Build & Run

Build all binaries:
```console
$ make install
```

Run BlockStore server (port 8081):
```console
$ make run-blockstore
```

Run RaftSurfstore server (requires IDX 0, 1, 2):
```console
$ make IDX=0 run-raft
$ make IDX=1 run-raft
$ make IDX=2 run-raft
```

Run tests:
```console
$ make test
```

Run specific test:
```console
$ make TEST_REGEX=TestName specific-test
```

Clean up builds:
```console
$ make clean
```

## Executables

### SurfstoreServerExec
Basic server for BlockStore or MetaStore services.

```
-s <type>    Service type: meta, block, both (required)
-p <port>    Port (default 8080)
-l           Listen on localhost only
-d           Debug mode
```

### SurfstoreRaftServerExec
Raft consensus server for distributed replicated storage.

```
-i <id>      Server ID (required)
-f <file>    Config file path (required)
-d           Debug mode
```

### SurfstoreClientExec
Client for syncing files with the SurfStore cluster.

```
-f <file>    Config file path (required)
-d           Debug mode
```

Positional arguments: `baseDir` `blockSize`

## Example Usage

```bash
# Terminal 1: Start BlockStore
go run cmd/SurfstoreServerExec/main.go -s block -p 8080 -l

# Terminal 2-4: Start 3 Raft nodes
go run cmd/SurfstoreRaftServerExec/main.go -f example_config.txt -i 0
go run cmd/SurfstoreRaftServerExec/main.go -f example_config.txt -i 1
go run cmd/SurfstoreRaftServerExec/main.go -f example_config.txt -i 2

# Terminal 5: Start client
go run cmd/SurfstoreClientExec/main.go -f example_config.txt ./my_dir 4096
```
