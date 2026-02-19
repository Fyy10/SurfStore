package surfstore

import (
	"bufio"
	"encoding/json"
	"io"
	"log"
	"net"
	"os"
	"sync"

	"google.golang.org/grpc"
)

type RaftConfig struct {
	RaftAddrs  []string
	BlockAddrs []string
}

// LoadRaftConfigFile loads the Raft configuration from the specified JSON file.
func LoadRaftConfigFile(filename string) (cfg RaftConfig) {
	configFD, e := os.Open(filename)
	if e != nil {
		log.Fatal("Error Open config file:", e)
	}
	defer configFD.Close()

	configReader := bufio.NewReader(configFD)
	decoder := json.NewDecoder(configReader)

	if err := decoder.Decode(&cfg); err == io.EOF {
		return
	} else if err != nil {
		log.Fatal(err)
	}
	return
}

// NewRaftServer creates a new RaftSurfstore server with the given ID and configuration.
func NewRaftServer(id int64, config RaftConfig) (*RaftSurfstore, error) {
	// TODO Any initialization you need here

	isLeaderMutex := sync.RWMutex{}
	isCrashedMutex := sync.RWMutex{}

	server := RaftSurfstore{
		id:          id,
		peers:       config.RaftAddrs,
		commitIndex: -1,
		lastApplied: -1,
		// FIXME: leader data structures
		// nextIndex:      make([]int, len(config.RaftAddrs)),
		// matchIndex:     make([]int, len(config.RaftAddrs)),
		isLeader:       false,
		isLeaderMutex:  &isLeaderMutex,
		term:           0,
		metaStore:      NewMetaStore(config.BlockAddrs),
		log:            make([]*UpdateOperation, 0),
		isCrashed:      false,
		isCrashedMutex: &isCrashedMutex,
	}

	return &server, nil
}

// ServeRaftServer starts the Raft server and any services.
// TODO: Start up the Raft server and any services here
func ServeRaftServer(server *RaftSurfstore) error {
	grpcServer := grpc.NewServer()
	RegisterRaftSurfstoreServer(grpcServer, server)

	// create listener
	listener, err := net.Listen("tcp", server.peers[server.id])
	if err != nil {
		log.Println("failed creating listener:", err)
		return err
	}

	return grpcServer.Serve(listener)
}
