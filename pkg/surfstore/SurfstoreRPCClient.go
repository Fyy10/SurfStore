package surfstore

import (
	context "context"
	"fmt"
	"log"
	"os"
	"time"

	grpc "google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// const (
// 	findLeaderInterval = 500 * time.Millisecond
// )

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir        string
	BlockSize      int
}

// GetBlock retrieves a block from the specified block store address.
func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	// close the connection
	return conn.Close()
}

// PutBlock stores a block at the specified block store address.
func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	// connect to the block store
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	success, err := c.PutBlock(ctx, block)
	if err != nil {
		conn.Close()
		return err
	}
	*succ = success.GetFlag()

	// close the connection
	return conn.Close()
}

// HasBlocks checks which of the given block hashes exist in the block store.
func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	// connect to the block store
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	hashesOut, err := c.HasBlocks(ctx, &BlockHashes{Hashes: blockHashesIn})
	if err != nil {
		conn.Close()
		return err
	}
	*blockHashesOut = hashesOut.GetHashes()

	// close the connection
	return conn.Close()
}

// GetBlockHashes retrieves all block hashes stored in the block store.
func (surfClient *RPCClient) GetBlockHashes(blockStoreAddr string, blockHashes *[]string) error {
	// connect to the block store
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	containedBlockHashes, err := c.GetBlockHashes(ctx, &emptypb.Empty{})
	if err != nil {
		conn.Close()
		return err
	}
	*blockHashes = containedBlockHashes.GetHashes()

	// close the connection
	return conn.Close()
}

// GetFileInfoMap retrieves the file info map from the meta store via leader.
func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	// connect to the meta store
	leaderId, err := surfClient.findLeader()
	if err != nil {
		log.Println("cannot find the leader:", err)
		return err
	}
	// use the leader addr
	conn, err := grpc.Dial(surfClient.MetaStoreAddrs[leaderId], grpc.WithInsecure())
	if err != nil {
		return err
	}
	// c := NewMetaStoreClient(conn)
	c := NewRaftSurfstoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	fileInfoMap, err := c.GetFileInfoMap(ctx, &emptypb.Empty{})
	if err != nil {
		conn.Close()
		return err
	}
	for k, v := range fileInfoMap.GetFileInfoMap() {
		(*serverFileInfoMap)[k] = v
	}

	// close the connection
	return conn.Close()
}

// UpdateFile updates the file metadata on the meta store via leader.
func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	// connect to the meta store
	leaderId, err := surfClient.findLeader()
	if err != nil {
		log.Println("cannot find the leader:", err)
		return err
	}
	// use the leader addr
	conn, err := grpc.Dial(surfClient.MetaStoreAddrs[leaderId], grpc.WithInsecure())
	if err != nil {
		return err
	}
	// c := NewMetaStoreClient(conn)
	c := NewRaftSurfstoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ver, err := c.UpdateFile(ctx, fileMetaData)
	if err != nil {
		conn.Close()
		return err
	}
	*latestVersion = ver.GetVersion()

	// close the connection
	return conn.Close()
}

// GetBlockStoreMap retrieves the mapping of block store addresses to block hashes.
func (surfClient *RPCClient) GetBlockStoreMap(blockHashesIn []string, blockStoreMap *map[string][]string) error {
	// connect to the meta store
	leaderId, err := surfClient.findLeader()
	if err != nil {
		log.Println("cannot find the leader:", err)
		return err
	}
	// use the leader addr
	conn, err := grpc.Dial(surfClient.MetaStoreAddrs[leaderId], grpc.WithInsecure())
	if err != nil {
		return err
	}
	// c := NewMetaStoreClient(conn)
	c := NewRaftSurfstoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	blockMap, err := c.GetBlockStoreMap(ctx, &BlockHashes{Hashes: blockHashesIn})
	if err != nil {
		conn.Close()
		return err
	}

	for k, v := range blockMap.GetBlockStoreMap() {
		(*blockStoreMap)[k] = v.Hashes
	}

	// close the connection
	return conn.Close()
}

// GetBlockStoreAddrs retrieves the list of block store addresses.
func (surfClient *RPCClient) GetBlockStoreAddrs(blockStoreAddrs *[]string) error {
	// connect to the meta store
	leaderId, err := surfClient.findLeader()
	if err != nil {
		log.Println("cannot find the leader:", err)
		return err
	}
	// use the leader addr
	conn, err := grpc.Dial(surfClient.MetaStoreAddrs[leaderId], grpc.WithInsecure())
	if err != nil {
		return err
	}
	// c := NewMetaStoreClient(conn)
	c := NewRaftSurfstoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	addrs, err := c.GetBlockStoreAddrs(ctx, &emptypb.Empty{})
	if err != nil {
		conn.Close()
		return err
	}
	*blockStoreAddrs = addrs.GetBlockStoreAddrs()

	// close the connection
	return conn.Close()
}

// findLeader finds the leader id by attempting to connect to the meta store servers.
// It returns the leader index if found, or an error if the majority of servers are crashed.
func (surfClient *RPCClient) findLeader() (int, error) {
	// leaderId := -1
	// err := error(nil)
	// for {
	// 	leaderId, _ = surfClient._findLeader()
	// 	if leaderId >= 0 && err == nil {
	// 		break
	// 	}
	// 	time.Sleep(findLeaderInterval)
	// }
	// return leaderId, nil

	// no retry
	return surfClient._findLeader()
}

// _findLeader is an internal method that searches for the leader by sending heartbeats to all servers.
func (surfClient *RPCClient) _findLeader() (int, error) {
	leaderId := -1
	crashed := 0

	for idx, addr := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			crashed++
			conn.Close()
			continue
		}

		c := NewRaftSurfstoreClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		succ, err := c.SendHeartbeat(ctx, &emptypb.Empty{})
		// crashed
		if err != nil {
			crashed++
			conn.Close()
			continue
		}

		// is leader
		if succ.Flag {
			leaderId = idx
			conn.Close()
			continue
		}

		conn.Close()
	}

	log.Println("leader id:", leaderId, "crashed:", crashed)

	if crashed > len(surfClient.MetaStoreAddrs)/2 {
		log.Println("majority crashed")
		// client exits 1 if the majority of servers are crashed
		os.Exit(1)
		// actually unreachable
		return -1, fmt.Errorf("majority crashed")
	}

	if leaderId < 0 {
		return -1, fmt.Errorf("no leader found")
	}

	return leaderId, nil
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// NewSurfstoreRPCClient creates a new Surfstore RPC client with the given parameters.
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {

	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:        baseDir,
		BlockSize:      blockSize,
	}
}
