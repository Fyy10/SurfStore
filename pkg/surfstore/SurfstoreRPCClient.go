package surfstore

import (
	context "context"
	"time"

	grpc "google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir        string
	BlockSize      int
}

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

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	// connect to the meta store
	// TODO: use the leader addr
	conn, err := grpc.Dial(surfClient.MetaStoreAddrs[0], grpc.WithInsecure())
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

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	// connect to the meta store
	// TODO: use the leader addr
	conn, err := grpc.Dial(surfClient.MetaStoreAddrs[0], grpc.WithInsecure())
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

func (surfClient *RPCClient) GetBlockStoreMap(blockHashesIn []string, blockStoreMap *map[string][]string) error {
	// connect to the meta store
	// TODO: use the leader addr
	conn, err := grpc.Dial(surfClient.MetaStoreAddrs[0], grpc.WithInsecure())
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

func (surfClient *RPCClient) GetBlockStoreAddrs(blockStoreAddrs *[]string) error {
	// connect to the meta store
	// TODO: use the leader addr
	conn, err := grpc.Dial(surfClient.MetaStoreAddrs[0], grpc.WithInsecure())
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

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {

	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:        baseDir,
		BlockSize:      blockSize,
	}
}
