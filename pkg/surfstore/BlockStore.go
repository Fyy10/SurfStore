package surfstore

import (
	context "context"
	"fmt"
	"log"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	hashString := blockHash.GetHash()
	block, ok := bs.BlockMap[hashString]
	if !ok {
		// cannot find the block
		log.Println("Cannot find the block, hash string:", hashString)
		return nil, fmt.Errorf("block not found")
	}

	log.Println("get block:", hashString)
	return block, nil
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	if len(block.GetBlockData()) != int(block.GetBlockSize()) {
		return &Success{Flag: false}, fmt.Errorf("inconsistent block size")
	}

	hashString := GetBlockHashString(block.GetBlockData())
	bs.BlockMap[hashString] = block

	log.Println("put block:", hashString)
	return &Success{Flag: true}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	containedStrings := make([]string, 0)
	hashStrings := blockHashesIn.GetHashes()
	for _, str := range hashStrings {
		if _, ok := bs.BlockMap[str]; ok {
			containedStrings = append(containedStrings, str)
		}
	}
	return &BlockHashes{Hashes: containedStrings}, nil
}

// Return a list containing all blockHashes on this block server
func (bs *BlockStore) GetBlockHashes(ctx context.Context, _ *emptypb.Empty) (*BlockHashes, error) {
	hashes := make([]string, 0)
	for k := range bs.BlockMap {
		hashes = append(hashes, k)
	}

	return &BlockHashes{Hashes: hashes}, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
