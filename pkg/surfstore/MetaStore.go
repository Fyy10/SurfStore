package surfstore

import (
	context "context"
	"fmt"
	"log"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap        map[string]*FileMetaData
	BlockStoreAddrs    []string
	ConsistentHashRing *ConsistentHashRing
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	oldMetaData, ok := m.FileMetaMap[fileMetaData.Filename]
	if !ok {
		// no previous version
		// version mismatch
		if fileMetaData.Version != 1 {
			return &Version{Version: 0}, fmt.Errorf(
				"the new file version %d is not exactly the next version (should be 1)",
				fileMetaData.Version)
		}
	} else {
		// has previous version
		// version mismatch
		if oldMetaData.Version+1 != fileMetaData.Version {
			return &Version{Version: oldMetaData.Version}, fmt.Errorf(
				"the new file version %d is not exactly the next version, server has version %d",
				fileMetaData.Version,
				oldMetaData.Version)
		}
	}

	// otherwise, update meta data
	m.FileMetaMap[fileMetaData.Filename] = &FileMetaData{
		Filename:      fileMetaData.Filename,
		Version:       fileMetaData.Version,
		BlockHashList: fileMetaData.BlockHashList,
	}

	log.Printf("update meta store, filename: %s, version: %d, hash0: %s\n",
		fileMetaData.Filename, fileMetaData.Version, fileMetaData.BlockHashList[0])

	// fmt.Println("updated map:")
	// PrintMetaMap(m.FileMetaMap)
	// fmt.Println()

	return &Version{Version: fileMetaData.Version}, nil
}

// BlockStoreMap maps block server address to the dedicated block hashes that the block server should store
func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	blockStoreMap := &BlockStoreMap{
		BlockStoreMap: map[string]*BlockHashes{},
	}

	for _, addr := range m.BlockStoreAddrs {
		blockStoreMap.BlockStoreMap[addr] = &BlockHashes{Hashes: make([]string, 0)}
	}

	for _, blockId := range blockHashesIn.Hashes {
		serverAddr := m.ConsistentHashRing.GetResponsibleServer(blockId)
		if serverAddr != "" {
			blockStoreMap.BlockStoreMap[serverAddr].Hashes = append(blockStoreMap.BlockStoreMap[serverAddr].Hashes, blockId)
		}
	}

	return blockStoreMap, nil
}

func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	return &BlockStoreAddrs{BlockStoreAddrs: m.BlockStoreAddrs}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddrs []string) *MetaStore {
	return &MetaStore{
		FileMetaMap:        map[string]*FileMetaData{},
		BlockStoreAddrs:    blockStoreAddrs,
		ConsistentHashRing: NewConsistentHashRing(blockStoreAddrs),
	}
}
