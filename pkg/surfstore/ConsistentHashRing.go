package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
)

const (
	BlockStoreHashPrefix = "blockstore"
)

type ConsistentHashRing struct {
	// map the server address hash to the server address
	ServerMap map[string]string

	// a sorted string slice of address hashes
	sortedKeys []string
}

// from block hash to the dedicated server address, return "" if no responsible server (block store should not store this block)
func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {
	if blockId == EMPTYFILE_HASHVALUE {
		return ""
	}

	if blockId == c.Hash("") {
		return ""
	}

	idx := sort.SearchStrings(c.sortedKeys, blockId)
	responsibleKey := c.sortedKeys[idx%len(c.sortedKeys)]
	return c.ServerMap[responsibleKey]
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))
}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	hashRing := &ConsistentHashRing{
		ServerMap:  map[string]string{},
		sortedKeys: []string{},
	}

	for _, addr := range serverAddrs {
		addrHash := hashRing.Hash(BlockStoreHashPrefix + addr)
		hashRing.ServerMap[addrHash] = addr
		hashRing.sortedKeys = append(hashRing.sortedKeys, addrHash)
	}

	sort.Strings(hashRing.sortedKeys)

	return hashRing
}
