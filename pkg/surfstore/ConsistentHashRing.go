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
	// ServerMap maps the server address hash to the server address
	ServerMap map[string]string

	// a sorted string slice of address hashes
	sortedKeys []string
}

// GetResponsibleServer finds the dedicated server address for the given block hash, returns "" if no responsible server (block store should not store this block)
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

// Hash computes the SHA256 hash string of an address string
func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))
}

// NewConsistentHashRing creates a new ConsistentHashRing with the given server addresses.
// It initializes the server map and sorted keys for consistent hashing.
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
