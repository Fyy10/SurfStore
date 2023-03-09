package surfstore

import (
	"bytes"
	"fmt"
	"os"
	"testing"
)

func TestGetFileHashList(t *testing.T) {
	blockSize := 64
	filePath := "BlockStore.go"
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		t.Fatal("file not exist:", err)
	}

	hashList, err := GetFileHashList(filePath, blockSize)
	if err != nil {
		t.Fatal("cannot get hash list:", err)
	}

	if fileInfo.Size() == 0 {
		if len(hashList) != 1 || hashList[0] != EMPTYFILE_HASHVALUE {
			t.Error("empty file should have one hash value \"" + EMPTYFILE_HASHVALUE + "\"")
		}
	} else {
		if int(fileInfo.Size())%blockSize == 0 {
			if len(hashList) != int(fileInfo.Size())/blockSize {
				t.Errorf("expected hash list of length %d but got %d", int(fileInfo.Size())/blockSize, len(hashList))
			}
		} else {
			if len(hashList) != int(fileInfo.Size())/blockSize+1 {
				t.Errorf("expected hash list of length %d but got %d", int(fileInfo.Size())/blockSize+1, len(hashList))
			}
		}
	}

	fileBlocks, err := GetFileBlocks(filePath, blockSize)
	if err != nil {
		t.Fatal("cannot get file blocks:", err)
	}
	if len(fileBlocks) != len(hashList) {
		t.Errorf("length mismatch, fileBlocks has length %d while hashList has length %d", len(fileBlocks), len(hashList))
	}
	for idx, hashString := range hashList {
		block := fileBlocks[idx]
		if hashString != GetBlockHashString(block) {
			t.Error("hash string mismatch")
		}
	}
}

func TestCompareHashList(t *testing.T) {
	blockSize := 64

	hash1, err := GetFileHashList("BlockStore.go", blockSize)
	if err != nil {
		t.Fatal("cannot get hash list:", err)
	}

	hash2, err := GetFileHashList("BlockStore.go", blockSize)
	if err != nil {
		t.Fatal("cannot get hash list:", err)
	}

	hash3, err := GetFileHashList("MetaStore.go", blockSize)
	if err != nil {
		t.Fatal("cannot get hash list:", err)
	}

	if same := CompareHashList(hash1, hash2); !same {
		t.Error("expected same=true, got", same)
	}
	if same := CompareHashList(hash1, hash3); same {
		t.Error("expected same=false, got", same)
	}
}

func TestGetFileBlocks(t *testing.T) {
	filePath := "BlockStore.go"
	blockSize := 64
	fileBlocks, err := GetFileBlocks(filePath, blockSize)
	if err != nil {
		t.Fatal("cannot get file blocks:", err)
	}

	originalContent, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatal("cannot read file content:", err)
	}

	blockContent := make([]byte, 0)
	for _, block := range fileBlocks {
		blockContent = append(blockContent, block...)
	}

	if !bytes.Equal(originalContent, blockContent) {
		t.Error("block content is different from file content")
	}
}

func TestGetFileHashMap(t *testing.T) {
	filePath := "BlockStore.go"
	blockSize := 64
	fileHashMap, err := GetFileHashMap(filePath, blockSize)
	if err != nil {
		t.Fatal("cannot get file hash map:", err)
	}

	for hash, block := range fileHashMap {
		fmt.Printf("%v: %q\n", hash, block)
	}

	fileHashList, err := GetFileHashList(filePath, blockSize)
	if err != nil {
		t.Fatal("cannot get file hash list:", err)
	}

	originalContent, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatal("cannot read file content:", err)
	}

	blockContent := make([]byte, 0)
	for _, blockHash := range fileHashList {
		blockContent = append(blockContent, fileHashMap[blockHash]...)
	}

	if !bytes.Equal(originalContent, blockContent) {
		t.Error("block content is different from file content")
	}
}
