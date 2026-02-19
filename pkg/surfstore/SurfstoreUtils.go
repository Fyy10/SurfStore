package surfstore

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
)

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	// load local meta map
	localMetaMap, err := LoadMetaFromMetaFile(client.BaseDir)
	if err != nil {
		log.Fatal("Error loading meta file:", err)
	}

	// get remote meta map
	remoteMetaMap := make(map[string]*FileMetaData)
	if err = client.GetFileInfoMap(&remoteMetaMap); err != nil {
		log.Fatal("Cannot get remote meta map:", err)
	}

	// get block store addresses
	var blockStoreAddrs []string
	if err := client.GetBlockStoreAddrs(&blockStoreAddrs); err != nil {
		log.Fatal("cannot get block store addresses:", err)
	}

	// update local meta map according to local files
	dirEntries, err := os.ReadDir(client.BaseDir)
	if err != nil {
		log.Fatal("cannot read directory entries:", err)
	}

	for _, entry := range dirEntries {
		// skip index.db
		if entry.Name() == DEFAULT_META_FILENAME {
			continue
		}

		fileInfo, err := entry.Info()
		if err != nil {
			log.Println("cannot get file info:", err, "filename:", entry.Name())
			continue
		}

		// skip directory
		if fileInfo.IsDir() {
			log.Println("skip sub directory", fileInfo.Name())
			continue
		}

		// prepare file meta
		fileMeta := FileMetaData{
			Filename: fileInfo.Name(),
		}

		filePath := ConcatPath(client.BaseDir, fileInfo.Name())

		blockHashList, err := GetFileHashList(filePath, client.BlockSize)
		if err != nil {
			log.Println("cannot read file", fileInfo.Name()+", skip")
			continue
		}
		fileMeta.BlockHashList = blockHashList

		previousFileMeta, ok := localMetaMap[fileInfo.Name()]
		if !ok {
			log.Println("no previous file named", fileInfo.Name()+", start at version 1")
			fileMeta.Version = 1
		} else {
			// compare block hashes
			hash1 := previousFileMeta.BlockHashList
			hash2 := fileMeta.BlockHashList
			if same := CompareHashList(hash1, hash2); !same {
				// this is a new version
				fileMeta.Version = previousFileMeta.Version + 1
			} else {
				// file not changed
				fileMeta.Version = previousFileMeta.Version
			}
		}
		localMetaMap[fileInfo.Name()] = &fileMeta
	}

	// upload new file blocks and update remote meta map according to local meta map
	for fileName, fileMeta := range localMetaMap {
		filePath := ConcatPath(client.BaseDir, fileName)

		// check if file is deleted
		if _, err := os.Stat(filePath); err != nil {
			if fileMeta.BlockHashList[0] != TOMBSTONE_HASHVALUE {
				// the previous version is not deleted in index.db
				// a new version marked as deleted
				fileMeta.Version++
				fileMeta.BlockHashList = []string{TOMBSTONE_HASHVALUE}
			} else {
				// previous version already deleted the file, no new version
				continue
			}
		}

		// local has the new version
		if fileMeta.Version > remoteMetaMap[fileName].GetVersion() {
			// new file version, upload file blocks

			// check if file is deleted
			deleted := fileMeta.BlockHashList[0] == TOMBSTONE_HASHVALUE
			if _, err := os.Stat(filePath); err == nil && deleted {
				// file marked deleted but still exists
				log.Println("file marked deleted but still exists, filename:", fileName)
			}
			// compute file hash list
			fileHashList, err := GetFileHashList(filePath, client.BlockSize)
			if err != nil && !deleted {
				// file not deleted but cannot get file hash list
				log.Fatal("cannot get file hashes:", err)
			}
			// get file hash map (block hash -> block content)
			fileHashMap, err := GetFileHashMap(filePath, client.BlockSize)
			if err != nil && !deleted {
				// file not deleted but cannot get file hash map
				log.Fatal("cannot get file hash map:", err)
			}

			// fetch block store map (block store address -> dedicated file block hashes)
			blockStoreMap := make(map[string][]string)
			if err := client.GetBlockStoreMap(fileHashList, &blockStoreMap); err != nil {
				log.Fatal("cannot get block store map:", err)
			}

			// upload file blocks to the dedicated block store
			for blockStoreAddr, hashList := range blockStoreMap {
				for _, blockHash := range hashList {
					block := fileHashMap[blockHash]
					var succ bool
					err = client.PutBlock(&Block{BlockData: block, BlockSize: int32(len(block))}, blockStoreAddr, &succ)
					if err != nil {
						log.Println("cannot put block:", err)
					}
				}
			}

			// update remote meta map
			var latestVersion int32
			err = client.UpdateFile(fileMeta, &latestVersion)
			if err != nil {
				log.Println("cannot update remote meta map:", err, "remote has version", latestVersion, "while local has version", fileMeta.GetVersion())
			}
		}
	}

	// download remote files (new file or new version)
	// delete file if needed
	err = client.GetFileInfoMap(&remoteMetaMap)
	if err != nil {
		log.Fatal("cannot get remote meta map:", err)
	}
	for fileName, fileMeta := range remoteMetaMap {
		filePath := ConcatPath(client.BaseDir, fileName)

		// it is possible that remote file updates earlier and local file updates later
		// then the version numbers will be the same but we need to download from remote
		if fileMeta.Version >= localMetaMap[fileName].GetVersion() {
			// local and remote files are the same, no need to download
			if CompareHashList(fileMeta.BlockHashList, localMetaMap[fileName].GetBlockHashList()) {
				continue
			}

			// remote has empty file
			if fileMeta.BlockHashList[0] == EMPTYFILE_HASHVALUE {
				file, err := os.Create(filePath)
				if err != nil {
					log.Fatal("cannot create file:", err)
				}
				file.Close()
				continue
			}

			// remote file removed
			if fileMeta.BlockHashList[0] == TOMBSTONE_HASHVALUE {
				if _, err := os.Stat(filePath); err != nil {
					// file not exist, skip remove
					continue
				}

				if err := os.Remove(filePath); err != nil {
					log.Println("cannot remove file", fileName+", error:", err)
				}
				continue
			}

			/*
				download new file
			*/
			// fetch block store map (block store address -> dedicated file block hashes)
			blockStoreMap := make(map[string][]string)
			if err := client.GetBlockStoreMap(fileMeta.BlockHashList, &blockStoreMap); err != nil {
				log.Fatal("cannot get block store map:", err)
			}
			// download blocks from dedicated block store servers, store in fileHashMap (unordered)
			// fileHashMap: block hash -> block content
			fileHashMap := make(map[string][]byte)
			for blockStoreAddr, hashList := range blockStoreMap {
				for _, blockHash := range hashList {
					var block Block
					err = client.GetBlock(blockHash, blockStoreAddr, &block)
					if err != nil {
						log.Fatal("cannot get block:", err)
					}
					if int(block.BlockSize) != len(block.BlockData) {
						log.Println("block size mismatch")
					}
					fileHashMap[blockHash] = block.BlockData
				}
			}
			// reconstruct file content
			fileContent := make([]byte, 0)
			for _, hashString := range fileMeta.BlockHashList {
				fileContent = append(fileContent, fileHashMap[hashString]...)
			}

			file, err := os.OpenFile(filePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
			if err != nil {
				log.Fatal("cannot open new file:", err, "path:", filePath)
			}

			n, err := file.Write(fileContent)
			if err != nil {
				log.Printf("file has content length %d, but wrote %d bytes, err: %s\n", len(fileContent), n, err.Error())
			}
			file.Close()
		}
	}

	// write remote meta map to local index.db
	WriteMetaFile(remoteMetaMap, client.BaseDir)

	// print meta map
	// fmt.Println("local meta map:")
	// PrintMetaMap(localMetaMap)
	// fmt.Println("remote meta map:")
	// PrintMetaMap(remoteMetaMap)
}

// GetFileHashList returns a slice of hash strings for file blocks of the given size.
// If the file is empty, it returns []string{EMPTYFILE_HASHVALUE}.
func GetFileHashList(filePath string, blockSize int) ([]string, error) {
	if blockSize <= 0 {
		return nil, fmt.Errorf("invalid block size")
	}

	fileContent, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	blockHashList := make([]string, 0)
	if len(fileContent) == 0 {
		// empty file
		blockHashList = append(blockHashList, EMPTYFILE_HASHVALUE)
	} else {
		fileReader := bytes.NewReader(fileContent)
		fileBlock := make([]byte, blockSize)
		for {
			n, err := fileReader.Read(fileBlock)
			if err != nil {
				if err == io.EOF {
					break
				}
				// failed reading from file reader
				return blockHashList, err
			}

			hashString := GetBlockHashString(fileBlock[:n])
			blockHashList = append(blockHashList, hashString)
		}
	}
	return blockHashList, nil
}

// CompareHashList compares two hash lists and returns true if they are identical.
func CompareHashList(hash1, hash2 []string) (same bool) {
	if len(hash1) != len(hash2) {
		return false
	}
	for idx, str := range hash1 {
		if str != hash2[idx] {
			return false
		}
	}
	return true
}

// GetFileBlocks reads a file and returns a slice of byte slices, each representing a block of the specified size.
func GetFileBlocks(filePath string, blockSize int) ([][]byte, error) {
	if blockSize <= 0 {
		return nil, fmt.Errorf("invalid block size")
	}

	fileContent, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	fileBlocks := make([][]byte, 0)
	fileReader := bytes.NewReader(fileContent)
	for {
		fileBlock := make([]byte, blockSize)
		n, err := fileReader.Read(fileBlock)
		if err != nil {
			if err == io.EOF {
				break
			}
			// failed reading from file reader
			return fileBlocks, err
		}

		fileBlocks = append(fileBlocks, fileBlock[:n])
	}
	return fileBlocks, nil
}

// map block hash to block content for a file
func GetFileHashMap(filePath string, blockSize int) (map[string][]byte, error) {
	fileBlocks, err := GetFileBlocks(filePath, blockSize)
	if err != nil {
		return nil, err
	}

	fileHashMap := make(map[string][]byte)
	for _, block := range fileBlocks {
		fileHashMap[GetBlockHashString(block)] = block
	}
	return fileHashMap, nil
}
