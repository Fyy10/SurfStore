package surfstore

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

/* Hash Related */
func GetBlockHashBytes(blockData []byte) []byte {
	h := sha256.New()
	h.Write(blockData)
	return h.Sum(nil)
}

func GetBlockHashString(blockData []byte) string {
	blockHash := GetBlockHashBytes(blockData)
	return hex.EncodeToString(blockHash)
}

/* File Path Related */
func ConcatPath(baseDir, fileDir string) string {
	return baseDir + "/" + fileDir
}

/*
	Writing Local Metadata File Related
*/

const createTable string = `create table if not exists indexes (
		fileName TEXT, 
		version INT,
		hashIndex INT,
		hashValue TEXT
	);`

const insertTuple string = `insert into indexes (fileName, version, hashIndex, hashValue)
		values (?, ?, ?, ?)`

// WriteMetaFile writes the file meta map back to local metadata file index.db
func WriteMetaFile(fileMetas map[string]*FileMetaData, baseDir string) error {
	// remove index.db file if it exists
	outputMetaPath := ConcatPath(baseDir, DEFAULT_META_FILENAME)
	if _, err := os.Stat(outputMetaPath); err == nil {
		e := os.Remove(outputMetaPath)
		if e != nil {
			log.Fatal("Error During Meta Write Back")
		}
	}
	db, err := sql.Open("sqlite3", outputMetaPath)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	statement, err := db.Prepare(createTable)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	statement.Exec()

	// insert tuples
	insertStatement, err := db.Prepare(insertTuple)
	if err != nil {
		log.Fatal("Cannot prepare insert statement, error:", err)
	}

	for _, meta := range fileMetas {
		hashList := meta.GetBlockHashList()
		for hashIdx, hashValue := range hashList {
			_, err = insertStatement.Exec(meta.GetFilename(), meta.GetVersion(), hashIdx, hashValue)
			if err != nil {
				log.Fatal("Cannot insert tuple, error:", err)
			}
		}
	}
	return nil
}

/*
Reading Local Metadata File Related
*/
const getDistinctFileName string = `select distinct fileName from indexes;`

const getTuplesByFileName string = `select * from indexes
		where fileName = ?
		order by hashIndex asc;`

// LoadMetaFromMetaFile loads the local metadata file into a file meta map.
// The key is the file's name and the value is the file's metadata.
// You can use this function to load the index.db file in this project.
func LoadMetaFromMetaFile(baseDir string) (fileMetaMap map[string]*FileMetaData, e error) {
	metaFilePath, _ := filepath.Abs(ConcatPath(baseDir, DEFAULT_META_FILENAME))
	fileMetaMap = make(map[string]*FileMetaData)
	metaFileStats, e := os.Stat(metaFilePath)
	if e != nil || metaFileStats.IsDir() {
		return fileMetaMap, nil
	}
	db, err := sql.Open("sqlite3", metaFilePath)
	if err != nil {
		log.Fatal("Error When Opening Meta")
	}

	// load metadata from index.db
	fileNameRows, err := db.Query(getDistinctFileName)
	if err != nil {
		log.Fatal("Error loading filename")
	}

	for fileNameRows.Next() {
		var fileName string
		var fileVersion int32
		var hashIndex int
		var hashValue string
		file := new(FileMetaData)
		blockHashList := make([]string, 0)

		// get fileName
		fileNameRows.Scan(&fileName)
		fileMetaRows, err := db.Query(getTuplesByFileName, fileName)
		if err != nil {
			log.Fatal("Error loading file meta data")
		}

		// get file meta data
		for fileMetaRows.Next() {
			fileMetaRows.Scan(&fileName, &fileVersion, &hashIndex, &hashValue)
			blockHashList = append(blockHashList, hashValue)
		}
		file.Filename = fileName
		file.Version = fileVersion
		file.BlockHashList = blockHashList
		fileMetaMap[file.Filename] = file
	}
	return fileMetaMap, nil
}

/*
	Debugging Related
*/

// PrintMetaMap prints the contents of the metadata map.
// You might find this function useful for debugging.
func PrintMetaMap(metaMap map[string]*FileMetaData) {

	fmt.Println("--------BEGIN PRINT MAP--------")

	for _, filemeta := range metaMap {
		fmt.Println("\t", filemeta.Filename, filemeta.Version)
		for _, blockHash := range filemeta.BlockHashList {
			fmt.Println("\t", blockHash)
		}
	}

	fmt.Println("---------END PRINT MAP--------")

}
