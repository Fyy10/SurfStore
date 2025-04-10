package main

import (
	"flag"
	"fmt"
	"github.com/Fyy10/SurfStore/pkg/surfstore"
	"io/ioutil"
	"log"
	"os"
	"strconv"
)

// Arguments
const ArgCount int = 2

// Usage strings
const (
	UsageString = "./run-client.sh -d -f config_file.txt baseDir blockSize"
	DebugName   = "d"
	DebugUsage  = "Output log statements"

	ConfigName  = "f config_file.txt"
	ConfigUsage = "Path to config file that specifies addresses for all Raft nodes"

	BasedirName  = "baseDir"
	BasedirUsage = "Base directory of the client"

	BlockName  = "blockSize"
	BlockUsage = "Size of the blocks used to fragment files"
)

// Exit codes
const (
	ExUsage int = 64
)

func main() {
	// Custom flag Usage message
	flag.Usage = func() {
		w := flag.CommandLine.Output()
		fmt.Fprintf(w, "Usage of %s:\n", UsageString)
		fmt.Fprintf(w, "  -%s: %v\n", DebugName, DebugUsage)
		fmt.Fprintf(w, "  -%s: %v\n", ConfigName, ConfigUsage)
		fmt.Fprintf(w, "  %s: %v\n", BasedirName, BasedirUsage)
		fmt.Fprintf(w, "  %s: %v\n", BlockName, BlockUsage)
	}

	// Parse command-line arguments and flags
	debug := flag.Bool("d", false, DebugUsage)
	configFile := flag.String("f", "", "(required) Config file")
	flag.Parse()

	// Use tail arguments to hold non-flag arguments
	args := flag.Args()

	if len(args) != ArgCount {
		flag.Usage()
		os.Exit(ExUsage)
	}
	addrs := surfstore.LoadRaftConfigFile(*configFile)

	baseDir := args[0]
	blockSize, err := strconv.Atoi(args[1])
	if err != nil {
		flag.Usage()
		os.Exit(ExUsage)
	}

	log.Println("Client syncing with ", addrs, baseDir, blockSize)

	// Disable log outputs if debug flag is missing
	if !(*debug) {
		log.SetFlags(0)
		log.SetOutput(ioutil.Discard)
	}

	rpcClient := surfstore.NewSurfstoreRPCClient(addrs.RaftAddrs, baseDir, blockSize)
	surfstore.ClientSync(rpcClient)
}
