package main

import (
	"flag"
	"fmt"
	"github.com/Fyy10/SurfStore/pkg/surfstore"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"
	"strings"

	"google.golang.org/grpc"
)

// Usage String
const UsageString = "./run-server.sh -s <service_type> -p <port> -l -d (blockStoreAddr*)"

// Set of valid services
var ServiceTypes = map[string]bool{"meta": true, "block": true, "both": true}

// Exit codes
const ExUsage int = 64

func main() {
	// Custom flag Usage message
	flag.Usage = func() {
		w := flag.CommandLine.Output()
		fmt.Fprintf(w, "Usage of %s:\n", UsageString)
		flag.VisitAll(func(f *flag.Flag) {
			fmt.Fprintf(w, "  -%s: %v\n", f.Name, f.Usage)
		})
		fmt.Fprintf(w, "  (blockStoreAddr*): BlockStore Address (include self if service type is both)\n")
	}

	// Parse command-line argument flags
	service := flag.String("s", "", "(required) Service Type of the Server: meta, block, both")
	port := flag.Int("p", 8080, "(default = 8080) Port to accept connections")
	localOnly := flag.Bool("l", false, "Only listen on localhost")
	debug := flag.Bool("d", false, "Output log statements")
	flag.Parse()

	// Use tail arguments to hold BlockStore address
	args := flag.Args()
	blockStoreAddrs := []string{}
	if len(args) >= 1 {
		blockStoreAddrs = args
	}

	// Valid service type argument
	if _, ok := ServiceTypes[strings.ToLower(*service)]; !ok {
		flag.Usage()
		os.Exit(ExUsage)
	}

	// Add localhost if necessary
	addr := ""
	if *localOnly {
		addr += "localhost"
	}
	addr += ":" + strconv.Itoa(*port)

	// Disable log outputs if debug flag is missing
	if !(*debug) {
		log.SetFlags(0)
		log.SetOutput(ioutil.Discard)
	}

	log.Fatal(startServer(addr, strings.ToLower(*service), blockStoreAddrs))
}

func startServer(hostAddr string, serviceType string, blockStoreAddrs []string) error {
	server := grpc.NewServer()

	// register server according to service type
	switch serviceType {
	case "meta":
		// register meta store server
		surfstore.RegisterMetaStoreServer(server, surfstore.NewMetaStore(blockStoreAddrs))
	case "block":
		// register block store server
		surfstore.RegisterBlockStoreServer(server, surfstore.NewBlockStore())
	case "both":
		// register both meta store server and block store server
		surfstore.RegisterMetaStoreServer(server, surfstore.NewMetaStore(blockStoreAddrs))
		surfstore.RegisterBlockStoreServer(server, surfstore.NewBlockStore())
	default:
		// not a valid service type, this will never happen
		log.Println("invalid service type:", serviceType)
		return fmt.Errorf("invalid service type %q", serviceType)
	}

	// create listener
	listener, err := net.Listen("tcp", hostAddr)
	if err != nil {
		log.Println("failed creating listener:", err)
		return err
	}

	return server.Serve(listener)
}
