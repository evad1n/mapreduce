package mapreduce

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
)

type (
	Interface interface {
		Map(key, value string, output chan<- Pair) error
		Reduce(key string, values <-chan string, output chan<- Pair) error
	}

	Pair struct {
		Key   string
		Value string
	}
)

var (
	master     bool
	wait       bool
	masterAddr string
	port       string
	tempdir    string
	M          int
	R          int
	mode       string // Part1/Part2/Main flags

	host string
)

func Start(client Interface) error {
	// FIX: what dis do?
	// TODO: get addr of rpc request client
	// TODO: good buffered channel size?
	runtime.GOMAXPROCS(1)
	log.SetFlags(log.Lshortfile)

	flag.BoolVar(&master, "master", false, "Whether this node is the master or a worker")
	flag.BoolVar(&wait, "wait", false, "Should workers wait for a master signal or start immediately upon joining")
	flag.StringVar(&masterAddr, "masterAddr", "localhost:8080", "Address of the master node")
	flag.StringVar(&port, "port", "8080", "The port to listen on")
	flag.StringVar(&tempdir, "tempdir", filepath.Join("tmp", fmt.Sprintf("mapreduce.%d", os.Getpid())), "The directory to store temporary files in")

	flag.IntVar(&M, "M", 10, "Number of map tasks")
	flag.IntVar(&R, "R", 10, "Number of reduce tasks")

	flag.StringVar(&mode, "mode", "main", "(part1|part2|main) For testing")

	flag.Parse()

	host = "localhost:" + port

	// For serving test
	// localServe(host, filepath.Join("tmp", fmt.Sprintf("mapreduce.%d", 244282)))

	switch mode {
	case "part1":
		if err := part1(); err != nil {
			return fmt.Errorf("part1: %v", err)
		}
		return nil
	case "part2":
		if err := part2(client); err != nil {
			return fmt.Errorf("part2: %v", err)
		}
		return nil
	}

	if master {
		log.Printf("Starting master node on port %s\n", port)
		// Verify input and output db
		if flag.NArg() != 2 {
			fmt.Fprintln(os.Stderr, "USAGE: PROGRAM -master <INPUT_DB> <OUTPUT_DB>")
			return errors.New("specify paths to input and output db at end")
		}
		inputPath := flag.Arg(0)
		outputPath := flag.Arg(1)

		if err := startMaster(client, inputPath, outputPath); err != nil {
			return fmt.Errorf("master failure %v", err)
		}
	} else {
		log.Printf("Starting worker node on port %s\n", port)
		if masterAddr == host {
			return fmt.Errorf("master address is same as worker (%s == %s)", masterAddr, host)
		}
		if err := startWorker(client); err != nil {
			return fmt.Errorf("worker failure: %v", err)
		}
	}

	return nil
}

// Serves data in tempdir over http at host
func localServe(host, tempdir string) {
	http.Handle("/data/", http.StripPrefix("/data", http.FileServer(http.Dir(tempdir))))
	log.Printf("Serving %s/* at %s", tempdir, makeURL(host, "*"))
	if err := http.ListenAndServe(host, nil); err != nil {
		log.Fatalf("Error in HTTP server for %s: %v", host, err)
	}
}

func makeURL(host, file string) string {
	return fmt.Sprintf("http://%s/data/%s", host, file)
}
