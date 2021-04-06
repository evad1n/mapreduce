package mapreduce

import (
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
	masterAddr string
	port       string
	tempdir    string
	M          int
	R          int

	host string
)

func Start(client Interface) error {
	runtime.GOMAXPROCS(1)

	log.SetFlags(log.Lshortfile)

	flag.BoolVar(&master, "master", false, "Whether this node is the master or a worker")
	flag.StringVar(&masterAddr, "masterAddr", "localhost:8080", "Address of the master node")
	flag.StringVar(&port, "port", "8080", "The port to listen on")
	flag.StringVar(&tempdir, "tempdir", filepath.Join("tmp", fmt.Sprintf("mapreduce.%d", os.Getpid())), "The directory to store temporary files in")

	flag.IntVar(&M, "M", 10, "Number of map tasks")
	flag.IntVar(&R, "R", 10, "Number of reduce tasks")

	flag.Parse()

	host = "localhost:" + port

	if master {
		log.Printf("Starting master node on port %s\n", port)
		// Verify input and output db
		if flag.NArg() != 2 {
			log.Fatalln("Please specify paths to input and output db at end")
			return nil
		}
		inputPath := flag.Arg(0)
		outputPath := flag.Arg(1)

		startMaster(client, inputPath, outputPath)
	} else {
		log.Printf("Starting worker node on port %s\n", port)
		if masterAddr == host {
			log.Fatalf("master address is same as worker (%s == %s)", masterAddr, host)
		}
		startWorker(client)
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
