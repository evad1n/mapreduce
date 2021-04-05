package main

import (
	"database/sql"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
)

const (
	host = "localhost:8080"
)

func main() {
	runtime.GOMAXPROCS(1)
	log.SetFlags(log.Lshortfile)

	// localServe(host, filepath.Join("tmp", fmt.Sprintf("mapreduce.%d", 198597)))

	part2()
}

func part1() {
	tempdir := "tmp"
	go localServe(host, tempdir)

	paths, err := splitDatabase("data/austen.db", tempdir, "output-%d.db", 20)
	if err != nil {
		log.Fatalf("split db: %v", err)
	}

	db, err := mergeDatabases(paths, "merged.db", "temp.db")
	if err != nil {
		log.Fatalf("merge dbs: %v", err)
	}
	defer db.Close()

	// Check count
	var total int
	if err := db.QueryRow("SELECT COUNT(*) AS count FROM pairs").Scan(&total); err != nil || err == sql.ErrNoRows {
		log.Fatalf("unable to get total size of data from source db: %v", err)
	}
	log.Printf("Rows in merged db: %d", total)
}

func part2() {
	tempdir := filepath.Join("tmp", fmt.Sprintf("mapreduce.%d", os.Getpid()))
	if err := os.Mkdir(tempdir, fs.ModePerm); err != nil {
		log.Fatalf("creating temp dir: %v", err)
	}
	// defer os.RemoveAll(tempdir)

	go localServe(host, tempdir)

	M, R := 9, 3

	_, err := splitDatabase("data/austen.db", tempdir, "map_%d_source.db", M)
	if err != nil {
		log.Fatalf("split db: %v", err)
	}

	hosts := make([]string, M)
	for i := range hosts {
		hosts[i] = host
	}

	// Map
	log.Println("Map...")
	for m := 0; m < M; m++ {
		task := MapTask{
			M:          M,
			R:          R,
			N:          m,
			SourceHost: host,
		}
		if err := task.Process(tempdir, Client{}); err != nil {
			log.Fatalf("map task: %v", err)
		}
	}

	// // Reduce
	log.Println("Reduce...")
	urls := make([]string, R)
	for i := 0; i < R; i++ {
		task := ReduceTask{
			M:           M,
			R:           R,
			N:           i,
			SourceHosts: hosts,
		}
		if err := task.Process(tempdir, Client{}); err != nil {
			log.Fatalf("reduce task: %v", err)
		}
		urls[i] = makeURL(host, task.outputFile())
	}

	db, err := mergeDatabases(urls, "merged.db", "temp.db")
	if err != nil {
		log.Fatalf("merge dbs: %v", err)
	}
	defer db.Close()

	// Check count
	var total int
	if err := db.QueryRow("SELECT COUNT(*) AS count FROM pairs").Scan(&total); err != nil || err == sql.ErrNoRows {
		log.Fatalf("unable to get total size of data from source db: %v", err)
	}
	log.Printf("Rows in merged db: %d", total)
}

// Serves data in splitDir over http at addr
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

// select key, value from pairs order by value+0 desc limit 20
