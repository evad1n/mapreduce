package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
)

const (
	tempdir = "tmp"
	addr    = "localhost:8080"
)

func main() {
	log.SetFlags(log.Lshortfile)

	go localServe(tempdir, addr)

	paths, err := splitDatabase("data/austen.db", tempdir, "output-%d.db", 20)
	if err != nil {
		log.Fatalf("split db: %v", err)
	}

	for i := range paths {
		paths[i] = "http://" + addr + "/" + paths[i]
		fmt.Println(paths[i])
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

// Serves data in splitDir over http at addr
func localServe(splitDir, addr string) {
	http.Handle("/"+splitDir+"/", http.StripPrefix("/"+splitDir, http.FileServer(http.Dir(splitDir))))
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Printf("Error in HTTP server for %s: %v", addr, err)
	}
}
