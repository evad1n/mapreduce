package mapreduce

import (
	"database/sql"
	"fmt"
	"io/fs"
	"io/ioutil"
	"log"
	"os"
)

func part1() error {
	if err := os.Mkdir(tempdir, fs.ModePerm); err != nil {
		return fmt.Errorf("creating temp dir: %v", err)
	}
	go localServe(host, tempdir)

	paths, err := splitDatabase("data/austen.db", tempdir, "output-%d.db", 20)
	if err != nil {
		return fmt.Errorf("split db: %v", err)
	}

	// Make paths a URL
	for i := range paths {
		paths[i] = makeURL(host, paths[i])
	}

	db, err := mergeDatabases(paths, "merged.db", "temp.db")
	if err != nil {
		return fmt.Errorf("merge dbs: %v", err)
	}
	defer db.Close()

	// Check count
	var total int
	if err := db.QueryRow("SELECT COUNT(*) AS count FROM pairs").Scan(&total); err != nil || err == sql.ErrNoRows {
		return fmt.Errorf("unable to get total size of data from source db: %v", err)
	}
	log.Printf("Rows in merged db: %d", total)

	return nil
}

func part2(client Interface) error {
	if err := os.Mkdir(tempdir, fs.ModePerm); err != nil {
		return fmt.Errorf("creating temp dir: %v", err)
	}

	go localServe(host, tempdir)

	M, R := 9, 3

	_, err := splitDatabase("data/austen.db", tempdir, "map_%d_source.db", M)
	if err != nil {
		return fmt.Errorf("split db: %v", err)
	}

	hosts := make([]string, M)
	for i := range hosts {
		hosts[i] = host
	}

	// Verify count of map output (should be M * R)
	currentCount := getFileCount(tempdir)
	totalOutputFiles := 0

	// Map
	log.Println("Map...")
	for m := 0; m < M; m++ {
		task := MapTask{
			M:          M,
			R:          R,
			N:          m,
			SourceHost: host,
		}
		if err := task.Process(tempdir, client); err != nil {
			return fmt.Errorf("map task: %v", err)
		}
		newCount := getFileCount(tempdir)
		// Minus 1 for downloading source file
		dCount := newCount - currentCount - 1
		log.Printf("map task %d generated %d output files\n", m, dCount)
		totalOutputFiles += dCount
		currentCount = newCount
	}

	log.Printf("map tasks generated %[1]d total output files. should be M*R => (%d * %d) == %[1]d\n", totalOutputFiles, M, R)

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
		if err := task.Process(tempdir, client); err != nil {
			return fmt.Errorf("reduce task: %v", err)
		}
		urls[i] = makeURL(host, task.outputFile())
	}

	db, err := mergeDatabases(urls, "merged.db", "temp.db")
	if err != nil {
		return fmt.Errorf("merge dbs: %v", err)
	}
	defer db.Close()

	// Check count
	var total int
	if err := db.QueryRow("SELECT COUNT(*) AS count FROM pairs").Scan(&total); err != nil || err == sql.ErrNoRows {
		return fmt.Errorf("unable to get total size of data from source db: %v", err)
	}
	log.Printf("Rows in merged db: %d", total)

	return nil
}

// Outside verification of intermediate file creation
func getFileCount(dir string) int {
	files, err := ioutil.ReadDir(tempdir)
	if err != nil {
		log.Printf("getFileCount error: %v", err)
	}
	return len(files)
}
