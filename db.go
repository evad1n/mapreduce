package main

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

func openDatabase(path string) (*sql.DB, error) {
	// the path to the database--this could be an absolute path
	options :=
		"?" + "_busy_timeout=10000" +
			"&" + "_case_sensitive_like=OFF" +
			"&" + "_foreign_keys=ON" +
			"&" + "_journal_mode=OFF" +
			"&" + "_locking_mode=NORMAL" +
			"&" + "mode=rw" +
			"&" + "_synchronous=OFF"
	db, err := sql.Open("sqlite3", path+options)
	if err != nil {
		return nil, fmt.Errorf("opening db %s: %v", path, err)
	}
	return db, nil
}

func createDatabase(path string) (*sql.DB, error) {
	if _, err := os.Stat(path); err == nil {
		if err := os.Remove(path); err != nil {
			return nil, fmt.Errorf("removing existing db: %v", err)
		}
	}

	db, err := openDatabase(path)
	if err != nil {
		return nil, fmt.Errorf("opening new db: %v", err)
	}

	_, err = db.Exec("CREATE TABLE pairs (key text, value text)")
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("creating table: %v", err)
	}

	return db, nil
}

// Splits a database into multiple (contiguous) shards. Returns filenames of output databases.
// e.g. paths, err := splitDatabase("input.db", "data", "output-%d.db", 50)
func splitDatabase(source, outputDir, outputPattern string, m int) ([]string, error) {
	// Open source database
	db, err := openDatabase(source)
	if err != nil {
		return nil, fmt.Errorf("opening source db: %v", err)
	}
	defer db.Close()

	outPaths := make([]string, m)

	// Get count to partition contiguously
	var total int
	if err := db.QueryRow("SELECT COUNT(*) AS count FROM pairs").Scan(&total); err != nil || err == sql.ErrNoRows {
		return nil, fmt.Errorf("unable to get total size of data from source db: %v", err)
	}
	log.Printf("Size of data: %d", total)

	// Fewer keys than map tasks
	if total < m {
		return nil, errors.New("fewer keys than map tasks")
	}

	// Get partition inputs
	base := total / m
	r := total % m

	rows, err := db.Query("SELECT key, value FROM pairs")
	if err != nil {
		return nil, fmt.Errorf("querying source db: %v", err)
	}
	defer rows.Close()

	count := 0
	for i := 0; i < m; i++ {
		// Set partition size
		partitionSize := base
		if i < r {
			partitionSize++
		}

		// Create out DB
		name := fmt.Sprintf(outputPattern, i)
		dir := filepath.Join(outputDir, name)
		db, err := createDatabase(dir)
		if err != nil {
			return nil, fmt.Errorf("creating output database: %v", err)
		}
		defer db.Close()
		outPaths[i] = dir

		for r := 0; r < partitionSize; r++ {
			rows.Next()
			count++
			var key, value string
			if err := rows.Scan(&key, &value); err != nil {
				return nil, fmt.Errorf("reading a row from source db: %v", err)
			}
			if _, err := db.Exec("INSERT INTO pairs (key, value) values (?, ?)", key, value); err != nil {
				return nil, fmt.Errorf("inserting into out db: %v", err)
			}
		}
	}

	// Check for errors from iterating over rows.
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating over source db: %v", err)
	}

	// Verify all rows were processed
	if count != total {
		err := errors.New("wrong number of keys processed")
		return nil, fmt.Errorf("%v processed: %d, total: %d", err, count, total)
	}

	// fmt.Printf("DATA:\ntotal:%d | count: %d | tasks: %d\n", total, count, m)
	// fmt.Printf("SPLIT:\nbase:%d | remainder: %d\n", base, r)

	// for i, p := range outPaths {
	// 	size := base
	// 	if i < r {
	// 		size++
	// 	}
	// 	fmt.Printf("%s: %d\n", p, size)
	// }

	return outPaths, nil
}

func mergeDatabases(urls []string, path string, temp string) (*sql.DB, error) {
	return nil, nil
}