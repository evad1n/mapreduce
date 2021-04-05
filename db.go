package main

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

// Opens an existing sqlite3 database
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
	db, _ := sql.Open("sqlite3", path+options)
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("opening db %s: %v", path, err)
	}
	return db, nil
}

// Creates a sqlite3 database. If the file already exists, it will be overwritten
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
		outPaths[i] = dir

		stmt, err := db.Prepare("INSERT INTO pairs (key, value) values (?, ?)")
		if err != nil {
			return nil, fmt.Errorf("preparing insert statement: %v", err)
		}

		for r := 0; r < partitionSize; r++ {
			rows.Next()
			count++
			var key, value string
			if err := rows.Scan(&key, &value); err != nil {
				return nil, fmt.Errorf("reading a row from source db: %v", err)
			}
			if _, err := stmt.Exec(key, value); err != nil {
				return nil, fmt.Errorf("inserting into out db: %v", err)
			}
		}
		stmt.Close()
		db.Close()
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

	return outPaths, nil
}

// Merge databases located trough urls into a destination local db, using temp as the temporary write file
func mergeDatabases(urls []string, dest string, temp string) (*sql.DB, error) {
	db, err := createDatabase(dest)
	if err != nil {
		return nil, fmt.Errorf("creating database: %v", err)
	}

	for _, url := range urls {
		// Download and store in temp dir
		if err := download(url, temp); err != nil {
			db.Close()
			return nil, fmt.Errorf("downloading db %s: %v", url, err)
		}
		// Merge and delete temp
		if err := gatherInto(db, temp); err != nil {
			db.Close()
			return nil, fmt.Errorf("merging db @(%s): %v", url, err)
		}
	}

	return db, nil
}

// Download a file over HTTP and store in dest path
func download(url, dest string) error {
	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("http get: %v", err)
	}
	defer resp.Body.Close()

	// Create the file
	out, err := os.Create(dest)
	if err != nil {
		return fmt.Errorf("creating destination file: %v", err)
	}
	defer out.Close()

	// Write the body to file
	if _, err := io.Copy(out, resp.Body); err != nil {
		return fmt.Errorf("copying data: %v", err)
	}

	return nil
}

const mergeCmd = `ATTACH ? AS merge;
INSERT INTO pairs SELECT * FROM merge.pairs;
DETACH merge;`

// Merges db at path <in> into <out> db
func gatherInto(out *sql.DB, in string) error {
	if _, err := out.Exec(mergeCmd, in); err != nil {
		return fmt.Errorf("merging db: %v", err)
	}

	// Delete input file
	if err := os.Remove(in); err != nil {
		return fmt.Errorf("removing input file: %v", err)
	}

	return nil
}
