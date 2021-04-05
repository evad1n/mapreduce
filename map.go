package main

import (
	"database/sql"
	"fmt"
	"hash/fnv"
)

type (
	MapTask struct {
		M, R       int    // total number of map and reduce tasks
		N          int    // map task number, 0-based
		SourceHost string // address of host with map input file
	}
)

// Filename helpers

func (task *MapTask) mapSourceFile(mapTaskNumber int) string {
	return fmt.Sprintf("map_%d_source.db", task.N)
}

func (task *MapTask) inputFile() string {
	return fmt.Sprintf("map_%d_input.db", task.N)
}

func (task *MapTask) outputFile(reduceTaskNumber int) string {
	return fmt.Sprintf("map_%d_output_%d.db", task.N, reduceTaskNumber)
}

// Actual mapper logic

func (task *MapTask) Process(tempdir string, client Interface) error {
	// Download input file
	inputFile := task.inputFile()
	if err := download(makeURL(task.SourceHost, inputFile), tempdir); err != nil {
		return fmt.Errorf("downloading input file: %v", err)
	}

	// Create output queries
	outDBs := make([]*sql.Stmt, task.R)
	for i := 0; i < task.R; i++ {
		db, err := createDatabase(task.outputFile(i))
		if err != nil {
			return fmt.Errorf("creating output files: %v", err)
		}
		stmt, err := db.Prepare("INSERT INTO pairs (key, value) values (?, ?)")
		if err != nil {
			return fmt.Errorf("preparing insert statement: %v", err)
		}
		outDBs[i] = stmt
		defer stmt.Close()
		defer db.Close()
	}

	// Process
	out := make(chan Pair, 200)

	// Open input db
	db, err := openDatabase(tempdir)
	if err != nil {
		return fmt.Errorf("opening downloaded db: %v", err)
	}
	defer db.Close()

	// db.Prepare()

	rows, err := db.Query("SELECT key, value FROM pairs")
	if err != nil {
		return fmt.Errorf("querying downloaded db: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			return fmt.Errorf("reading a row from downloaded db: %v", err)
		}
		// Call client map and gather output
		if err := client.Map(key, value, out); err != nil {
			return fmt.Errorf("client map failure: %v", err)
		}
		for pair := range out {
			// Find output file
			hash := fnv.New32() // from the stdlib package hash/fnv
			hash.Write([]byte(pair.Key))
			r := int(hash.Sum32() % uint32(task.R))
			if _, err := outDBs[r].Exec(pair.Key, pair.Value); err != nil {
				return fmt.Errorf("inserting into output db: %v", err)
			}

		}
	}

	// Check for errors from iterating over rows.
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterating over downloaded db: %v", err)
	}

	close(out)

	return nil
}
