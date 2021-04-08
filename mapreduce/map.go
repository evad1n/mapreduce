package mapreduce

import (
	"database/sql"
	"fmt"
	"hash/fnv"
	"log"
	"path/filepath"
)

type (
	MapTask struct {
		M, R       int    // total number of map and reduce tasks
		N          int    // map task number, 0-based
		SourceHost string // address of host with map input file
	}
)

// Filename helpers

func (task *MapTask) sourceFile() string {
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
	inputFile := filepath.Join(tempdir, task.inputFile())
	if err := download(makeURL(task.SourceHost, task.sourceFile()), inputFile); err != nil {
		return fmt.Errorf("downloading source file: %v", err)
	}

	// Create output queries
	outStmts := make([]*sql.Stmt, task.R)
	for i := 0; i < task.R; i++ {
		db, err := createDatabase(filepath.Join(tempdir, task.outputFile(i)))
		if err != nil {
			return fmt.Errorf("creating output files: %v", err)
		}
		stmt, err := db.Prepare("INSERT INTO pairs (key, value) values (?, ?)")
		if err != nil {
			return fmt.Errorf("preparing insert statement: %v", err)
		}
		outStmts[i] = stmt
		defer stmt.Close()
		defer db.Close()
	}

	// Process

	// Open input db
	db, err := openDatabase(inputFile)
	if err != nil {
		return fmt.Errorf("opening input db: %v", err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT key, value FROM pairs")
	if err != nil {
		return fmt.Errorf("querying input db: %v", err)
	}
	defer rows.Close()

	// Stats
	inCount, outCount := 0, 0

	for rows.Next() {
		inCount++
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			return fmt.Errorf("reading a row from input db: %v", err)
		}

		// Call client map and gather output
		mapOut := make(chan Pair, 200)
		done := make(chan error)

		// Goroutine for writing intermediate kv
		go task.writeOutput(mapOut, done, outStmts, &outCount)

		if err := client.Map(key, value, mapOut); err != nil {
			return fmt.Errorf("client map failure: %v", err)
		}

		// Wait for writing to finish
		if err := <-done; err != nil {
			return fmt.Errorf("writing output: %v", err)
		}
	}

	// Check for errors from iterating over rows.
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterating over downloaded db: %v", err)
	}

	// Log stats
	log.Printf("map task %d processed %d pairs, generated %d pairs, created %d intermediate output files\n", task.N, inCount, outCount, task.R)

	return nil
}

func (task *MapTask) writeOutput(output <-chan Pair, done chan<- error, outStmts []*sql.Stmt, count *int) {
	for pair := range output {
		*count++
		// Find output file
		hash := fnv.New32() // from the stdlib package hash/fnv
		hash.Write([]byte(pair.Key))
		r := int(hash.Sum32() % uint32(task.R))
		if _, err := outStmts[r].Exec(pair.Key, pair.Value); err != nil {
			done <- fmt.Errorf("inserting into output db: %v", err)
			return
		}
	}

	done <- nil
}
