package main

import (
	"database/sql"
	"fmt"
)

type (
	ReduceTask struct {
		M, R        int      // total number of map and reduce tasks
		N           int      // reduce task number, 0-based
		SourceHosts []string // addresses of map workers
	}
)

// Filename helpers

func (task *ReduceTask) mapInputFile(mapTaskNumber int) string {
	return fmt.Sprintf("map_%d_output_%d.db", mapTaskNumber, task.N)
}

func (task *ReduceTask) inputFile() string {
	return fmt.Sprintf("reduce_%d_input.db", task.N)
}

func (task *ReduceTask) outputFile() string {
	return fmt.Sprintf("reduce_%d_output.db", task.N)
}

func (task *ReduceTask) reducePartialFile() string {
	return fmt.Sprintf("reduce_%d_partial.db", task.N)
}

func (task *ReduceTask) reduceTempFile() string {
	return fmt.Sprintf("reduce_%d_temp.db", task.N)
}

// Actual reducer logic

func (task *ReduceTask) Process(tempdir string, client Interface) error {
	// Create input database by merging all map outputs

	// Get correct URLs for input files
	urls := make([]string, task.M)
	for i := 0; i < task.M; i++ {
		urls[i] = makeURL(task.SourceHosts[i], task.mapInputFile(i))
	}

	inDB, err := mergeDatabases(urls, task.inputFile(), "tmp.db")
	if err != nil {
		return fmt.Errorf("merging databases: %v", err)
	}
	defer inDB.Close()

	// Create output database
	outDB, err := createDatabase(task.outputFile())
	if err != nil {
		return fmt.Errorf("creating out database: %v", err)
	}
	defer outDB.Close()

	// Process using client.Reduce
	reduceOut := make(chan Pair, 100)
	reduceIn := make(chan string, 100)
	// results := make(chan error)

	rows, err := inDB.Query("SELECT key, value FROM pairs ORDER BY key, value")
	if err != nil {
		return fmt.Errorf("querying input db: %v", err)
	}
	defer rows.Close()

	prevKey := ""
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			return fmt.Errorf("reading a row from input db: %v", err)
		}

		// Call client reduce and gather output
		// TODO: how to handle errors in goroutines
		// TODO: why do we use goroutines for tasks

		// If keys have changed
		if prevKey != key {
			// Close previous call if not first key
			if prevKey != "" {
				close(reduceIn)
				// Wait for output to finish
			} else {
				if err := client.Reduce(key, reduceIn, reduceOut); err != nil {
					return fmt.Errorf("client reduce failure: %v", err)
				}
			}
			// Signal client to start reduce on new key
			if err := client.Reduce(key, reduceIn, reduceOut); err != nil {
				return fmt.Errorf("client reduce failure: %v", err)
			}
		}
		reduceIn <- value

		prevKey = key
	}

	// Check for errors from iterating over rows.
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterating over downloaded db: %v", err)
	}

	// Sync goroutines here
	// if res := <-results

	return nil
}

// Handle writing of reduce output to the out db
func reduceWrite(db *sql.DB, input <-chan Pair, result chan<- error) {
	stmt, err := db.Prepare("INSERT INTO pairs (key, value) values (?, ?)")
	if err != nil {
		result <- fmt.Errorf("preparing insert statement: %v", err)
		return
	}

	for pair := range input {
		if _, err := stmt.Exec(pair.Key, pair.Value); err != nil {
			result <- fmt.Errorf("inserting into db: %v", err)
			return
		}
	}

	result <- nil
}

// Handle reading from input db and sending to client reduce function
func reduceRead(db *sql.DB, result chan<- error) {

}
