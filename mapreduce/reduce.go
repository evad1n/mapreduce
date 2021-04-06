package mapreduce

import (
	"database/sql"
	"fmt"
	"log"
	"path/filepath"
)

type (
	ReduceTask struct {
		M, R        int      // total number of map and reduce tasks
		N           int      // reduce task number, 0-based
		SourceHosts []string // addresses of map workers
	}

	KeyBatch struct {
		Key   string
		Input <-chan string
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

func (task *ReduceTask) partialFile() string {
	return fmt.Sprintf("reduce_%d_partial.db", task.N)
}

func (task *ReduceTask) tempFile() string {
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

	inDB, err := mergeDatabases(urls, filepath.Join(tempdir, task.inputFile()), filepath.Join(tempdir, task.tempFile()))
	if err != nil {
		return fmt.Errorf("merging databases: %v", err)
	}
	defer inDB.Close()

	// Create output database
	outDB, err := createDatabase(filepath.Join(tempdir, task.outputFile()))
	if err != nil {
		return fmt.Errorf("creating out database: %v", err)
	}
	defer outDB.Close()
	outStmt, err := outDB.Prepare("INSERT INTO pairs (key, value) values (?, ?)")
	if err != nil {
		return fmt.Errorf("preparing insert statement: %v", err)
	}
	defer outStmt.Close()

	// Stats
	keyCount, valCount, outCount := 0, 0, 0

	// Process using client.Reduce
	rows, err := inDB.Query("SELECT key, value FROM pairs ORDER BY key, value")
	if err != nil {
		return fmt.Errorf("querying input db: %v", err)
	}
	defer rows.Close()

	keyBatches := make(chan KeyBatch)
	readDone, writeDone := make(chan error), make(chan error)

	go readInput(rows, keyBatches, readDone, &valCount)

	for batch := range keyBatches {
		keyCount++
		reduceOut := make(chan Pair, 100)

		go task.writeOutput(reduceOut, writeDone, outStmt, &outCount)

		if err := client.Reduce(batch.Key, batch.Input, reduceOut); err != nil {
			return fmt.Errorf("client reduce failure: %v", err)
		}

		// Wait for goroutines to finish batch (pipe write err to read so errors cascade)
		readDone <- <-writeDone
	}

	// Log stats
	log.Printf("reduce task %d processed %d keys and %d values, generated %d pairs\n", task.N, keyCount, valCount, outCount)

	return nil
}

// Handle writing of reduce output to the out db
func (task *ReduceTask) writeOutput(output <-chan Pair, done chan<- error, stmt *sql.Stmt, count *int) {
	for pair := range output {
		*count++
		if _, err := stmt.Exec(pair.Key, pair.Value); err != nil {
			done <- fmt.Errorf("inserting into db: %v", err)
			return
		}
	}

	done <- nil
}

// Handle reading from input db and sending to client reduce function
func readInput(rows *sql.Rows, batchChannel chan<- KeyBatch, done chan error, valCount *int) {
	var retErr error
	var prevKey string
	var currInput chan string
	// Defer error and signal for main to close
	defer func() {
		close(batchChannel)
		done <- retErr
	}()

	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			if prevKey != "" {
				close(currInput)
			}
			retErr = fmt.Errorf("reading a row from input db: %v", err)
			return
		}

		// If keys have changed
		if prevKey != key {
			if prevKey != "" {
				// Close previous call if not first key
				close(currInput)
				// Wait for output to finish
				if err := <-done; err != nil {
					// Error somewhere else so abandon
					return
				}
			}
			// Start new batch
			currInput = make(chan string, 100)
			batchChannel <- KeyBatch{
				Key:   key,
				Input: currInput,
			}
		}
		*valCount++
		currInput <- value

		prevKey = key
	}
	// Check for errors from iterating over rows.
	if err := rows.Err(); err != nil {
		if prevKey != "" {
			close(currInput)
		}
		retErr = fmt.Errorf("iterating over downloaded db: %v", err)
	}

	// Close last call
	close(currInput)
	// Wait for output to finish
	if err := <-done; err != nil {
		// Error somewhere else so abandon
		return
	}
}
