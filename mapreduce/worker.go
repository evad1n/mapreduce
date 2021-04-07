package mapreduce

import (
	"fmt"
	"io/fs"
	"log"
	"os"
	"time"
)

const (
	requestInterval = 100 // Milliseconds
)

func startWorker(client Interface) error {
	// Start an HTTP server to serve intermediate data files to other workers and back to the master.
	if err := os.Mkdir(tempdir, fs.ModePerm); err != nil {
		return fmt.Errorf("creating temp dir: %v", err)
	}

	go localServe(host, tempdir)

	workerNode := Node{
		Done: make(chan TaskDone),
	}
	_, err := workerNode.startRPC()
	if err != nil {
		return fmt.Errorf("can't start RPC server: %v", err)
	}

	// Notify master
	var wait bool
	if err := call(masterAddr, "NodeActor.Ping", host, &wait); err != nil {
		return fmt.Errorf("connecting to master: %v", err)
	}
	if wait {
		// Wait for master to start the worker
		<-workerNode.Done
	}

	ticker := time.NewTicker(time.Millisecond * requestInterval)

JobLoop:
	for range ticker.C {
		// Request a job from the master.
		var job interface{}
		if err := call(masterAddr, "NodeActor.RequestJob", host, &job); err != nil {
			switch err.Error() {
			case "no more jobs":
				break JobLoop
			default:
				log.Printf("requesting job: %v\n", err)
				continue
			}
		}

		// Determine type of task and process accordingly
		switch task := job.(type) {
		case MapTask:
			var taskErr error
			if taskErr = task.Process(tempdir, client); taskErr != nil {
				log.Printf("reduce task: %v\n", taskErr)
			}
			result := TaskDone{
				Number: task.N,
				Addr:   host,
				Err:    taskErr,
			}
			if err := call(masterAddr, "NodeActor.FinishJob", result, nil); err != nil {
				return fmt.Errorf("finishing job: %v", err)
			}
		case ReduceTask:
			var taskErr error
			if taskErr = task.Process(tempdir, client); taskErr != nil {
				log.Printf("reduce task: %v\n", taskErr)
			}
			result := TaskDone{
				Number: task.N,
				Addr:   host, // Add filename for merging
				Err:    taskErr,
			}
			if err := call(masterAddr, "NodeActor.FinishJob", result, nil); err != nil {
				return fmt.Errorf("finishing job: %v", err)
			}
		default:
			return fmt.Errorf("unknown task type: %v", task)
		}
	}

	<-workerNode.Done

	if err := shutDown(); err != nil {
		return fmt.Errorf("shutting down: %v", err)
	}

	return nil
}

// Graceful shutdown of worker
func shutDown() error {
	// Remove all temp files
	if err := os.RemoveAll(tempdir); err != nil {
		return fmt.Errorf("unable to clear tempdir: %v", err)
	}

	log.Println("Shutting down...")
	os.Exit(0)
	return nil
}
