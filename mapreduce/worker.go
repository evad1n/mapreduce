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
	defer os.RemoveAll(tempdir)

	go localServe(host, tempdir)

	workerNode := Node{
		Done: make(chan JobDone, 1),
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
		log.Println("Waiting for master to start...")
		<-workerNode.Done
	}

	ticker := time.NewTicker(time.Millisecond * requestInterval)

	lastPhase := Wait

	// Label to break out of nested scopes
JobLoop:
	for range ticker.C {
		// Request a job from the master.
		var job Job
		if err := call(masterAddr, "NodeActor.RequestJob", host, &job); err != nil {
			if lastPhase >= ReduceDone {
				break JobLoop
			}
			return fmt.Errorf("requesting job: %v", err)
		}

		// Determine type of task and process accordingly
		if !job.Wait {
			if job.Phase == Map {
				task := job.MapTask
				log.Printf("Received map task %d. Processing...\n", task.N)
				if err := task.Process(tempdir, client); err != nil {
					return fmt.Errorf("map job: %v", err)
				}
				result := JobDone{
					Number: task.N,
					Addr:   host,
				}
				if err := call(masterAddr, "NodeActor.FinishJob", result, nil); err != nil {
					return fmt.Errorf("finishing map job: %v", err)
				}
			} else {
				task := job.ReduceTask
				log.Printf("Received reduce task %d. Processing...\n", task.N)
				if err = task.Process(tempdir, client); err != nil {
					return fmt.Errorf("reduce job: %v", err)
				}
				result := JobDone{
					Number: task.N,
					Addr:   host,
				}
				if err := call(masterAddr, "NodeActor.FinishJob", result, nil); err != nil {
					return fmt.Errorf("finishing reduce job: %v", err)
				}
			}
		} else {
			// Log reason for waiting
			if job.Phase != lastPhase {
				switch job.Phase {
				case MapDone:
					log.Println("Waiting for map jobs to finish...")
				case ReduceDone:
					log.Println("Waiting for reduce jobs to finish...")
				default:
					break JobLoop
				}
			}
		}
		lastPhase = job.Phase
	}

	log.Println("Waiting for master to finish...")
	<-workerNode.Done

	log.Println("Shutting down...")
	return nil
}
