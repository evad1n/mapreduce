package mapreduce

import (
	"io/fs"
	"log"
	"os"
	"time"
)

func startWorker(client Interface) {
	// Start an HTTP server to serve intermediate data files to other workers and back to the master.
	if err := os.Mkdir(tempdir, fs.ModePerm); err != nil {
		log.Fatalf("creating temp dir: %v", err)
	}
	defer os.RemoveAll(tempdir)

	go localServe(host, tempdir)

	ticker := time.NewTicker(time.Millisecond * 100)
	for range ticker.C {
		// Request a job from the master.
		var job interface{}
		if err := call(masterAddr, "NodeActor.RequestJob", host, &job); err != nil {
			log.Printf("requesting job: %v\n", err)
			// No more jobs
			if err.Error() == "no more jobs" {
				break
			} else {
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
				log.Fatalf("finishing job: %v\n", err)
			}
		case ReduceTask:
			var taskErr error
			if taskErr = task.Process(tempdir, client); taskErr != nil {
				log.Printf("reduce task: %v\n", taskErr)
			}
			result := TaskDone{
				Number: task.N,
				Addr:   makeURL(host, task.outputFile()), // Add filename for merging
				Err:    taskErr,
			}
			if err := call(masterAddr, "NodeActor.FinishJob", result, nil); err != nil {
				log.Fatalf("finishing job: %v\n", err)
			}
		default:
			log.Fatalf("unknown task type: %v", task)
		}

		// Signal task is finished

	}
}

// Graceful shutdown of worker
func shutDown() {
	// Remove all temp files
	if err := os.RemoveAll(tempdir); err != nil {
		log.Fatalf("unable to clear tempdir: %v", err)
	}

	log.Println("Shutting down...")
	os.Exit(0)
}
