package mapreduce

import (
	"io/fs"
	"log"
	"os"
	"path/filepath"
)

func startMaster(client Interface, inputPath, outputPath string) {
	// Split the input file and start an HTTP server to serve source chunks to map workers.
	if err := os.Mkdir(tempdir, fs.ModePerm); err != nil {
		log.Fatalf("creating temp dir: %v", err)
	}
	defer os.RemoveAll(tempdir)

	_, err := splitDatabase(inputPath, tempdir, "map_%d_source.db", M)
	if err != nil {
		log.Fatalf("split db: %v", err)
	}

	// Start http server from tempdir
	go localServe(host, tempdir)

	// Generate the full set of map tasks and reduce tasks. Note that reduce tasks will be incomplete initially, because they require a list of the hosts that handled each map task.
	mapTasks := make([]MapTask, M)
	for i := 0; i < M; i++ {
		mapTasks[i] = MapTask{
			M:          M,
			R:          R,
			N:          i,
			SourceHost: host,
		}
	}
	reduceTasks := make([]ReduceTask, R)
	for i := 0; i < R; i++ {
		reduceTasks[i] = ReduceTask{
			M:           M,
			R:           R,
			N:           i,
			SourceHosts: make([]string, M),
		}
	}

	// Create and start an RPC server to handle incoming client requests.
	//  Note that it can use the same HTTP server that shares static files.
	masterNode := Node{
		Phase:       0,
		NextJob:     0,
		DoneJobs:    0,
		MapTasks:    mapTasks,
		ReduceTasks: reduceTasks,
		Done:        make(chan TaskDone, 10),
	}
	actor, err := masterNode.startRPC()
	if err != nil {
		log.Fatalf("can't start RPC server: %v", err)
	}

	log.Printf("Master [%s] waiting for workers...\n", host)

	// Wait until all jobs are complete.
	done := make(chan []string)
	go actor.waitForJobs(masterNode.Done, done)
	reduceHosts := <-done

	// Gather the reduce outputs and join them into a single output file.
	outDB, err := mergeDatabases(reduceHosts, outputPath, filepath.Join(tempdir, ""))
	if err != nil {
		log.Fatalf("merging reduce output dbs: %v", err)
	}
	defer outDB.Close()

	log.Printf("Output db located at %s\n", outputPath)

	// Tell all workers to shut down, then shut down the master.
	// Get unique hosts
	workers := make(map[string]struct{})
	for _, addr := range reduceHosts {
		workers[addr] = struct{}{}
	}
	// Shut 'em down
	for addr := range workers {
		call(addr, "NodeActor.Terminate", nil, nil)
	}

	log.Println("Master shutting down...")
}

func (a *NodeActor) waitForJobs(taskDone <-chan TaskDone, done chan<- []string) {
	// Build sourcehosts as tasks complete
	mapHosts := make([]string, M)
	reduceHosts := make([]string, R)

	for task := range taskDone {
		a.run(func(n *Node) {
			switch {
			case n.Phase == 0:
				mapHosts[task.Number] = task.Addr
				n.DoneJobs++

				// Done with all map jobs
				if n.DoneJobs == M {
					// Fill in source hosts for reduce tasks
					for i := 0; i < R; i++ {
						n.ReduceTasks[i].SourceHosts = mapHosts
					}

					log.Println("Map phase completed")

					n.Phase = 1
					n.NextJob = 0
					n.DoneJobs = 0
				}

			case n.Phase == 1:
				// Reduce return addr will have filename too
				reduceHosts[task.Number] = task.Addr
				n.DoneJobs++

				// Done with all reduce jobs
				if n.DoneJobs == R {
					log.Println("Reduce phase completed")

					n.Phase = 2
					done <- reduceHosts
				}
			default:
				// Ignore
				log.Printf("Ignoring task completion in phase 2: host %v; number: %v\n", task.Addr, task.Number)
			}
		})
	}
}
