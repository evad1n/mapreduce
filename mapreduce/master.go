package mapreduce

import (
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
)

func startMaster(client Interface, inputPath, outputPath string) error {
	// Split the input file and start an HTTP server to serve source chunks to map workers.
	if err := os.Mkdir(tempdir, fs.ModePerm); err != nil {
		return fmt.Errorf("creating temp dir: %v", err)
	}
	defer os.RemoveAll(tempdir)

	// Start http server from tempdir
	go localServe(host, tempdir)

	_, err := splitDatabase(inputPath, tempdir, "map_%d_source.db", M)
	if err != nil {
		return fmt.Errorf("split db: %v", err)
	}

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
		Phase:       Wait,
		NextJob:     0,
		DoneJobs:    0,
		MapTasks:    mapTasks,
		ReduceTasks: reduceTasks,
		Done:        make(chan JobDone, 10),
		Workers:     make(map[string]struct{}),
	}
	actor, err := masterNode.startRPC()
	if err != nil {
		return fmt.Errorf("can't start RPC server: %v", err)
	}

	// Phase -1 is waiting phase
	if wait {
		log.Printf("Master @[%s] waiting for user input to start...\n", host)
		fmt.Println("Press ENTER to start...")
		var ignore string
		fmt.Scanln(&ignore)
		masterNode.Phase = Map
		fmt.Println("Starting workers...")
		for workerAddr := range masterNode.Workers {
			log.Printf("starting worker @[%s]", workerAddr)
			if err := call(workerAddr, "NodeActor.Signal", struct{}{}, nil); err != nil {
				log.Printf("error contacting worker @[%s]: %v\n", workerAddr, err)
			}
		}
	} else {
		masterNode.Phase = Map
		log.Printf("Master @[%s] waiting for workers...\n", host)
	}

	// Wait until all jobs are complete.
	reduceHosts := actor.waitForJobs(masterNode.Done)

	// Create correct urls
	outputURLs := make([]string, R)
	for i := 0; i < R; i++ {
		outputURLs[i] = makeURL(reduceHosts[i], reduceTasks[i].outputFile())
	}

	// Gather the reduce outputs and join them into a single output file.
	outDB, err := mergeDatabases(outputURLs, outputPath, filepath.Join(tempdir, "tmp.db"))
	if err != nil {
		return fmt.Errorf("merging reduce output dbs: %v", err)
	}
	defer outDB.Close()

	log.Printf("Output db located at %s\n", outputPath)

	masterNode.Phase = Finish

	// Tell all workers to shut down, then shut down the master.
	for addr := range masterNode.Workers {
		log.Printf("shutting down worker @[%s]", addr)
		if err := call(addr, "NodeActor.Signal", struct{}{}, nil); err != nil {
			log.Printf("error shutting down worker: %v", err)
		}
	}

	log.Println("Master shutting down...")

	return nil
}

func (a *NodeActor) waitForJobs(taskDone <-chan JobDone) []string {
	// Build sourcehosts as tasks complete
	mapHosts := make([]string, M)
	reduceHosts := make([]string, R)

	for task := range taskDone {
		var currPhase int
		a.run(func(n *Node) {
			switch {
			case n.Phase == Map || n.Phase == MapDone:
				log.Printf("Map task %d completed by [%s]\n", task.Number, task.Addr)
				mapHosts[task.Number] = task.Addr
				n.DoneJobs++

				// Done with all map jobs
				if n.DoneJobs == M {
					// Fill in source hosts for reduce tasks
					for i := 0; i < R; i++ {
						n.ReduceTasks[i].SourceHosts = mapHosts
					}

					log.Println("Map phase completed")

					n.Phase = Reduce
					n.NextJob = 0
					n.DoneJobs = 0
				}

			case n.Phase == Reduce || n.Phase == ReduceDone:
				log.Printf("Reduce task %d completed by [%s]\n", task.Number, task.Addr)
				reduceHosts[task.Number] = task.Addr
				n.DoneJobs++

				// Done with all reduce jobs
				if n.DoneJobs == R {
					log.Println("Reduce phase completed")

					n.Phase = Merge
				}
			default:
				// Ignore
				log.Printf("Ignoring task completion in phase %d: host %v; number: %v\n", n.Phase, task.Addr, task.Number)
			}
			currPhase = n.Phase
		})
		if currPhase >= Merge {
			break
		}
	}

	return reduceHosts
}
