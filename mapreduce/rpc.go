package mapreduce

import (
	"log"
	"net/rpc"
)

type (
	Node struct {
		Phase       int // Map, reduce or done
		NextJob     int
		DoneJobs    int
		MapTasks    []MapTask
		ReduceTasks []ReduceTask
		Done        chan JobDone
		Workers     map[string]struct{} // Worker addresses
	}

	// NodeActor represents an RPC actor for the mapreduce node
	NodeActor chan<- handler
	// Some operation on a Node
	handler func(*Node)

	// RPC structs
	Job struct {
		Phase      int
		Wait       bool // Whether this Job contains an actual job or the worker should just wait
		MapTask    *MapTask
		ReduceTask *ReduceTask
	}

	JobDone struct {
		Number int
		Addr   string
	}
)

const (
	Wait = iota
	Map
	MapDone
	Reduce
	ReduceDone
	Merge
	Finish
)

// Returns next job, or error of there are no more jobs
func (n *Node) GetNextJob(workerAddr string) Job {
	job := Job{
		Phase: n.Phase,
		Wait:  true,
	}
	switch n.Phase {
	case Map:
		// Map
		if n.NextJob < M {
			log.Printf("Map task %d assigned to [%s]\n", n.NextJob, workerAddr)
			job.MapTask = &n.MapTasks[n.NextJob]
			job.Wait = false
			n.NextJob++

			if n.NextJob >= M {
				n.Phase = MapDone
			}
		}
	case Reduce:
		// Reduce
		if n.NextJob < R {
			log.Printf("Reduce task %d assigned to [%s]\n", n.NextJob, workerAddr)
			job.ReduceTask = &n.ReduceTasks[n.NextJob]
			job.Wait = false
			n.NextJob++

			if n.NextJob >= R {
				n.Phase = ReduceDone
			}
		}
	}

	return job
}

// Start the RPC server on the node
func (n *Node) startRPC() (NodeActor, error) {
	actor := n.startActor()
	rpc.Register(actor)
	rpc.HandleHTTP()
	return actor, nil
}

func (n *Node) startActor() NodeActor {
	ch := make(chan handler)
	// Launch actor channel
	go func() {
		for evt := range ch {
			evt(n)
		}
	}()
	return ch
}

// Blocks until actor executes
func (a NodeActor) run(f handler) {
	done := make(chan struct{})
	a <- func(n *Node) {
		f(n)
		done <- struct{}{}
	}
	<-done
}

// The RPC call
func call(address string, method string, request interface{}, reply interface{}) error {
	client, err := rpc.DialHTTP("tcp", string(address))
	if err != nil {
		return err
	}
	defer client.Close()

	// Synchronous call
	if err := client.Call(method, request, reply); err != nil {
		return err
	}

	return nil
}

// Ping connects a worker to the master
func (a NodeActor) Ping(addr string, wait *bool) error {
	a.run(func(n *Node) {
		log.Printf("worker connected from %s\n", addr)
		n.Workers[addr] = struct{}{}
		if n.Phase == Wait {
			*wait = true
		} else {
			*wait = false
		}
	})
	return nil
}

// Sends a signal to the worker, that is handled according to the phase of the job (start/shutdown)
func (a NodeActor) Signal(_ struct{}, _ *struct{}) error {
	a.run(func(n *Node) {
		n.Done <- JobDone{}
	})
	return nil
}

// A worker requests a job from the master
func (a NodeActor) RequestJob(workerAddr string, job *Job) error {
	var err error
	a.run(func(n *Node) {
		*job = n.GetNextJob(workerAddr)
	})
	return err
}

func (a NodeActor) FinishJob(job JobDone, _ *struct{}) error {
	a.run(func(n *Node) {
		n.Done <- job
	})

	return nil
}
