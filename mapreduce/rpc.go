package mapreduce

import (
	"encoding/gob"
	"errors"
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
		Done        chan TaskDone
		Workers     []string // Slice of worker addresses
	}

	// NodeActor represents an RPC actor for the mapreduce node
	NodeActor chan<- handler
	// Some operation on a Node
	handler func(*Node)

	// RPC structs
	TaskDone struct {
		Number int
		Addr   string
		Err    error
	}
)

// Need to do this to to allow variable struct passed to RequestJob
func init() {
	gob.Register(MapTask{})
	gob.Register(ReduceTask{})
}

// Returns next job, or error of there are no more jobs
func (n *Node) GetNextJob() (interface{}, error) {
	switch n.Phase {
	case -1:
		// Master was instructed to wait
		return nil, errors.New("waiting for master")
	case 0:
		// Map
		if n.NextJob < M {
			log.Printf("Map task %d assigned\n", n.NextJob)
			return n.MapTasks[n.NextJob], nil
		}
		return nil, errors.New("waiting for map jobs to finish")
	case 1:
		// Reduce
		if n.NextJob < R {
			log.Printf("Reduce task %d assigned\n", n.NextJob)
			return n.ReduceTasks[n.NextJob], nil
		}
		fallthrough
	default:
		return nil, errors.New("no more jobs")
	}
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
		if n.Phase == -1 {
			n.Workers = append(n.Workers, addr)
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
		n.Done <- TaskDone{}
	})
	return nil
}

// A worker requests a job from the master
func (a NodeActor) RequestJob(workerAddr string, job *interface{}) error {
	var err error
	a.run(func(n *Node) {
		if *job, err = n.GetNextJob(); err == nil {
			n.NextJob++
		}
	})
	return err
}

func (a NodeActor) FinishJob(task TaskDone, _ *struct{}) error {
	a.run(func(n *Node) {
		n.Done <- task
	})

	return nil
}
