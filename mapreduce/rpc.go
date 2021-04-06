package mapreduce

import (
	"encoding/gob"
	"errors"
	"net/rpc"
)

type (
	// TODO: should there be 2 differnet nodes master/worker or what...
	Node struct {
		Phase       int // Map, reduce or done
		NextJob     int
		DoneJobs    int
		MapTasks    []MapTask
		ReduceTasks []ReduceTask
		Done        chan TaskDone
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

// FIX: This is definitely not right
// Complains about not registering interface mapreduce.MapTask
func init() {
	gob.Register(MapTask{})
	gob.Register(ReduceTask{})
}

// Returns next job, or error of there are no more jobs
func (n *Node) GetNextJob() (interface{}, error) {
	switch n.Phase {
	case 0:
		// Map
		if n.NextJob < M {
			return n.MapTasks[n.NextJob], nil
		}
		return nil, errors.New("waiting for map jobs to finish")
	case 1:
		// Reduce
		if n.NextJob < R {
			return n.ReduceTasks[n.NextJob], nil
		}
		fallthrough
	default:
		return nil, errors.New("no more jobs")
	}
}

// Start the RPC server on the node
func (n *Node) startRPC() (NodeActor, error) {
	// Make sure port isn't in use frst
	// listener, err := net.Listen("tcp", host)
	// if err != nil {
	// 	return nil, fmt.Errorf("listen error: %v", err)
	// }
	actor := n.startActor()
	rpc.Register(actor)
	rpc.HandleHTTP()
	// go http.Serve(listener, nil)
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

// Ping simply tests an RPC connection
func (a NodeActor) Ping(_ struct{}, reply *bool) error {
	a.run(func(n *Node) {
		*reply = true
	})
	return nil
}

func (a NodeActor) Terminate(_ struct{}, _ *struct{}) error {
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
