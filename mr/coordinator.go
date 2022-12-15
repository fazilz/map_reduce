package mr

import (
	"github.com/google/uuid"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	sync.Mutex
	todo_files       []string
	max_reduce_tasks int
	curr_reduce_task int
	workerStatus     sync.Map
	done             bool
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) WorkerHeartBeat(args *HeartBeat, reply *HeartBeatReply) error {
	// update worker status
	storedState, loaded := c.workerStatus.LoadOrStore(args.WorkerId, args.State)
	if loaded && storedState != args.State {
		c.workerStatus.Store(args.WorkerId, args.State)
	}

	if args.State == WORKER_BUSY {
		reply.ShouldTerminate = false
		return nil
	}
	// worker is ready
	c.Lock()
	defer c.Unlock()
	last_idx := len(c.todo_files) - 1
	if last_idx > -1 {
		reply.Filename = c.todo_files[last_idx]
		reply.IsMap = true
        reply.MaxReduceId = c.max_reduce_tasks - 1
        c.todo_files = c.todo_files[:last_idx]
	} else if c.curr_reduce_task < c.max_reduce_tasks {
		// no more map tasks left
		reply.IsMap = false
		reply.ReduceId = c.curr_reduce_task
        reply.MaxReduceId = c.max_reduce_tasks
		c.curr_reduce_task += 1
	} else {
		// no map or reduce tasks left
		reply.ShouldTerminate = true
	}
	return nil
}

func (c *Coordinator) RegisterWorker(args *RegisterWorker, reply *RegisterWorkerReply) error {
	reply.WorkerId = uuid.NewString()
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.Lock()
	defer c.Unlock()
	if len(c.todo_files) != 0 || c.curr_reduce_task < c.max_reduce_tasks {
		return false
	}
	// check if any of workers are busy before returning
	c.done = true
	c.workerStatus.Range(func(key, value interface{}) bool {
		if value == WORKER_BUSY {
			c.done = false
			return false
		}
		return true
	})
	return c.done
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{max_reduce_tasks: nReduce, curr_reduce_task: 0, todo_files: files}
	c.server()
	return &c
}
