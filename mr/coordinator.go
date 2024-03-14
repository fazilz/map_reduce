package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"

	"github.com/google/uuid"
)

type Coordinator struct {
	sync.Mutex
	todo_files          []string
	max_reduce_tasks    int
	curr_reduce_task    int
	active_map_tasks    int
	active_reduce_tasks int
	workerStatus        sync.Map
}

func (c *Coordinator) WorkerHeartBeat(args *HeartBeat, reply *HeartBeatReply) error {
	// update worker status
	//log.Printf("worker heartbeat: id: %s, status: %s ", args.WorkerId, args.State)
	storedState, loaded := c.workerStatus.LoadOrStore(args.WorkerId, args.State)
	if loaded && storedState != args.State {
		c.workerStatus.Store(args.WorkerId, args.State)
		// go ...
	}
	if args.State == WORKER_MAP || args.State == WORKER_REDUCE {
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
		c.todo_files = c.todo_files[:last_idx]
		c.active_map_tasks += 1
		//log.Printf("starting map task for file: %s", reply.Filename)
	} else if c.curr_reduce_task < c.max_reduce_tasks && c.active_map_tasks == 0 {
		// no more map tasks left
		reply.IsMap = false
		reply.IsReduce = true
		reply.ReduceId = c.curr_reduce_task
		c.curr_reduce_task += 1
		c.active_reduce_tasks += 1
		//log.Printf("starting reduce task #%d", reply.ReduceId)
	} else if c.active_reduce_tasks == 0 && c.active_map_tasks == 0 {
		// no map or reduce tasks left
		reply.ShouldTerminate = true
	}
	return nil
}

func (c *Coordinator) RegisterWorker(args *RegisterWorker, reply *RegisterWorkerReply) error {
	reply.WorkerId = uuid.NewString()
	reply.MaxReduceId = c.max_reduce_tasks
	//log.Printf("Registered new worker with uuid: %s", reply.WorkerId)
	return nil
}

func (c *Coordinator) TaskDone(args *TaskDone, reply *TaskDone) error {
	c.Lock()
	defer c.Unlock()
	if args.Task == WORKER_MAP {
		c.active_map_tasks -= 1
	} else if args.Task == WORKER_REDUCE {
		c.active_reduce_tasks -= 1
	}
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
	if c.active_map_tasks > 0 && c.active_reduce_tasks > 0 {
		return false
	}
	
	log.Print("Completed tasks, exiting coordinator")
	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{max_reduce_tasks: nReduce, curr_reduce_task: 0, todo_files: files}
	c.server()
	//log.Printf("Created coordinator with max_reduce_tasks: %d and %d files for processing", nReduce, len(files))
	//log.Printf("Files to process: %v", files)
	return &c
}
