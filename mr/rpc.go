package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

const (
	WORKER_READY string = "ready"
	WORKER_BUSY  string = "busy"
)

// register worker
type RegisterWorker struct {
	State string
}

type RegisterWorkerReply struct {
	WorkerId string
}

// worker state (basically heartbeat)
type HeartBeat struct {
	WorkerId string
	State    string
}

type HeartBeatReply struct {
	ShouldTerminate bool // if true then filenames is ignored
	IsMap           bool // map or reduce.
	Filename        string
	MaxReduceId     int // needed in map to know which reduce file to write to
	ReduceId        int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
