package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type WorkerState struct {
	sync.Mutex
	id          string
	state       string
	maxReduceId int
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	worker := WorkerState{id: register(), state: WORKER_READY}
	for {
		reply := worker.heartBeat()
		worker.maxReduceId = reply.MaxReduceId
		log.Println(worker.id, reply)
		if reply.ShouldTerminate {
			fmt.Printf("worker %v exiting\n", worker.id)
			break
		} else if reply.IsMap {
			go worker.mapcall(reply.Filename, mapf)
		} else {
			go worker.reduce(reply.ReduceId, reducef)
		}
		time.Sleep(250 * time.Millisecond)
	}
	return
}

func (w *WorkerState) mapcall(filename string, mapf func(string, string) []KeyValue) {
	w.workerBusy()

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	workerMap := make(map[int][]KeyValue)
	scontent := string(content)
	kva := mapf(filename, scontent)
	for _, kv := range kva {
		reduceId := ihash(kv.Key) % w.maxReduceId
		workerMap[reduceId] = append(workerMap[reduceId], kv)
	}

	for reduceId, kvs := range workerMap {
        mrout := "mr-" + w.id + "-" + strconv.Itoa(reduceId)
		ofile, _ := os.Create(mrout)
		enc := json.NewEncoder(ofile)
		for _, kv := range kvs {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot enocode json in map function, %v", err)
			}
		}
        log.Println(ofile.Stat())
		ofile.Close()
	}
	w.workerReady()
}

func (w *WorkerState) workerBusy() {
	w.Lock()
	w.state = WORKER_BUSY
	w.Unlock()
}

func (w *WorkerState) workerReady() {
	w.Lock()
	w.state = WORKER_READY
	w.Unlock()
}

func (w *WorkerState) reduce(reduceId int, reducef func(string, []string) string) {
	w.workerBusy()

	reduceIdS := strconv.Itoa(reduceId)
	filenames := fileNamesWithSuffix(reduceIdS)

	kvs := make(map[string][]string)
	for _, filename := range filenames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvs[kv.Key] = append(kvs[kv.Key], kv.Value)
		}
	}

	ofile, _ := os.Create("mr-out-" + reduceIdS)
	//TODO: remove the multiple calls to Fprintf
	for k, v := range kvs {
		fmt.Fprintf(ofile, "%v %v\n", k, reducef(k, v))
	}
	ofile.Close()

	w.workerReady()
}

func register() string {
	reply := RegisterWorkerReply{}
	call("Coordinator.RegisterWorker", RegisterWorker{"ready"}, &reply)
	return reply.WorkerId
}

func fileNamesWithSuffix(suffix string) []string {
	var filenames []string
	return filenames
}

func (w *WorkerState) heartBeat() HeartBeatReply {
	reply := HeartBeatReply{}
	w.Lock()
	args := HeartBeat{w.id, w.state}
	call("Coordinator.WorkerHeartBeat", &args, &reply)
	w.Unlock()
	return reply
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
