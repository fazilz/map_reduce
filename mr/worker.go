package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	registerWorker := register()
	worker := WorkerState{id: registerWorker.WorkerId, maxReduceId: registerWorker.MaxReduceId, state: WORKER_READY}
	for {
		reply := worker.heartBeat()
		if reply.ShouldTerminate {
			//log.Printf("worker %v exiting\n", worker.id)
			break
		} else if reply.IsMap {
			go worker.mapcall(reply.Filename, mapf)
		} else if reply.IsReduce {
			go worker.reduce(reply.ReduceId, reducef)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func (w *WorkerState) mapcall(filename string, mapf func(string, string) []KeyValue) {
	w.busy(WORKER_MAP)

	//log.Printf("WORKER: starting MAP on %s", filename)

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
	kva := mapf(filename, string(content))
	//log.Printf("total key value pairs %d", len(kva))
	sort.Sort(ByKey(kva))
	for _, kv := range kva {
		reduceId := ihash(kv.Key) % w.maxReduceId
		workerMap[reduceId] = append(workerMap[reduceId], kv)
	}

	for reduceId, kvs := range workerMap {
		//log.Printf("total kvs pairs %d for reduceId %d", len(kvs), reduceId)
		mrout := "mr-" + w.id + "-" + strconv.Itoa(reduceId) + ".json"
		ofile, _ := os.OpenFile(mrout, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		enc := json.NewEncoder(ofile)
		enc.Encode(&kvs)
		ofile.Close()
	}
	taskDone := TaskDone{w.id, WORKER_MAP}
	call("Coordinator.TaskDone", &taskDone, &taskDone)
	//log.Printf("WORKER: finished MAP on %s", filename)
	w.ready()
}

func (w *WorkerState) busy(state string) {
	w.Lock()
	w.state = state
	w.Unlock()
}

func (w *WorkerState) ready() {
	w.Lock()
	w.state = WORKER_READY
	w.Unlock()
}

func (w *WorkerState) reduce(reduceId int, reducef func(string, []string) string) {
	w.busy(WORKER_REDUCE)

	//log.Printf("Starting reduce task #%d", reduceId)

	reduceIdS := strconv.Itoa(reduceId)
	filenames := fileNamesWithSuffix(reduceIdS + ".json")

	kvs_map := make(map[string][]string)
	for _, filename := range filenames {
		//log.Printf("opening reduce file %v", filename)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kvs []KeyValue
			if err := dec.Decode(&kvs); err != nil {
				break
			}
			for _, kv := range kvs {
				kvs_map[kv.Key] = append(kvs_map[kv.Key], kv.Value)
			}
		}
		file.Close()
	}

	ofile, _ := os.Create("mr-out-" + reduceIdS + ".txt")
	defer ofile.Close()
	for k, vs := range kvs_map {
		fmt.Fprintf(ofile, "%v %v\n", k, reducef(k, vs))
	}
	taskDone := TaskDone{w.id, WORKER_REDUCE}
	call("Coordinator.TaskDone", &taskDone, &taskDone)
	//log.Printf("Finished reduce task #%d", reduceId)
	w.ready()
}

func register() RegisterWorkerReply {
	reply := RegisterWorkerReply{}
	call("Coordinator.RegisterWorker", RegisterWorker{"ready"}, &reply)
	return reply
}

func fileNamesWithSuffix(suffix string) []string {
	var filenames []string
	files, err := ioutil.ReadDir(".")
	if err != nil {
		log.Fatal(err)
	}
	for _, file := range files {
		if strings.HasSuffix(file.Name(), suffix) && !file.IsDir() {
			filenames = append(filenames, file.Name())
		}
	}
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
