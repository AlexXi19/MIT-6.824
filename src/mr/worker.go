package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"sort"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

const TIMEOUT_SECONDS = 10

// Enum for task type
const (
	MapTask    = "MapTask"
	ReduceTask = "ReduceTask"
	Wait       = "Wait"
	Complete   = "Complete"
)

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

	i := rand.Intn(30)
	fmt.Printf("Worker %v, started \n", i)

	worker := WorkerNode{i}
	worker.StartWorking(mapf, reducef)
}

type WorkerNode struct {
	WorkerId int
}

func (w *WorkerNode) StartWorking(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	metadata := w.GetMetadata()

	for {
		fmt.Printf("Worker %v, starting loop \n", w.WorkerId)

		// Fetch task from master or hang
		task := w.GetTask()

		// Perform task
		if task.TaskType == MapTask {
			performMap(mapf, task.Filename, task.TaskId, metadata.NReduce)
		} else if task.TaskType == ReduceTask {
			performReduce(reducef, task.TaskId, metadata.NMap)
		} else if task.TaskType == Complete {
			fmt.Printf("Worker %v, completed all tasks \n", w.WorkerId)
			return
		} else {
			panic("Invalid task type")
		}

		// Report task completion to master
		w.CompleteTask(task)
		fmt.Printf("Worker %v, completed task %v \n", w.WorkerId, task.TaskId)
	}
}

func performMap(mapf func(string, string) []KeyValue, filename string, mapTaskId int, NReduce int) {
	fmt.Printf("Performing map task %v \n", mapTaskId)
	content := ReadFile(filename)
	kva := mapf(filename, string(content))

	// Write intermediate files
	for _, kv := range kva {
		reduceTaskId := ihash(kv.Key) % NReduce
		intermediateFilename := fmt.Sprintf("tmp-reduce/mr-%v-%v", mapTaskId, reduceTaskId)
		WriteIntermediateFile(intermediateFilename, kv)
	}
}

// This function doesn't need to be locked
func WriteIntermediateFile(filename string, kv KeyValue) {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("cannot open %v: %v", filename, err)
		panic(err)
	}
	defer file.Close()

	fmt.Fprintf(file, "%v %v\n", kv.Key, kv.Value)
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func performReduce(reducef func(string, []string) string, reduceTaskId int, NMap int) {
	fmt.Printf("Performing reduce task %v \n", reduceTaskId)

	// Read all intermediate files
	intermediate := []KeyValue{}
	for i := 0; i < NMap; i++ {
		intermediateFilename := fmt.Sprintf("tmp-reduce/mr-%v-%v", i, reduceTaskId)
		intermediate = append(intermediate, DeserializeReduceTask(intermediateFilename)...)
	}

	// Sort by key
	sort.Sort(ByKey(intermediate))

	// Perform reduce
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		WriteOutput(intermediate[i].Key, output)

		i = j
	}

}

// This function needs to be write locked
func WriteOutput(key string, value string) {
	file, err := os.OpenFile("mr-out", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("cannot open %v: %v", "mr-out", err)
		panic(err)
	}
	defer file.Close()

	_, err = fmt.Fprintf(file, "%v %v\n", key, value)
	if err != nil {
		log.Fatalf("cannot write to %v", "mr-out")
		panic(err)
	}
}

func DeserializeReduceTask(filename string) []KeyValue {
	content := ReadFile(filename)

	// Create key value map by reading line by line
	// and splitting by space
	kvMap := []KeyValue{}
	for _, line := range strings.Split(string(content), "\n") {
		if line == "" {
			continue
		}
		split := strings.Split(line, " ")
		key := split[0]
		value := split[1]
		kvMap = append(kvMap, KeyValue{key, value})
	}
	return kvMap

}

func ReadFile(filename string) []byte {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		panic(err)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		panic(err)
	}
	file.Close()

	return content
}

func (w *WorkerNode) GetTask() Task {
	startTime := time.Now().Unix()
	for {
		reply := Task{}
		request := GetTaskRequest{
			WorkerId: w.WorkerId,
		}

		call("Master.GetTask", &request, &reply)

		fmt.Printf("Task obtained: Task type: %v \n", reply.TaskType)

		if reply.TaskType == Wait {
			time.Sleep(3 * time.Second)
			if time.Now().Unix()-startTime > TIMEOUT_SECONDS {
				panic("Timed out")
			}
		} else if reply.TaskType == MapTask || reply.TaskType == ReduceTask || reply.TaskType == Complete {
			return reply
		} else {
			panic("Invalid task type")
		}
	}
}

func (w *WorkerNode) GetMetadata() Metadata {
	reply := Metadata{}
	request := GetMetadataRequest{
		WorkerId: w.WorkerId,
	}

	call("Master.GetMetadata", &request, &reply)

	return reply
}

func (w *WorkerNode) CompleteTask(task Task) {
	fmt.Printf("Worker %v, completing task %v \n", w.WorkerId, task.TaskId)
	args := CompleteTaskRequest{
		WorkerId: w.WorkerId,
		TaskId:   task.TaskId,
	}
	reply := CompleteTaskResponse{}

	call("Master.CompleteTask", &args, &reply)
	if !reply.Success {
		panic("Failed to complete task")
	}
}
