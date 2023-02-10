package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

const num_workers = 1

// Enum for task type
const (
	MapTask    = "MapTask"
	ReduceTask = "ReduceTask"
	Wait       = "Wait"
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

	// Your worker implementation here.
	for i := 0; i < num_workers; i++ {
		worker := WorkerNode{i}
		go worker.StartWorking(mapf, reducef)
	}
}

type WorkerNode struct {
	WorkerId int
}

func (w *WorkerNode) StartWorking(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		fmt.Println("Worker {}, starting loop", w.WorkerId)

		// Fetch task from master or hang
		task := w.GetTask()
		fmt.Println("Worker {}, got task {}", w.WorkerId, task.TaskId)

		// Perform task

		// Report task completion to master
		w.CompleteTask(task)
		fmt.Println("Worker {}, completed task {}", w.WorkerId, task.TaskId)
	}
}

func ReadFile(filename string) ([]byte, error) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		return nil, err
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		return nil, err
	}
	file.Close()

	return content, nil
}

func (w *WorkerNode) GetTask() Task {
	for {
		reply := Task{}
		request := GetTaskRequest{
			WorkerId: w.WorkerId,
		}

		call("Master.GetTask", &request, &reply)

		if reply.TaskType == Wait {
			time.Sleep(3 * time.Second)
		} else if reply.TaskType == MapTask || reply.TaskType == ReduceTask {
			return reply
		} else {
			panic("Invalid task type")
		}
	}
}

func (w *WorkerNode) CompleteTask(task Task) {
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
