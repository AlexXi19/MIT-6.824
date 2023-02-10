package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
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

	for {
		fmt.Printf("Worker %v, starting loop \n", w.WorkerId)

		// Fetch task from master or hang
		task := w.GetTask()

		// Perform task
		if task.TaskType == MapTask {
			time.Sleep(time.Second)
		} else if task.TaskType == ReduceTask {
			time.Sleep(time.Second)
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
