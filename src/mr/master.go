package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type Master struct {
	// Your definitions here.
	TaskQueue        []*Task
	TaskPendingQueue []*Task
	nReduce          int
	completed        bool
	taskId           int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (m *Master) GetTask(args *GetTaskRequest, reply *Task) error {
	var task *Task
	if len(m.TaskQueue) == 0 {
		if !m.completed {
			task = &Task{
				TaskType: Wait,
				Filename: "",
				TaskId:   -1,
				Start:    -1,
			}
		} else {
			task = &Task{
				TaskType: Complete,
				Filename: "",
				TaskId:   -1,
				Start:    -1,
			}
		}
	} else {
		task = m.TaskQueue[0]
		task.Start = time.Now().Unix()
		m.TaskPendingQueue = append(m.TaskPendingQueue, task)
		m.TaskQueue = m.TaskQueue[1:]
	}

	reply.Filename = task.Filename
	reply.TaskId = task.TaskId
	reply.TaskType = task.TaskType
	reply.Start = task.Start

	fmt.Printf("RPC call processed, task assigned to worker: %v with id: %v with type: %s \n", args.WorkerId, reply.TaskId, reply.TaskType)
	return nil
}

func (m *Master) removeTaskFromQueue(taskId int) (*Task, error) {
	for i, task := range m.TaskPendingQueue {
		if task.TaskId == taskId {
			m.TaskPendingQueue = append(m.TaskPendingQueue[:i], m.TaskPendingQueue[i+1:]...)
			return task, nil
		}
	}
	return nil, errors.New("Task not found")
}

func (m *Master) CompleteTask(args *CompleteTaskRequest, reply *CompleteTaskResponse) error {
	// Remove task from pending queue
	removedTask, err := m.removeTaskFromQueue(args.TaskId)
	if err != nil {
		fmt.Println(err)
		reply.Success = false
	}

	if len(m.TaskPendingQueue) == 0 && len(m.TaskQueue) == 0 {
		if removedTask.TaskType == MapTask {
			// Populate reduce tasks
			for i := 0; i < m.nReduce; i++ {
				m.TaskQueue = append(m.TaskQueue, &Task{
					TaskType: ReduceTask,
					Filename: "",
					TaskId:   m.taskId,
				})
				m.taskId++
			}

		} else if removedTask.TaskType == ReduceTask {
			// All tasks completed
			m.completed = true
		} else {
			panic("Invalid task type")
		}
	}

	// Add task to completed queue
	reply.Success = true

	fmt.Printf("RPC call processed, task completed by worker: %v with id: %v with type: %s \n", args.WorkerId, args.TaskId, removedTask.TaskType)

	return nil
}

// If a task stays in the pending queue for too long, it should be moved fromm pending to the task queue
func (m *Master) checkTaskDeadline() {
	currTime := time.Now().Unix()
	for _, task := range m.TaskPendingQueue {
		if currTime-task.Start > TIMEOUT_SECONDS {
			fmt.Printf("Task %v timed out, moving to task queue \n", task.TaskId)
			m.TaskQueue = append(m.TaskQueue, task)
			m.removeTaskFromQueue(task.TaskId)
		}
	}

}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	return m.completed
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{nReduce: nReduce}
	taskCounter := 0

	// Create map tasks
	fmt.Println("Creating map tasks...")
	for _, file := range files {
		task := Task{
			TaskType: MapTask,
			Filename: file,
			TaskId:   taskCounter,
		}
		m.TaskQueue = append(m.TaskQueue, &task)
		taskCounter++
	}
	m.taskId = taskCounter

	m.server()

	go func() {
		for {
			m.checkTaskDeadline()
			time.Sleep(1 * time.Second)
		}
	}()

	return &m
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}

	fmt.Println("Server is up and running!")
	go http.Serve(l, nil)
}
