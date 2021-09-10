package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type TaskInfo struct {
	files    []string
	status   string
	workerID int
	timer    *time.Timer
}

type Master struct {
	// Your definitions here.
	nReduce           int
	activeMapTasks    int // num of map tasks whose status is not Completed
	activeReduceTasks int // num of reduce tasks whose status is not Completed
	mapTasks          []*TaskInfo
	reduceTasks       []*TaskInfo
	mu                sync.Mutex
}

const (
	Idle       = "Idle"
	InProgress = "InProgress"
	Completed  = "Completed"
)

const (
	nMap          = 3
	verbose       = false
	WorkerTimeout = 10 * time.Second
	NoTaskTimeout = 5 * time.Second
)

func makeTaskInfo() *TaskInfo {
	ti := TaskInfo{status: Idle, workerID: -1}
	return &ti
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.activeMapTasks > 0 {
		m.assignIdleTask(args, reply, MapTask)
		if verbose {
			fmt.Printf("reply: %v\n", reply)
		}
		return nil
	}

	if m.activeReduceTasks > 0 {
		m.assignIdleTask(args, reply, ReduceTask)
		if verbose {
			fmt.Printf("reply: %v\n", reply)
		}
		return nil
	}

	return errors.New("All's done")
}

func (m *Master) TaskCompletion(args *TaskCompletionArgs,
	reply *TaskCompletionReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	switch task := args.TaskType; task {
	case MapTask:
		// update table
		mapTask := m.mapTasks[args.TaskNo]
		if mapTask.status != InProgress || mapTask.workerID != args.WorkerID {
			// ignore the message if already completed or
			// the task is assigned to another worker
			return nil
		}
		mapTask.status = Completed
		mapTask.timer.Stop()
		m.activeMapTasks--
		if verbose {
			fmt.Printf("%d %s remains\n", m.activeMapTasks, MapTask)
		}
		// add to reduce tasks
		for _, fname := range args.OFiles {
			reduceTaskNo, err := ExtractLastNumber(fname)
			if err != nil {
				log.Fatalf("Ill formatted output file: %v\n", fname)
				return errors.New("Bad output files")
			}

			if m.reduceTasks[reduceTaskNo] == nil {
				m.reduceTasks[reduceTaskNo] = makeTaskInfo()
				m.activeReduceTasks++
			}

			reduceTask := m.reduceTasks[reduceTaskNo]
			reduceTask.files = append(reduceTask.files, fname)
		}
	case ReduceTask:
		reduceTask := m.reduceTasks[args.TaskNo]
		if reduceTask.status != InProgress || reduceTask.workerID != args.WorkerID {
			return nil
		}
		reduceTask.status = Completed
		reduceTask.timer.Stop()
		m.activeReduceTasks--
		if verbose {
			fmt.Printf("%d %s remains\n", m.activeReduceTasks, ReduceTask)
		}
	default:
		fmt.Printf("Unexpected TaskType: %s\n", task)
		return errors.New("Bad TaskType")
	}
	if verbose {
		fmt.Printf("%s #%d completed, outputs in %v\n",
			args.TaskType, args.TaskNo, args.OFiles)
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	// Your code here.
	m.mu.Lock()
	defer m.mu.Unlock()
	ret := (m.activeMapTasks == 0) && (m.activeReduceTasks == 0)

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.nReduce = nReduce
	taskInfos := make([]*TaskInfo, nMap)
	for i := range taskInfos {
		taskInfos[i] = makeTaskInfo()
	}

	for i, v := range files {
		taskInfo := taskInfos[i%nMap]
		taskInfo.files = append(taskInfo.files, v)
	}
	m.mapTasks = taskInfos
	m.activeMapTasks = len(taskInfos)

	m.reduceTasks = make([]*TaskInfo, nReduce)
	// unlike map tasks, reduce tasks count are incremented
	// when files of intermediate outputs are sent back
	m.activeReduceTasks = 0

	m.server()
	return &m
}

// helper functions

// extract last number from intermediate file name with
// format like "out-1-3"
func ExtractLastNumber(fname string) (int, error) {
	lastDash := strings.LastIndex(fname, "-")
	return strconv.Atoi(fname[lastDash+1:])
}

// if exists idle task, assign it and return true
// else, return false
func (m *Master) assignIdleTask(args *AssignTaskArgs,
	reply *AssignTaskReply, taskType string) bool {
	// fetch proper table in the struct

	var table []*TaskInfo
	switch taskType {
	case MapTask:
		table = m.mapTasks
	case ReduceTask:
		table = m.reduceTasks
	default:
		log.Fatalf("Bad taskType %s\n", taskType)
		return false
	}
	for i, v := range table {
		if v == nil {
			continue
		}
		if v.status == Idle {
			// update mapTasks table
			v.status = InProgress
			v.workerID = args.WorkerID
			// update reply
			reply.TaskNo = i
			reply.Files = v.files
			reply.TaskType = taskType
			reply.NOut = m.nReduce
			if verbose {
				fmt.Printf("Master assigned %s #%d to %d\n", taskType, i, v.workerID)
			}
			// set a timer for TIMEOUT seconds
			// reset the task to Idle if not stopped
			f := func() {
				m.mu.Lock()
				defer m.mu.Unlock()
				v.status = Idle
				v.workerID = -1
				if verbose {
					fmt.Println("Timer expires, reset task")
				}
			}
			t := time.AfterFunc(WorkerTimeout, f)
			v.timer = t
			return true
		}
	}
	reply.TaskType = NoTask
	return false
}
