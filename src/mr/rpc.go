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

// Add your RPC definitions here.
const (
	MapTask    = "MapTask"
	ReduceTask = "ReduceTask"
	NoTask     = "NoTask"
)

type AssignTaskArgs struct {
	WorkerID int
}

type AssignTaskReply struct {
	TaskNo   int
	Files    []string
	TaskType string
	NOut     int
}

type TaskCompletionArgs struct {
	WorkerID int
	TaskNo   int
	TaskType string
	OFiles   []string
}

type TaskCompletionReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
