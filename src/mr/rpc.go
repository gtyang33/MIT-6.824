package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

type TaskType int
type Status int
const(
	MapTask TaskType = iota
	ReduceTask
	Done
)

const(
	idle Status = iota
	working
	timeout
	completed
)
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

// get task
type Task struct {
	Id int
	Typ TaskType
	FileName []string
	NReduce int
	TimeOut time.Duration
}

type GetTaskArgs struct {

}

type GetTaskReply struct {
	Task
	IsComplete bool
}

// report task status

type ReportArgs struct {
	Status
	Id int
	Typ TaskType
}

type ReportReply struct {

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
