package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
)
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

//task types
type TaskType int

const (
	MapTask    = 0
	ReduceTask = 1
)

type Task struct {
	TaskType TaskType //MapTask or ReduceTask
	TaskId   int      //the index of MapTask or the ReduceTask
	NReduce  int      //the number of all ReduceTasks
	Filename string   //the input filename of this task
}

//type TaskReduce struct {
//	ReduceId  int
//	StartTime time.Time
//}

type AssignArgs struct {
}

const (
	NotDone      = 0
	MapMaybeDone = 1
	MaybeDone    = 2
	Done         = 3
)

type AssignReply struct {
	Task Task
	Done int
}

type MapDoneArgs struct {
}

type MapDoneReply struct {
	Done bool
}

type DoneArgs struct {
	Task           Task
	OutputFileName map[string]bool
}

type DoneReply struct {
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
