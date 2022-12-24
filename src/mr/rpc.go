package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
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

// type RPCType int

// const (
// 	ASK_FOR_WORK RPCType = iota
// )

// The task could be map/reduce
type TaskRequest struct {
	WorkerId int
}

type MapResponse struct {
	WorkerId int
	NReduce  int
	MapId    int
	Filename string
}

type ReduceResponse struct {
	WorkerId    int
	ReduceId    int
	FilenameCsv string // comma sparated file names
}

type MapFinishMsg struct {
	WorkerId    int
	MapId       int
	FilenameCsv string // comma sparated names for intermediate files
}

type ReduceFinishMsg struct {
	WorkerId int
	ReduceId int
	Filename string
}

type TaskFinishResponse struct {
	WorkerId  int
	WaitMilli int
	AskAgain  bool
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
