package mr

import (
	"container/heap"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strings"
	"sync"
	"time"
)

const RE_ASSIGN_THRESHOLD = 5

type WorkItem struct {
	workId         int
	lastAssignTime int64
}

// A PriorityQueue implements heap.Interface and holds Items.
type PriorityQueue []*WorkItem

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].lastAssignTime < pq[j].lastAssignTime
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *PriorityQueue) Push(x interface{}) {
	item := x.(*WorkItem)
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	*pq = old[0 : n-1]
	return item
}

type Coordinator struct {
	taskMutex  sync.Mutex
	taskStack  []string
	top        int
	nReduce    int
	mFinishCnt int
	rAssignCnt int
	rFinishCnt int
	mDoneFlags []bool
	rDoneFlags []bool
	reduceList [][]string
	mAssignpq  PriorityQueue
	rAssignpq  PriorityQueue
}

func isTimeToReassign(pq *PriorityQueue, doneFlags []bool) bool {
	for len(*pq) > 0 && doneFlags[(*pq)[0].workId] {
		heap.Pop(pq)
	}
	return len(*pq) > 0 && (time.Now().Unix()-(*pq)[0].lastAssignTime) > RE_ASSIGN_THRESHOLD
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// RPC handler for workers asking for works.
func (c *Coordinator) GetMapTask(req *TaskRequest, resp *MapResponse) error {
	LogPrintf("Received map request from worker %v\n", req.WorkerId)
	resp.WorkerId = req.WorkerId
	c.taskMutex.Lock()
	defer c.taskMutex.Unlock()

	if c.top >= 0 {
		resp.Filename = c.taskStack[c.top]
		resp.NReduce = c.nReduce
		resp.MapId = c.top
		LogPrintf("Assign [map]:%v, file: %v to worker: %v, remaining # of tasks: %v\n",
			c.top, resp.Filename, resp.WorkerId, c.top)
		c.top--
		item := &WorkItem{workId: resp.MapId, lastAssignTime: time.Now().Unix()}
		heap.Push(&c.mAssignpq, item)
	} else if isTimeToReassign(&c.mAssignpq, c.mDoneFlags) {
		item := heap.Pop(&c.mAssignpq).(*WorkItem)
		resp.Filename = c.taskStack[item.workId]
		resp.NReduce = c.nReduce
		resp.MapId = item.workId
		item.lastAssignTime = time.Now().Unix()
		heap.Push(&c.mAssignpq, item)
		LogPrintf("Re-assign [map]:%v, file: %v to worker: %v, remaining # of tasks: %v\n",
			resp.MapId, resp.Filename, resp.WorkerId, (len(c.taskStack) - c.mFinishCnt))
	} else {
		if c.mFinishCnt == len(c.taskStack) {
			resp.MapId = -1
			LogPrintf("Worker %v got no map task assigned.\n", resp.WorkerId)
		} else {
			resp.MapId = -2
			LogPrintf("There are unfinished map works, the worker %v needs to wait.\n",
				resp.WorkerId)
		}
	}
	return nil
}

func (c *Coordinator) FinishMap(req *MapFinishMsg, resp *TaskFinishResponse) error {
	LogPrintf("Received map finish msg from worker %v, files: %v\n",
		req.WorkerId, req.FilenameCsv)

	resp.WorkerId = req.WorkerId
	resp.AskAgain = false
	resp.WaitMilli = 0
	c.taskMutex.Lock()
	defer c.taskMutex.Unlock()

	if c.mFinishCnt == len(c.taskStack) {
		return nil
	}

	resp.AskAgain = true
	resp.WaitMilli = 100
	if c.mDoneFlags[req.MapId] {
		// todo: return error and make the worker delete the intermediate file
		return fmt.Errorf("the map task: %v has already been completed", req.MapId)
	}

	filenames := strings.Split(req.FilenameCsv, ",")
	if len(filenames) != c.nReduce {
		return fmt.Errorf("invalid length of filenameCsv. actual: %v, expect: %v",
			len(filenames), c.nReduce)
	}
	for reduceId := 0; reduceId < c.nReduce; reduceId++ {
		c.reduceList[reduceId] = append(c.reduceList[reduceId], filenames[reduceId])
	}
	c.mDoneFlags[req.MapId] = true
	c.mFinishCnt++
	if c.mFinishCnt == len(c.taskStack) {
		// mark the empty reduce buckets as finished
		for i, rfiles := range c.reduceList {
			if len(rfiles) == 0 {
				c.rFinishCnt++
				c.rDoneFlags[i] = true
				LogPrintf("reduceList[%v] is empty\n", i)
			}
		}
		resp.AskAgain = false
		resp.WaitMilli = 0
	}
	LogPrintf("FinishMap end mFinishCnt = %v, ask = %v, len.stack = %v\n",
		c.mFinishCnt, resp.AskAgain, len(c.taskStack))
	return nil
}

func (c *Coordinator) GetReduceTask(req *TaskRequest, resp *ReduceResponse) error {
	LogPrintf("Received reduce request from worker %v\n", req.WorkerId)
	resp.WorkerId = req.WorkerId
	resp.ReduceId = -1
	c.taskMutex.Lock()
	defer c.taskMutex.Unlock()

	if c.mFinishCnt != len(c.taskStack) {
		// this means there still are some unfinished map tasks
		resp.ReduceId = -2
		return nil
	}

	for c.rAssignCnt < c.nReduce {
		if c.rDoneFlags[c.rAssignCnt] {
			c.rAssignCnt++
			continue
		}
		resp.FilenameCsv = strings.Join(c.reduceList[c.rAssignCnt], ",")
		resp.ReduceId = c.rAssignCnt
		c.rAssignCnt++
		LogPrintf("Assign [reduce]:%v to worker: %v\n", c.rAssignCnt, req.WorkerId)

		item := &WorkItem{workId: resp.ReduceId, lastAssignTime: time.Now().Unix()}
		heap.Push(&c.rAssignpq, item)
		break
	}

	if resp.ReduceId >= 0 {
		return nil
	}

	if isTimeToReassign(&c.rAssignpq, c.rDoneFlags) {
		item := heap.Pop(&c.rAssignpq).(*WorkItem)
		resp.FilenameCsv = strings.Join(c.reduceList[item.workId], ",")
		resp.ReduceId = item.workId
		item.lastAssignTime = time.Now().Unix()
		LogPrintf("Re-assign [reduce]:%v to worker: %v\n", item.workId, req.WorkerId)
		heap.Push(&c.rAssignpq, item)
	} else if c.rFinishCnt == c.nReduce {
		LogPrintf("Worker %v got no reduce task assigned.\n", req.WorkerId)
	} else {
		resp.ReduceId = -3
		LogPrintf("There are unfinished reduce works, the worker %v needs to wait.\n",
			resp.WorkerId)
	}
	return nil
}

func (c *Coordinator) FinishReduce(req *ReduceFinishMsg, resp *TaskFinishResponse) error {
	LogPrintf("Received reduce finish msg from worker %v, reduceId: %v, file: %v\n",
		req.WorkerId, req.ReduceId, req.Filename)

	if req.ReduceId < 0 || req.ReduceId >= c.nReduce {
		return fmt.Errorf("request from worker: %v contains an invalid reduceId: %v",
			req.WorkerId, req.ReduceId)
	}

	resp.WorkerId = req.WorkerId
	resp.AskAgain = true
	resp.WaitMilli = 100
	c.taskMutex.Lock()
	defer c.taskMutex.Unlock()

	if c.rDoneFlags[req.ReduceId] {
		LogPrintf("Reduce task: %v has already been completed.\n", req.ReduceId)
		return nil
	}

	c.rFinishCnt++
	c.rDoneFlags[req.ReduceId] = true
	if c.rFinishCnt == c.nReduce {
		resp.AskAgain = false
		resp.WaitMilli = 0
	}
	// rename the file
	fname := fmt.Sprintf("mr-out-%v", req.ReduceId)
	LogPrintf("Renaming %v to %v\n", req.Filename, fname)
	err := os.Rename(req.Filename, fname)
	if err != nil {
		return fmt.Errorf("renaming file: %v failed: %w", req.Filename, err)
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false
	c.taskMutex.Lock()
	ret = c.rFinishCnt == c.nReduce
	c.taskMutex.Unlock()
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.taskStack = files
	c.top = len(c.taskStack) - 1
	c.nReduce = nReduce
	c.mFinishCnt = 0
	c.mDoneFlags = make([]bool, len(c.taskStack))
	c.rAssignCnt = 0
	c.rFinishCnt = 0
	c.rDoneFlags = make([]bool, nReduce)
	c.reduceList = make([][]string, nReduce)
	for i := 0; i < len(c.taskStack); i++ {
		c.mDoneFlags[i] = false
	}
	for i := 0; i < nReduce; i++ {
		c.rDoneFlags[i] = false
	}

	c.mAssignpq = make(PriorityQueue, 0)
	heap.Init(&c.mAssignpq)

	c.rAssignpq = make(PriorityQueue, 0)
	heap.Init(&c.rAssignpq)

	c.server()
	return &c
}
