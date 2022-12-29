package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func LogPrintf(format string, a ...interface{}) {
	str := fmt.Sprintf(format, a...)
	fmt.Printf("[%v][pid=%v]: "+str, time.Now().Format("01-02-2006 15:04:05.0000"), os.Getpid())
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func idChecker(reqid int, respid int) {
	if reqid != respid {
		log.Fatalf("Request worker: %v got an unmatched workerID: %v\n",
			reqid, respid)
	}
}

func deleteFiles(files []string) {
	for _, f := range files {
		os.Remove(f)
	}
}

func getMapTask() (*MapResponse, bool) {
	selfPid := os.Getpid()
	args := TaskRequest{WorkerId: selfPid}
	reply := new(MapResponse)
	for {
		*reply = MapResponse{}
		LogPrintf("Worker %v RPC GetMapTask\n", args.WorkerId)
		ok := call("Coordinator.GetMapTask", &args, reply)
		if !ok {
			log.Fatalln("RPC call failed in getMapTask")
		}

		idChecker(selfPid, reply.WorkerId)

		if reply.MapId >= 0 {
			LogPrintf("Worker %v got map work %v:%v\n", selfPid, reply.MapId, reply.Filename)
			return reply, true
		}

		if reply.MapId == -1 {
			// no more unassigned works to do
			return reply, false
		}

		if reply.MapId == -2 {
			LogPrintf("Wait for task re-assignment due to incomplete map phase.\n")
			time.Sleep(time.Second)
			continue
		}

		// UNREACHABLE
		log.Fatalf("Got invalid mapId: %v\n", reply.MapId)
	}
}

func executeMap(mapf func(string, string) []KeyValue, filename string) []KeyValue {
	mapfile, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(mapfile)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	mapfile.Close()

	kva := mapf(filename, string(content))
	return kva
}

func encodeMapResult(workerId int, mapId int, numReduce int,
	kvContents []KeyValue) string {

	filenames := make([]string, numReduce)
	interFiles := make([]*os.File, numReduce)
	jencoders := make([]*json.Encoder, numReduce)
	for reduceId := 0; reduceId < numReduce; reduceId++ {
		filenames[reduceId] = fmt.Sprintf("mr-%v-%v-%v", workerId, mapId, reduceId)
		f, err := os.Create(filenames[reduceId])
		if err != nil {
			log.Fatalf("Creating intermediate file %v failed.\n", filenames[reduceId])
		}
		interFiles[reduceId] = f
		jencoders[reduceId] = json.NewEncoder(f)
	}

	for _, kv := range kvContents {
		rid := ihash(kv.Key) % numReduce
		enc := jencoders[rid]
		enc.Encode(kv)
	}
	for i := 0; i < numReduce; i++ {
		interFiles[i].Close()
	}

	return strings.Join(filenames, ",")
}

func finishMap(mapId int, filenameCsv string) (int, bool) {
	selfPid := os.Getpid()
	args := MapFinishMsg{WorkerId: selfPid, MapId: mapId, FilenameCsv: filenameCsv}
	reply := TaskFinishResponse{}

	LogPrintf("Worker %v RPC FinishMap\n", args.WorkerId)
	ok := call("Coordinator.FinishMap", &args, &reply)
	if !ok {
		// if there are any errors, we delete the intermediate files
		deleteFiles(strings.Split(filenameCsv, ","))
		log.Fatalln("RPC call failed in finishMap")
	}

	idChecker(selfPid, reply.WorkerId)
	LogPrintf("Worker %v managed to notify the coordinator map done. waitmills = %v, askAgain = %v\n",
		selfPid, reply.WaitMilli, reply.AskAgain)
	return reply.WaitMilli, reply.AskAgain
}

func mapWrapper(mapf func(string, string) []KeyValue) {
	// RPC to the coordinator to get an unassigned map task
	for {
		reply, hasTask := getMapTask()
		if !hasTask {
			break
		}
		kva := executeMap(mapf, reply.Filename)

		filenameCsv := encodeMapResult(reply.WorkerId, reply.MapId, reply.NReduce, kva)

		// Notify the coordinator and get next task if any
		waitMilli, askAgain := finishMap(reply.MapId, filenameCsv)
		if !askAgain {
			break
		}
		if waitMilli > 0 {
			time.Sleep(time.Millisecond * time.Duration(waitMilli))
		}
	}
}

func getReduceTask() (*ReduceResponse, bool) {
	selfPid := os.Getpid()
	args := TaskRequest{WorkerId: selfPid}
	reply := new(ReduceResponse)

	for {
		*reply = ReduceResponse{}
		LogPrintf("Worker %v RPC GetReduceTask.\n", args.WorkerId)
		ok := call("Coordinator.GetReduceTask", &args, reply)
		if !ok {
			log.Fatalln("RPC call failed in getReduceTask")
		}

		idChecker(selfPid, reply.WorkerId)

		if reply.ReduceId >= 0 {
			LogPrintf("Worker %v got reduce work %v, files: %v\n", selfPid,
				reply.ReduceId, reply.FilenameCsv)
			return reply, true
		}

		if reply.ReduceId == -1 {
			// no more unassigned works to do
			return reply, false
		}

		if reply.ReduceId == -2 {
			// the coordinator is not ready yet to assign any reduce tasks
			LogPrintf("The coordinator is not ready yet, wait...\n")
			time.Sleep(time.Millisecond * time.Duration(100))
			continue
		}

		if reply.ReduceId == -3 {
			LogPrintf("Wait for task re-assignment due to incomplete reduce phase.\n")
			time.Sleep(time.Second)
			continue
		}

		// UNREACHABLE
		log.Fatalf("Got invalid reduceId: %v\n", reply.ReduceId)
	}
}

func decodeMapResult(fname string) []KeyValue {
	file, err := os.Open(fname)
	if err != nil {
		log.Fatalf("Cannot open file: %v\n", fname)
	}

	dec := json.NewDecoder(file)
	kva := []KeyValue{}
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}

	file.Close()
	return kva
}

func executeReduce(reducef func(string, []string) string, intermediate []KeyValue,
	workerId int, reduceId int) string {

	sort.Sort(ByKey(intermediate))
	oname := fmt.Sprintf("w%v-r%v-output.txt", workerId, reduceId)
	ofile, _ := os.Create(oname)
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
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
	return oname
}

func finishReduce(outfname string, reduceId int) (int, bool) {
	selfPid := os.Getpid()
	args := ReduceFinishMsg{WorkerId: selfPid, ReduceId: reduceId, Filename: outfname}
	reply := TaskFinishResponse{}

	LogPrintf("Worker %v RPC finishReduce.\n", args.WorkerId)
	ok := call("Coordinator.FinishReduce", &args, &reply)
	if !ok {
		// If by any chance we failed to tell the coordinator the reduce task we finished, it means
		// the coordinator has already got the whole work done and we can simply delete the file
		// and exit.
		os.Remove(outfname)
		log.Fatalln("RPC call failed in FinishReduce")
	}

	idChecker(selfPid, reply.WorkerId)

	LogPrintf("Worker %v managed to notify the coordinator reduce done. waitmills = %v, askAgain = %v\n",
		selfPid, reply.WaitMilli, reply.AskAgain)
	return reply.WaitMilli, reply.AskAgain
}

func reduceWrapper(reducef func(string, []string) string) {
	for {
		intermediate := []KeyValue{}
		// RPC to the coordinator to get an unassigned reduce task
		reply, hasTask := getReduceTask()
		if !hasTask {
			break
		}

		fnames := strings.Split(reply.FilenameCsv, ",")
		for _, fname := range fnames {
			kva := decodeMapResult(fname)
			intermediate = append(intermediate, kva...)
		}
		outfname := executeReduce(reducef, intermediate, reply.WorkerId, reply.ReduceId)

		waitMilli, askAgain := finishReduce(outfname, reply.ReduceId)
		deleteFiles(fnames) // it's safe to delete the intermediate file now
		if !askAgain {
			break
		}
		if waitMilli > 0 {
			time.Sleep(time.Millisecond * time.Duration(waitMilli))
		}
	}
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	mapWrapper(mapf)

	reduceWrapper(reducef)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		LogPrintf("reply.Y %v\n", reply.Y)
	} else {
		LogPrintf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
