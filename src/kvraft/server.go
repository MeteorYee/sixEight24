package kvraft

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		str := fmt.Sprintf(format, a...)
		fmt.Printf("[%v]: "+str, time.Now().Format("01-02-2006 15:04:05.0000"))
	}
	return
}

const SV_CHAN_TIME_OUT = 2 // 2 seconds

type ServerError uint8

const (
	SvOK ServerError = iota
	SvChanTimeOut
	SvChanClosed
	SvUnknownKey
)

type Op struct {
	ClientId  int
	RequestId uint64
	Opcode    uint8
	Key       string
	Value     string
}

type ApplyReply struct {
	key    string
	value  string
	err    ServerError
	opcode uint8
}

type RequestEntry struct {
	clientId  int
	requestId uint64
	ch        chan ApplyReply
}

type KVServer struct {
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	mu    sync.Mutex        // protect the server's whole state
	table map[string]string // the table stores all of the KV pairs

	reqMap     map[int](map[uint64]RequestEntry) // {clientId : {requestId : RequestEntry}}
	maxSrvdIds map[int]uint64                    // {clientId : maxServedId for the client}
}

func (kv *KVServer) timedWait(clntId int, reqId uint64, ch <-chan ApplyReply, sec int) ApplyReply {
	select {
	case ret, ok := <-ch:
		if !ok {
			return ApplyReply{err: SvChanClosed}
		} else {
			return ret
		}
	case <-time.After(time.Duration(sec) * time.Second):
		// As the applier will only do the operation once, if we don't receive a valid response, it
		// means there might be someone who got the response from the channel, or the raft cluster
		// took too much time to reconfigure itself. We will let the client side decide what to do.
		DPrintf("kv:%v, client: %v, request id: %v, times out while waiting for applier!\n",
			kv.me, clntId, reqId)
		return ApplyReply{err: SvChanTimeOut}
	}
}

// `kv.mu` must be acquired while entering into the function
func (kv *KVServer) isRequestProcessing(clientId int, reqId uint64) bool {
	clientMap, ok := kv.reqMap[clientId]
	isProcessing := false
	if ok {
		_, isProcessing = clientMap[reqId]
	}
	return isProcessing
}

// `kv.mu` must be acquired while entering into the function
func (kv *KVServer) hasRequestServed(clientId int, reqId uint64) bool {
	maxSvId, ok := kv.maxSrvdIds[clientId]
	return ok && maxSvId >= reqId
}

// Return values:
// (1). the channel required to wait for the applier's reply;
// (2). whether the request has been served, where the first return value will be nil if true
func (kv *KVServer) buildRequestChan(clientId int, reqId uint64, isProcessing bool) (
	chan ApplyReply, bool) {

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.hasRequestServed(clientId, reqId) {
		return nil, true
	}

	clientMap, ok := kv.reqMap[clientId]
	if !ok {
		kv.assertf(!isProcessing, "buildRequestChan: ClientId:%v, RequestId:%v\n",
			clientId, reqId)
		clientMap = make(map[uint64]RequestEntry)
		kv.reqMap[clientId] = clientMap
	}

	var appch chan ApplyReply
	entry, hasEntry := clientMap[reqId]
	if hasEntry {
		kv.assertf(isProcessing, "a pre-existing request entry means a duplicated request arrived\n")
		DPrintf("kv:%v receives a duplicate (r:%v, c:%v).\n", kv.me, reqId, clientId)
		appch = entry.ch
	} else {
		appch = make(chan ApplyReply, 1)
		clientMap[reqId] = RequestEntry{clientId: clientId, requestId: reqId, ch: appch}
	}

	return appch, false
}

func (kv *KVServer) setRequestChan(clientId int, reqId uint64, ch chan ApplyReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	clientMap, ok := kv.reqMap[clientId]
	if !ok {
		clientMap = make(map[uint64]RequestEntry)
		kv.reqMap[clientId] = clientMap
	}
	clientMap[reqId] = RequestEntry{clientId: clientId, requestId: reqId, ch: ch}
}

func (kv *KVServer) explainErr4GetReply(serr ServerError, greply *GetReply) {
	switch serr {
	case SvOK:
		greply.Err = OK
	case SvChanTimeOut:
		greply.Err = ErrRetry
	case SvUnknownKey:
		greply.Err = ErrNoKey
	default:
		// Get operation shall not get a SvChanClosed error because it doesn't care about
		// duplicated requests.
		kv.assertf(false, "Got an invalid error code: %v when doing Get.\n", serr)
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	reply.RequestId = args.RequestId
	kv.mu.Lock()
	isProcessing := kv.isRequestProcessing(args.ClientId, args.RequestId)
	hasServed := kv.hasRequestServed(args.ClientId, args.RequestId)
	kv.mu.Unlock()

	kv.assertf(!isProcessing && !hasServed,
		"There shall never be duplicated Get requests.args: %+v, new: %v, served: %v\n",
		*args, isProcessing, hasServed)

	// Because Get operation has got no side effects, we don't care if the request id is duplicated
	// and just go ahead to do the work.
	index, term, isLeader := kv.rf.Start(Op{ClientId: args.ClientId, RequestId: args.RequestId,
		Opcode: OP_GET, Key: args.Key})
	DPrintf("kv:%v received Get (r:%v, c:%v), index:%v, term:%v\n", kv.me, args.RequestId,
		args.ClientId, index, term)

	if !isLeader {
		reply.Err = ErrWrongLeader
		DPrintf("kv:%v is not the leader in Get, (r:%v, c:%v)\n", kv.me, args.RequestId,
			args.ClientId)
		return
	}

	// make a buffered channel of size 1 to prevent from blocking the applier
	ch := make(chan ApplyReply, 1)
	kv.setRequestChan(args.ClientId, args.RequestId, ch)

	chReply := kv.timedWait(args.ClientId, args.RequestId, ch, SV_CHAN_TIME_OUT)

	kv.explainErr4GetReply(chReply.err, reply)
	if reply.Err != OK {
		return
	}

	_, isLeader = kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		DPrintf("kv:%v is not the leader in Get, (r:%v, c:%v)\n", kv.me, args.RequestId,
			args.ClientId)
		return
	}

	if chReply.key != args.Key || chReply.opcode != OP_GET {
		DPrintf("kv:%v, Get value's log entry may've been overriden, (r:%v, c:%v)\n",
			kv.me, args.RequestId, args.ClientId)
		reply.Err = ErrRetry
		return
	}

	reply.Value = chReply.value
}

func (kv *KVServer) explainErr4PtAppReply(serr ServerError, preply *PutAppendReply) {
	switch serr {
	case SvOK:
		fallthrough
	case SvChanClosed:
		// a closed channel means the request has been served
		preply.Err = OK
	case SvChanTimeOut:
		preply.Err = ErrRetry
	case SvUnknownKey:
		preply.Err = ErrNoKey
	default:
		kv.assertf(false, "Got an invalid error code: %v when doing P/A.\n", serr)
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	reply.RequestId = args.RequestId
	kv.mu.Lock()
	isProcessing := kv.isRequestProcessing(args.ClientId, args.RequestId)
	hasServed := kv.hasRequestServed(args.ClientId, args.RequestId)
	kv.mu.Unlock()

	//  isProcessing | hasServed | Possible?
	// --------------+-----------+----------
	//        T      |     T     |     F     served ones got no entries in the reqMap
	// --------------+-----------+----------
	//        T      |     F     |     T     a duplicate one and not served
	// --------------+-----------+----------
	//        F      |     T     |     F     processed ones
	// --------------+-----------+----------
	//        F      |     F     |     T     a new coming one
	//
	// A~B + ~AB + ~A~B = A~B + ~A
	kv.assertf((isProcessing && !hasServed) || !isProcessing,
		"P/A handler, args: %+v, new: %v, served: %v\n", *args, isProcessing, hasServed)
	if hasServed {
		reply.Err = OK
		DPrintf("kv:%v received a duplicated request (args: %+v) which has already been served.\n",
			kv.me, *args)
		return
	}

	index := -1
	var term int
	var isLeader bool
	if isProcessing {
		term, isLeader = kv.rf.GetState()
	} else {
		index, term, isLeader = kv.rf.Start(Op{ClientId: args.ClientId, RequestId: args.RequestId,
			Opcode: args.Opcode, Key: args.Key, Value: args.Value})
	}
	DPrintf("kv:%v received P/A (r:%v, c:%v), isProcessing:%v, hasServed:%v, index:%v"+
		", term:%v\n", kv.me, args.RequestId, args.ClientId, isProcessing, hasServed, index, term)

	if !isLeader {
		reply.Err = ErrWrongLeader
		DPrintf("kv:%v is not the leader in P/A, (r:%v, c:%v)\n", kv.me,
			args.RequestId, args.ClientId)
		return
	}

	ch, hasServed := kv.buildRequestChan(args.ClientId, args.RequestId, isProcessing)
	if hasServed { // we need to check this yet again
		reply.Err = OK
		DPrintf("kv:%v received a duplicated request (args: %+v) which has already been served.\n",
			kv.me, *args)
		return
	}

	chReply := kv.timedWait(args.ClientId, args.RequestId, ch, SV_CHAN_TIME_OUT)
	kv.explainErr4PtAppReply(chReply.err, reply)
	if reply.Err != OK {
		return
	}

	if chReply.key != args.Key || (chReply.opcode != OP_PUT && chReply.opcode != OP_APPEND) {
		DPrintf("kv:%v, P/A value's log entry may've been overriden, (r:%v, c:%v)\n",
			kv.me, args.RequestId, args.ClientId)
		reply.Err = ErrRetry
		return
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) assertf(assertion bool, format string, a ...interface{}) {
	if assertion {
		return
	}
	str := fmt.Sprintf(format, a...)
	log.Fatalf("[server=%v][ASSERT TRAP]: "+str, kv.me)
}

func (kv *KVServer) applyCommand(cmd *Op, index int) {
	reply := ApplyReply{key: cmd.Key, err: SvOK, opcode: cmd.Opcode}
	var foundKey bool
	kv.mu.Lock()

	var hasEntry bool
	var entry RequestEntry
	clientMap, clientExists := kv.reqMap[cmd.ClientId]
	if clientExists {
		entry, hasEntry = clientMap[cmd.RequestId]
		if hasEntry {
			kv.assertf(cmd.ClientId == entry.clientId, "clientId unmatched! (cmd: %v, entry: %+v)\n",
				cmd, entry)
			kv.assertf(cmd.RequestId == entry.requestId,
				"requestId unmatched! (cmd: %v, entry: %+v)\n", cmd, entry)
			delete(clientMap, cmd.RequestId)
		}
	}

	reply.value, foundKey = kv.table[cmd.Key]

	switch cmd.Opcode {
	case OP_GET:
		if !foundKey {
			reply.err = SvUnknownKey
		}
	case OP_PUT:
		kv.table[cmd.Key] = cmd.Value
	case OP_APPEND:
		if foundKey {
			kv.table[cmd.Key] += cmd.Value // TODO: need check duplicate first!!!
		} else {
			kv.table[cmd.Key] = cmd.Value
		}
	default:
		kv.assertf(false, "The applier has got an unknown op: %v\n", cmd.Opcode)
	}

	// update the max served id
	id, ok := kv.maxSrvdIds[cmd.ClientId]
	kv.maxSrvdIds[cmd.ClientId] = cmd.RequestId

	kv.mu.Unlock()
	if hasEntry {
		entry.ch <- reply
		close(entry.ch)
	}

	if ok && id >= cmd.RequestId {
		// This happens when the client first sent the request to server A who didn't make it to
		// commit the operation, but successfully distributed the log L1 to server B. Then the
		// client gave up communicating with A and connected with B, where a new but duplicate log
		// L2 generated, because at this time B had no idea that L1 (L1 carries the same information
		// with L2 in theory) had already been logged by A in the past. Henceforth, B will first
		// apply L1 which should be undoubtably executed and then ignore L2.
		DPrintf("kv:%v found a duplicate request (r:%v, c:%v) in log idx:%v, ignore it!\n", kv.me,
			cmd.RequestId, cmd.ClientId, index)
	} else {
		DPrintf("kv:%v applied command:%+v idx:%v, clientExists: %v, hasEntry: %v\n", kv.me, *cmd,
			index, clientExists, hasEntry)
	}
}

func (kv *KVServer) applyRoutine() {
	for msg := range kv.applyCh {
		if kv.killed() {
			break
		}

		if msg.SnapshotValid {
			// TODO
		} else if msg.CommandValid {
			cmd, ok := msg.Command.(Op)
			if ok {
				kv.applyCommand(&cmd, msg.CommandIndex)
			} else {
				DPrintf("[WARN]: the applier has got an invalid command: %v, index: %v\n",
					msg.Command, msg.CommandIndex)
			}
		} else {
			DPrintf("[WARN]: received an invalid apply msg, cmd: %v, cIdx: %v, sTerm: %v, sIdx: %v\n",
				msg.Command, msg.CommandIndex, msg.SnapshotTerm, msg.SnapshotIndex)
		}
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.table = make(map[string]string)
	kv.reqMap = make(map[int](map[uint64]RequestEntry))
	kv.maxSrvdIds = make(map[int]uint64)

	// TODO:
	// 1. implement the Get/Put/Append RPC calls
	go kv.applyRoutine()
	return kv
}
