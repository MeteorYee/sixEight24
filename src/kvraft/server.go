package kvraft

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		str := fmt.Sprintf(format, a...)
		fmt.Printf("[%v]: "+str, time.Now().Format("01-02-2006 15:04:05.0000"))
	}
	return
}

const SV_CHAN_TIME_OUT = 1      // 1 second
const SV_SNAPSHOT_FACTOR = 1.25 // when it's SV_SNAPSHOT_FACTOR * maxraftstate, we do snapshot
const SV_APPLY_CHAN_SIZE = 32   // the apply channel buffer size

type ServerError uint8

const (
	SvOK ServerError = iota
	SvChanTimeOut
	SvChanClosed
	SvUnknownKey
	SvSnapshotApply
)

type Op struct {
	ClientId  int64
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
	retryCnt int
	ch       chan ApplyReply
}

type KVServer struct {
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	snpshtSwitch int32      // used to prevent redundant notifications for snapshot
	snpshtcond   *sync.Cond // snapshot conditinal varialbe
	mu           sync.Mutex // protect the server's whole state
	persister    *raft.Persister

	maxraftstate int // snapshot if log grows this big

	// === Persistent States ===
	table      map[string]string // the table stores all of the KV pairs
	maxSrvdIds map[int64]uint64  // {clientId : maxServedId for the client}
	lastAppIdx int

	// === Volatile States ===
	reqMap map[int64](map[uint64]RequestEntry) // {clientId : {requestId : RequestEntry}}
}

func (kv *KVServer) timedWait(clntId int64, reqId uint64, ch <-chan ApplyReply, sec int) ApplyReply {
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
		DPrintf("kv:%v, (r:%v, c:%v), times out while waiting for applier!\n",
			kv.me, reqId, clntId)
		return ApplyReply{err: SvChanTimeOut}
	}
}

func (kv *KVServer) setRequestChan(clientId int64, reqId uint64, ch chan ApplyReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	clientMap, ok := kv.reqMap[clientId]
	if !ok {
		clientMap = make(map[uint64]RequestEntry)
		kv.reqMap[clientId] = clientMap
	}
	delete(clientMap, reqId) // clear any pre-existing entry
	clientMap[reqId] = RequestEntry{retryCnt: 0, ch: ch}
}

func (kv *KVServer) delRequestChan(clientId int64, reqId uint64) {
	kv.mu.Lock()
	clientMap, ok := kv.reqMap[clientId]
	if ok {
		delete(clientMap, reqId)
	}
	kv.mu.Unlock()
}

func (kv *KVServer) explainErr4GetReply(serr ServerError, greply *GetReply) {
	switch serr {
	case SvOK:
		greply.Err = OK
	case SvSnapshotApply:
		// When we get the reply from the snapshoter, it means we're not the leader any more.
		greply.Err = ErrWrongLeader
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

	// make a buffered channel of size 1 to prevent from blocking the applier
	ch := make(chan ApplyReply, 1)
	kv.setRequestChan(args.ClientId, args.RequestId, ch)

	// Because Get operation has got no side effects, we don't care if the request id is duplicated
	// and just go ahead to do the work.
	index, term, isLeader := kv.rf.Start(Op{ClientId: args.ClientId, RequestId: args.RequestId,
		Opcode: OP_GET, Key: args.Key})
	DPrintf("kv:%v received Get (r:%v, c:%v), index:%v, term:%v\n", kv.me, args.RequestId,
		args.ClientId, index, term)

	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.delRequestChan(args.ClientId, args.RequestId)
		DPrintf("kv:%v is not the leader in Get, (r:%v, c:%v)\n", kv.me, args.RequestId,
			args.ClientId)
		return
	}

	chReply := kv.timedWait(args.ClientId, args.RequestId, ch, SV_CHAN_TIME_OUT)
	if chReply.err == SvChanTimeOut {
		// prevent channel leak
		kv.delRequestChan(args.ClientId, args.RequestId)
	}

	kv.explainErr4GetReply(chReply.err, reply)
	if reply.Err != OK {
		return
	}

	reply.Value = chReply.value
	kv.assertf(chReply.key == args.Key && chReply.opcode == OP_GET,
		"mistaken reply in Get! (r:%v, c:%v)\n", args.RequestId, args.ClientId)
}

func (kv *KVServer) explainErr4PtAppReply(serr ServerError, preply *PutAppendReply) {
	switch serr {
	case SvOK:
		fallthrough
	case SvSnapshotApply:
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
	ch := make(chan ApplyReply, 1)
	kv.setRequestChan(args.ClientId, args.RequestId, ch)

	index, term, isLeader := kv.rf.Start(Op{ClientId: args.ClientId, RequestId: args.RequestId,
		Opcode: args.Opcode, Key: args.Key, Value: args.Value})
	DPrintf("kv:%v received P/A (r:%v, c:%v), index:%v, term:%v\n", kv.me, args.RequestId,
		args.ClientId, index, term)

	if !isLeader {
		reply.Err = ErrWrongLeader
		DPrintf("kv:%v is not the leader in P/A, (r:%v, c:%v)\n", kv.me,
			args.RequestId, args.ClientId)
		kv.delRequestChan(args.ClientId, args.RequestId)
		return
	}

	chReply := kv.timedWait(args.ClientId, args.RequestId, ch, SV_CHAN_TIME_OUT)
	kv.explainErr4PtAppReply(chReply.err, reply)
	if reply.Err != OK {
		return
	}

	kv.assertf(chReply.err == SvSnapshotApply || (chReply.key == args.Key && chReply.opcode ==
		args.Opcode), "mistaken reply in P/A! (r:%v, c:%v)\n", args.RequestId, args.ClientId)
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
	kv.snpshtcond.Signal()
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
	var hasEntry bool
	var entry RequestEntry

	kv.mu.Lock()

	clientMap, clientExists := kv.reqMap[cmd.ClientId]
	if clientExists {
		entry, hasEntry = clientMap[cmd.RequestId]
		if hasEntry {
			delete(clientMap, cmd.RequestId)
		}
	}

	reply.value, foundKey = kv.table[cmd.Key]
	id, ok := kv.maxSrvdIds[cmd.ClientId]
	isdup := ok && id >= cmd.RequestId

	if !isdup {
		// update the max served id if it applies
		kv.maxSrvdIds[cmd.ClientId] = cmd.RequestId

		switch cmd.Opcode {
		case OP_GET:
			if !foundKey {
				reply.err = SvUnknownKey
			}
		case OP_PUT:
			kv.table[cmd.Key] = cmd.Value
		case OP_APPEND:
			if foundKey {
				kv.table[cmd.Key] += cmd.Value
			} else {
				kv.table[cmd.Key] = cmd.Value
			}
		default:
			kv.assertf(false, "The applier has got an unknown op: %v\n", cmd.Opcode)
		}
	}

	kv.assertf(index == kv.lastAppIdx+1, "raft index out of order! prev:%v, cur:%v\n",
		kv.lastAppIdx, index)
	kv.lastAppIdx = index

	kv.mu.Unlock()

	if hasEntry {
		entry.ch <- reply
		close(entry.ch)
	}

	if isdup {
		// This happens when the client first sent the request to server A who didn't make it to
		// commit the operation, but successfully distributed the log L1 to server B. Then the
		// client gave up communicating with A and connected with B, where a new but duplicate log
		// L2 generated, because at this time B had no idea that L1, which carries basically the
		// same information as L2, had already been logged by A in the past. Henceforth, B will
		// first undoubtably apply L1 and then ignore L2.
		DPrintf("kv:%v found a duplicate request (r:%v, c:%v) in log idx:%v, ignore it!\n", kv.me,
			cmd.RequestId, cmd.ClientId, index)
	}
	// else {
	// 	DPrintf("kv:%v applied command:%+v idx:%v, clientExists: %v, hasEntry: %v\n", kv.me, *cmd,
	// 		index, clientExists, hasEntry)
	// }

	if kv.time4Snapshot() && kv.turnOnSnpshtSwitch() {
		kv.snpshtcond.Signal()
		DPrintf("kv:%v signaled the snapshoter (r:%v, c:%v) at log idx:%v. raft size:%v\n", kv.me,
			cmd.RequestId, cmd.ClientId, index, kv.persister.RaftStateSize())
	}
}

func (kv *KVServer) applyRoutine() {
	for msg := range kv.applyCh {
		if kv.killed() {
			break
		}

		if msg.SnapshotValid {
			DPrintf("kv:%v is applying snapshot, idx:%v, term:%v\n", kv.me, msg.SnapshotIndex,
				msg.SnapshotTerm)
			kv.applySnapshot(msg.Snapshot, msg.SnapshotIndex)
		} else if msg.CommandValid {
			cmd, ok := msg.Command.(Op)
			kv.assertf(ok, "The applier has got an invalid command: %v, index: %v\n",
				msg.Command, msg.CommandIndex)
			kv.applyCommand(&cmd, msg.CommandIndex)
		} else {
			kv.assertf(false,
				"Received an invalid apply msg, cmd: %v, cIdx: %v, sTerm: %v, sIdx: %v\n",
				msg.Command, msg.CommandIndex, msg.SnapshotTerm, msg.SnapshotIndex)
		}
	}
}

func (kv *KVServer) time4Snapshot() bool {
	return kv.maxraftstate > 0 && float64(kv.persister.RaftStateSize()) >
		float64(kv.maxraftstate)*SV_SNAPSHOT_FACTOR
}

func (kv *KVServer) snapshot() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(kv.table)
	e.Encode(kv.maxSrvdIds)
	e.Encode(kv.lastAppIdx)

	kv.rf.Snapshot(kv.lastAppIdx, w.Bytes())
	DPrintf("kv:%v saving snapshot, lastAppIdx:%v, raft size:%v\n", kv.me, kv.lastAppIdx,
		kv.persister.RaftStateSize())

	isoff := kv.turnOffSnpshtSwitch()
	kv.assertf(isoff, "Some sneaky guy has switched off `snpshtSwitch`!\n")
}

func (kv *KVServer) applySnapshot(data []byte, sindex int) {
	kv.assertf(len(data) > 0, "Invalid data in applySnapshot!\n")

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.table = make(map[string]string)
	err := d.Decode(&kv.table)
	kv.assertf(err == nil, "Failed to decode `table` when applying snapshot, err: %v\n", err)

	kv.maxSrvdIds = make(map[int64]uint64)
	err = d.Decode(&kv.maxSrvdIds)
	kv.assertf(err == nil, "Failed to decode `maxSrvdIds` when applying snapshot, err: %v\n", err)

	kv.lastAppIdx = 0
	err = d.Decode(&kv.lastAppIdx)
	kv.assertf(err == nil, "Failed to decode `lastAppIdx` when applying snapshot, err: %v\n", err)

	if sindex > 0 {
		kv.assertf(sindex == kv.lastAppIdx,
			"Found an inconsistent snapshot: sIdx:%v, savedIdx:%v\n", sindex, kv.lastAppIdx)
	}

	// the applier clear the request map as much as it can
	for clientId, clientMap := range kv.reqMap {
		maxSrvdId := kv.maxSrvdIds[clientId]
		for reqId, entry := range clientMap {
			if reqId <= maxSrvdId {
				entry.ch <- ApplyReply{err: SvSnapshotApply}
				close(entry.ch)
				delete(clientMap, reqId)
			}
		}
	}
}

func (kv *KVServer) turnOnSnpshtSwitch() bool {
	return atomic.CompareAndSwapInt32(&kv.snpshtSwitch, 0, 1)
}

func (kv *KVServer) turnOffSnpshtSwitch() bool {
	return atomic.CompareAndSwapInt32(&kv.snpshtSwitch, 1, 0)
}

func (kv *KVServer) isSnpshtSwitchOn() bool {
	return atomic.CompareAndSwapInt32(&kv.snpshtSwitch, 1, 1)
}

func (kv *KVServer) snapshotRoutine() {
	kv.assertf(kv.maxraftstate > 0, "Invalid maxraftstate: %v\n", kv.maxraftstate)
	kv.mu.Lock()
	defer kv.mu.Unlock()

	for !kv.killed() {
		if kv.time4Snapshot() && kv.isSnpshtSwitchOn() {
			DPrintf("kv:%v snapshoter waken up, raft state: %v\n", kv.me, kv.persister.RaftStateSize())
			kv.snapshot()
		} else {
			kv.snpshtcond.Wait()
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

	kv.applyCh = make(chan raft.ApplyMsg, SV_APPLY_CHAN_SIZE)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.persister = persister
	kv.reqMap = make(map[int64](map[uint64]RequestEntry))

	if persister.SnapshotSize() > 0 {
		kv.applySnapshot(persister.ReadSnapshot(), 0)
	} else {
		kv.table = make(map[string]string)
		kv.maxSrvdIds = make(map[int64]uint64)
		kv.lastAppIdx = 0
	}
	kv.snpshtcond = sync.NewCond(&kv.mu)
	kv.snpshtSwitch = 0

	go kv.applyRoutine()
	if maxraftstate > 0 {
		go kv.snapshotRoutine()
	}
	return kv
}
