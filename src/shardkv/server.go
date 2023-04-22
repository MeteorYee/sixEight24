package shardkv

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		str := fmt.Sprintf(format, a...)
		fmt.Printf("[%v]: "+str, time.Now().Format("01-02-2006 15:04:05.000"))
	}
	return
}

func (kv *ShardKV) logf(format string, a ...interface{}) {
	if !Debug {
		return
	}
	str := fmt.Sprintf(format, a...)
	fmt.Printf("[%v][gid=%v][kv=%v]: "+str, time.Now().Format("01-02-2006 15:04:05.000"),
		kv.gid, kv.me)
}

const SV_CHAN_TIME_OUT = 2      // 2 seconds
const SV_SNAPSHOT_FACTOR = 1.25 // when it's SV_SNAPSHOT_FACTOR * maxraftstate, we do snapshot
const SV_APPLY_CHAN_SIZE = 32   // the apply channel buffer size
const SV_RESTART_THRESHOLD = 1
const SV_POLL_CFG_TIME_OUT = 100    // millis
const SV_WAIT_CFG_TIME_OUT_MIN = 10 // millis

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

type ShardState uint8

const (
	INVALID ShardState = iota
	VALID
	MOVE_IN
	MOVE_OUT
)

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	dead         int32 // set by Kill()
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	snpshtSwitch int32      // used to prevent redundant notifications for snapshot
	snpshtcond   *sync.Cond // snapshot conditinal varialbe
	persister    *raft.Persister

	// === Persistent States ===
	table       map[string]string // the table stores all of the KV pairs
	maxSrvdIds  map[int64]uint64  // {clientId : maxServedId for the client}
	lastAppIdx  int
	shardStates [shardctrler.NShards]ShardState

	// === Volatile States ===
	reqMap map[int64](map[uint64]RequestEntry) // {clientId : {requestId : RequestEntry}}

	// client to the controler
	mck *shardctrler.Clerk

	config shardctrler.Config
	cfgmu  sync.Mutex
}

func (kv *ShardKV) getConfig() shardctrler.Config {
	kv.cfgmu.Lock()
	cfg := kv.config
	kv.cfgmu.Unlock()
	return cfg
}

func (kv *ShardKV) timedWait(clntId int64, reqId uint64, ch <-chan ApplyReply, sec int) ApplyReply {
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
		kv.logf("(r:%v, c:%v), times out while waiting for applier!\n", reqId, clntId)
		return ApplyReply{err: SvChanTimeOut}
	}
}

// `kv.mu` must be acquired while entering into the function
func (kv *ShardKV) checkRequestStatus(clientId int64, reqId uint64) (bool, int) {
	clientMap, ok := kv.reqMap[clientId]
	isProcessing := false
	var entry RequestEntry
	if ok {
		entry, isProcessing = clientMap[reqId]
		if isProcessing {
			entry.retryCnt++
			clientMap[reqId] = entry
		}
	}
	return isProcessing, entry.retryCnt
}

// `kv.mu` must be acquired while entering into the function
func (kv *ShardKV) hasRequestServed(clientId int64, reqId uint64) bool {
	maxSvId, ok := kv.maxSrvdIds[clientId]
	return ok && maxSvId >= reqId
}

// Return values:
// The 1st is the channel required to wait for the applier's reply;
// The 2nd is whether the request has been served, where the first return value will be nil if true
func (kv *ShardKV) setOrGetReqChan(clientId int64, reqId uint64, isProcessing bool, restart bool) (
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
		kv.logf("receives a duplicate (r:%v, c:%v).\n", reqId, clientId)
		appch = entry.ch
		if restart {
			entry.retryCnt = 0
		}
	} else {
		appch = make(chan ApplyReply, 1)
		clientMap[reqId] = RequestEntry{retryCnt: 0, ch: appch}
	}

	return appch, false
}

func (kv *ShardKV) setRequestChan(clientId int64, reqId uint64, index int, ch chan ApplyReply) {
	kv.assertf(index > 0, "Invalid index:%v\n", index)
	kv.mu.Lock()
	defer kv.mu.Unlock()

	clientMap, ok := kv.reqMap[clientId]
	if !ok {
		clientMap = make(map[uint64]RequestEntry)
		kv.reqMap[clientId] = clientMap
	}
	clientMap[reqId] = RequestEntry{retryCnt: 0, ch: ch}
}

func (kv *ShardKV) checkConfig(cnum int, shard int) bool {
	cfg := kv.getConfig()
	for cnum > cfg.Num {
		diff := SV_POLL_CFG_TIME_OUT - SV_WAIT_CFG_TIME_OUT_MIN + 1
		timeout := rand.Intn(diff) + SV_WAIT_CFG_TIME_OUT_MIN
		time.Sleep(time.Duration(timeout) * time.Millisecond)
		cfg = kv.getConfig()
	}

	if cnum < cfg.Num {
		return false
	}

	// TODO: shard migration
	kv.assertf(cfg.Shards[shard] == kv.gid, "Invalid config:%+v, kv.gid=%v\n", cfg, kv.gid)
	return true
}

func (kv *ShardKV) explainErr4GetReply(serr ServerError, greply *GetReply) {
	switch serr {
	case SvOK:
		greply.Err = OK
	case SvSnapshotApply:
		// When we get the reply from the snapshoter, it means we're not the leader any more.
		greply.Err = ErrWrongLeader
	case SvChanTimeOut:
		greply.Err = ErrTimeout
	case SvUnknownKey:
		greply.Err = ErrNoKey
	default:
		// Get operation shall not get a SvChanClosed error because it doesn't care about
		// duplicated requests.
		kv.assertf(false, "Got an invalid error code: %v when doing Get.\n", serr)
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	reply.RequestId = args.RequestId
	if !kv.checkConfig(args.CfgNum, args.Shard) {
		reply.Err = ErrWrongGroup
		kv.logf("wrong group in Get, (r:%v, c:%v), cnum:%v, shard:%v\n",
			args.RequestId, args.ClientId, args.CfgNum, args.Shard)
		return
	}

	kv.mu.Lock()
	isProcessing, _ := kv.checkRequestStatus(args.ClientId, args.RequestId)
	hasServed := kv.hasRequestServed(args.ClientId, args.RequestId)
	kv.mu.Unlock()

	kv.assertf(!isProcessing && !hasServed,
		"There shall never be duplicated Get requests. args: %+v, new: %v, served: %v\n",
		*args, isProcessing, hasServed)

	// Because Get operation has got no side effects, we don't care if the request id is duplicated
	// and just go ahead to do the work.
	index, term, isLeader := kv.rf.Start(Op{ClientId: args.ClientId, RequestId: args.RequestId,
		Opcode: OP_GET, Key: args.Key})
	kv.logf("received Get (r:%v, c:%v), index:%v, term:%v\n", args.RequestId, args.ClientId,
		index, term)

	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.logf("is not the leader in Get, (r:%v, c:%v)\n", args.RequestId,
			args.ClientId)
		return
	}

	// make a buffered channel of size 1 to prevent from blocking the applier
	ch := make(chan ApplyReply, 1)
	kv.setRequestChan(args.ClientId, args.RequestId, index, ch)

	chReply := kv.timedWait(args.ClientId, args.RequestId, ch, SV_CHAN_TIME_OUT)

	kv.explainErr4GetReply(chReply.err, reply)
	if reply.Err != OK {
		return
	}

	_, isLeader = kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.logf("is not the leader in Get, (r:%v, c:%v)\n", args.RequestId, args.ClientId)
		return
	}

	reply.Value = chReply.value
	kv.assertf(chReply.key == args.Key && chReply.opcode == OP_GET,
		"mistaken reply in Get! (r:%v, c:%v)\n", args.RequestId, args.ClientId)
}

func (kv *ShardKV) explainErr4PtAppReply(serr ServerError, preply *PutAppendReply) {
	switch serr {
	case SvOK:
		fallthrough
	case SvSnapshotApply:
		fallthrough
	case SvChanClosed:
		// a closed channel means the request has been served
		preply.Err = OK
	case SvChanTimeOut:
		preply.Err = ErrTimeout
	case SvUnknownKey:
		preply.Err = ErrNoKey
	default:
		kv.assertf(false, "Got an invalid error code: %v when doing P/A.\n", serr)
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	reply.RequestId = args.RequestId
	if !kv.checkConfig(args.CfgNum, args.Shard) {
		reply.Err = ErrWrongGroup
		kv.logf("wrong group in P/A, (r:%v, c:%v), cnum:%v, shard:%v\n",
			args.RequestId, args.ClientId, args.CfgNum, args.Shard)
		return
	}

	kv.mu.Lock()
	isProcessing, retryCnt := kv.checkRequestStatus(args.ClientId, args.RequestId)
	hasServed := kv.hasRequestServed(args.ClientId, args.RequestId)
	kv.mu.Unlock()

	// The corresponding log may have been erased, as with the figure 8 in Raft paper. Thus,
	// we sometimes need to issus another Start() call. One might argue that it will risk
	// duplicating requests. However, the applier will take care of it anyway.
	restart := retryCnt >= SV_RESTART_THRESHOLD

	//  isProcessing | hasServed | Possible?
	// --------------+-----------+----------
	//        T      |     T     |     F     served ones got no entries in the reqMap
	// --------------+-----------+----------
	//        T      |     F     |     T     a duplicate one and not served
	// --------------+-----------+----------
	//        F      |     T     |     T     processed ones
	// --------------+-----------+----------
	//        F      |     F     |     T     a new coming one
	//
	// A~B + ~AB + ~A~B = A~B + ~A
	kv.assertf((isProcessing && !hasServed) || !isProcessing,
		"P/A handler, args: %+v, new: %v, served: %v\n", *args, isProcessing, hasServed)
	if hasServed {
		reply.Err = OK
		kv.logf("received a duplicated request args: %+v which has already been served.\n", *args)
		return
	}

	index := -1
	var term int
	var isLeader bool
	if isProcessing && !restart {
		term, isLeader = kv.rf.GetState()
	} else {
		index, term, isLeader = kv.rf.Start(Op{ClientId: args.ClientId, RequestId: args.RequestId,
			Opcode: args.Opcode, Key: args.Key, Value: args.Value})
	}
	kv.logf("received P/A (r:%v, c:%v), isProcessing:%v, hasServed:%v, restart:%v, retry:%v"+
		", index:%v, term:%v\n", args.RequestId, args.ClientId, isProcessing, hasServed,
		restart, retryCnt, index, term)

	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.logf("is not the leader in P/A, (r:%v, c:%v)\n", args.RequestId, args.ClientId)
		return
	}

	ch, hasServed := kv.setOrGetReqChan(args.ClientId, args.RequestId, isProcessing, restart)
	if hasServed { // we need to check this yet again
		reply.Err = OK
		kv.logf("received a duplicated request (args: %+v) which has already been served.\n", *args)
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

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	kv.snpshtcond.Signal()
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) assertf(assertion bool, format string, a ...interface{}) {
	if assertion {
		return
	}
	str := fmt.Sprintf(format, a...)
	log.Fatalf("[server=%v][ASSERT TRAP]: "+str, kv.me)
}

func (kv *ShardKV) applyCommand(cmd *Op, index int) {
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
		kv.logf("found a duplicate request (r:%v, c:%v) in log idx:%v, ignore it!\n",
			cmd.RequestId, cmd.ClientId, index)
	}
	// else {
	// 	kv.logf("applied command:%+v idx:%v, clientExists: %v, hasEntry: %v\n", *cmd,
	// 		index, clientExists, hasEntry)
	// }

	if kv.time4Snapshot() && kv.turnOnSnpshtSwitch() {
		kv.snpshtcond.Signal()
		kv.logf("signaled the snapshoter (r:%v, c:%v) at log idx:%v. raft size:%v\n",
			cmd.RequestId, cmd.ClientId, index, kv.persister.RaftStateSize())
	}
}

func (kv *ShardKV) applyRoutine() {
	for msg := range kv.applyCh {
		if kv.killed() {
			break
		}

		if msg.SnapshotValid {
			kv.logf("is applying snapshot, idx:%v, term:%v\n", msg.SnapshotIndex, msg.SnapshotTerm)
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

func (kv *ShardKV) time4Snapshot() bool {
	return kv.maxraftstate > 0 && float64(kv.persister.RaftStateSize()) >
		float64(kv.maxraftstate)*SV_SNAPSHOT_FACTOR
}

func (kv *ShardKV) snapshot() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(kv.table)
	e.Encode(kv.maxSrvdIds)
	e.Encode(kv.lastAppIdx)

	kv.rf.Snapshot(kv.lastAppIdx, w.Bytes())
	kv.logf("saving snapshot, lastAppIdx:%v, raft size:%v\n", kv.lastAppIdx,
		kv.persister.RaftStateSize())

	isoff := kv.turnOffSnpshtSwitch()
	kv.assertf(isoff, "Some sneaky guy has switched off `snpshtSwitch`!\n")
}

func (kv *ShardKV) applySnapshot(data []byte, sindex int) {
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

func (kv *ShardKV) turnOnSnpshtSwitch() bool {
	return atomic.CompareAndSwapInt32(&kv.snpshtSwitch, 0, 1)
}

func (kv *ShardKV) turnOffSnpshtSwitch() bool {
	return atomic.CompareAndSwapInt32(&kv.snpshtSwitch, 1, 0)
}

func (kv *ShardKV) isSnpshtSwitchOn() bool {
	return atomic.CompareAndSwapInt32(&kv.snpshtSwitch, 1, 1)
}

func (kv *ShardKV) snapshotRoutine() {
	kv.assertf(kv.maxraftstate > 0, "Invalid maxraftstate: %v\n", kv.maxraftstate)
	kv.mu.Lock()
	defer kv.mu.Unlock()

	for !kv.killed() {
		if kv.time4Snapshot() && kv.isSnpshtSwitchOn() {
			kv.logf("snapshoter waken up, raft state: %v\n", kv.persister.RaftStateSize())
			kv.snapshot()
		} else {
			kv.snpshtcond.Wait()
		}
	}
}

func (kv *ShardKV) pollConfigRoutine() {
	for {
		cfg := kv.mck.Query(-1)

		kv.cfgmu.Lock()
		updated := cfg.Num > kv.config.Num
		if updated {
			kv.config = cfg
		}
		kv.cfgmu.Unlock()

		if updated {
			kv.logf("updates config:%+v\n", cfg)
		}
		time.Sleep(SV_POLL_CFG_TIME_OUT * time.Millisecond)
	}
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
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

	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	go kv.pollConfigRoutine()

	return kv
}
