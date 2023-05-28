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
	SvRetry
	SvWrongGroup
	SvHigherConfigNum
	SvTryOlderConfig
)

type Op struct {
	ClientId  int64
	RequestId uint64
	Shard     int
	Opcode    uint8
	Opdata    []byte
}

type ApplyReply struct {
	key    string
	value  string
	err    ServerError
	opcode uint8
	smap   *map[string]string // used for storing shard data
}

type RequestEntry struct {
	retryCnt int
	ch       chan ApplyReply
}

type ShardState uint8

type ShardInfo struct {
	State ShardState
	Cnum  int
}

const (
	SHRD_INVALID ShardState = iota
	SHRD_VALID
	SHRD_MOVE_IN
	SHRD_MOVE_OUT
)

type MigrateArgs struct {
	TraceId uint64 // used to make sure the (args, reply) pair is an one-to-one match
	Shard   int
	Cnum    int
	Gid     int
	State   ShardState
}

type MigrateReply struct {
	TraceId   uint64
	Err       string
	ShardData map[string]string
}

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
	table      [shardctrler.NShards]map[string]string // the table stores all of the KV pairs
	maxSrvdIds map[int64]uint64                       // {clientId : maxServedId for the client}
	lastAppIdx int
	shardInfos [shardctrler.NShards]ShardInfo

	config      shardctrler.Config
	isMigrating bool

	// === Volatile States ===
	reqMap         map[int64](map[uint64]RequestEntry) // {clientId : {requestId : RequestEntry}}
	requestCounter uint64

	// client to the controler
	mck *shardctrler.Clerk
}

func (kv *ShardKV) getConfig(shard int) (num int, gid int) {
	kv.mu.Lock()
	num = kv.config.Num
	gid = kv.config.Shards[shard]
	kv.mu.Unlock()
	return
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

func (kv *ShardKV) delRequestChan(clientId int64, reqId uint64) {
	kv.mu.Lock()
	clientMap, ok := kv.reqMap[clientId]
	if ok {
		delete(clientMap, reqId)
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) checkConfig(cnum int, shard int) bool {
	myCnum, gid := kv.getConfig(shard)
	for cnum > myCnum {
		diff := SV_POLL_CFG_TIME_OUT - SV_WAIT_CFG_TIME_OUT_MIN + 1
		timeout := rand.Intn(diff) + SV_WAIT_CFG_TIME_OUT_MIN
		time.Sleep(time.Duration(timeout) * time.Millisecond)
		myCnum, gid = kv.getConfig(shard)
	}

	return cnum > 0 && cnum == myCnum && gid == kv.gid
}

func encodeGet(key string) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(key)
	return w.Bytes()
}

func encodePutAppend(key string, value string) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(key)
	e.Encode(value)
	return w.Bytes()
}

func encodeMigrate(cnum int, state ShardState) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(cnum)
	e.Encode(state)
	return w.Bytes()
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
	case SvRetry:
		greply.Err = ErrRetry
	case SvWrongGroup:
		greply.Err = ErrWrongGroup
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
		Shard: args.Shard, Opcode: OP_GET, Opdata: encodeGet(args.Key)})
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
	case SvRetry:
		preply.Err = ErrRetry
	case SvWrongGroup:
		preply.Err = ErrWrongGroup
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
			Shard: args.Shard, Opcode: args.Opcode, Opdata: encodePutAppend(args.Key, args.Value)})
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

func (kv *ShardKV) explainErr4MigrateReply(serr ServerError, mreply *MigrateReply) {
	switch serr {
	case SvHigherConfigNum:
		mreply.Err = ErrAbort
	case SvOK:
		mreply.Err = OK
	case SvChanTimeOut:
		mreply.Err = ErrTimeout
	case SvTryOlderConfig:
		// the shard data requested is not on the current server, try older config
		mreply.Err = ErrTryOlderConfig
	default:
		kv.assertf(false, "Got an illegal error code: %v when doing MigrateShard.\n", serr)
	}
}

func (kv *ShardKV) ModifyShard(args *MigrateArgs, reply *MigrateReply) {
	reply.TraceId = args.TraceId
	// we set client id to -gid, as real client ids could never be a negative number
	clntId := int64(-args.Gid)
	// fake a request id just for the sake of locating the right channel
	reqId := atomic.AddUint64(&kv.requestCounter, 1)
	index, term, isLeader := kv.rf.Start(Op{ClientId: clntId, RequestId: reqId,
		Shard: args.Shard, Opcode: OP_SHARD_MIGRATE,
		Opdata: encodeMigrate(args.Cnum, args.State)})

	kv.logf("Received ModifyShard args:%+v, (r:%v, c:%v), index:%v, term:%v\n",
		*args, reqId, clntId, index, term)

	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.logf("is not the leader ModifyShard, (gid:%v, tr:%v)\n", args.Gid, args.TraceId)
		return
	}

	ch := make(chan ApplyReply, 1)
	kv.setRequestChan(clntId, reqId, index, ch)

	chReply := kv.timedWait(clntId, reqId, ch, SV_CHAN_TIME_OUT)
	if chReply.err == SvChanTimeOut {
		// prevent channel leak
		kv.delRequestChan(clntId, reqId)
	}
	kv.explainErr4MigrateReply(chReply.err, reply)

	if reply.Err == OK && chReply.smap != nil && len(*chReply.smap) > 0 {
		reply.ShardData = *chReply.smap
	}
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
	log.Fatalf("[gid=%v][kv=%v][ASSERT TRAP]: "+str, kv.gid, kv.me)
}

// kv.mu should be acquired
func (kv *ShardKV) applyCheckState(shard int) (err ServerError) {
	state := kv.shardInfos[shard].State
	switch state {
	case SHRD_INVALID:
		fallthrough
	case SHRD_MOVE_OUT:
		err = SvWrongGroup
	case SHRD_MOVE_IN:
		err = SvRetry
	case SHRD_VALID:
		err = SvOK
	default:
		kv.assertf(false, "Unknown state:%v\n", state)
	}
	return
}

func (kv *ShardKV) applyGet(cmd *Op, reply *ApplyReply) {
	reply.err = kv.applyCheckState(cmd.Shard)
	if reply.err != SvOK {
		return
	}

	r := bytes.NewBuffer(cmd.Opdata)
	d := labgob.NewDecoder(r)

	var key string
	err := d.Decode(&key)
	kv.assertf(err == nil, "Failed to decode `key` when applying Get, err: %v\n", err)

	var foundKey bool
	reply.key = key
	reply.value, foundKey = kv.table[cmd.Shard][key]
	if !foundKey {
		reply.err = SvUnknownKey
	}
}

func (kv *ShardKV) applyPutAppend(cmd *Op, reply *ApplyReply) {
	reply.err = kv.applyCheckState(cmd.Shard)
	if reply.err != SvOK {
		return
	}

	r := bytes.NewBuffer(cmd.Opdata)
	d := labgob.NewDecoder(r)

	var key string
	var value string
	err := d.Decode(&key)
	kv.assertf(err == nil, "Failed to decode `key` when applying P/A, err: %v\n", err)
	err = d.Decode(&value)
	kv.assertf(err == nil, "Failed to decode `value` when applying P/A, err: %v\n", err)

	reply.key = key
	_, foundKey := kv.table[cmd.Shard][key]

	if cmd.Opcode == OP_PUT {
		kv.table[cmd.Shard][key] = value
	} else {
		kv.assertf(cmd.Opcode == OP_APPEND, "Illegal opcode:%v in applyPutAppend\n", cmd.Opcode)
		if foundKey {
			kv.table[cmd.Shard][key] += value
		} else {
			kv.table[cmd.Shard][key] = value
		}
	}

	// update the max served id if it applies
	kv.maxSrvdIds[cmd.ClientId] = cmd.RequestId
}

func (kv *ShardKV) clearShardData(shard int) {
	for k := range kv.table[shard] {
		delete(kv.table[shard], k)
	}
}

func (kv *ShardKV) copyShardData(shard int, reply *ApplyReply) {
	m := make(map[string]string, len(kv.table[shard]))
	for k, v := range kv.table[shard] {
		m[k] = v
	}
	reply.smap = &m
}

func (kv *ShardKV) handleDupMigrate(state ShardState, shard int, reply *ApplyReply) {
	if state == SHRD_MOVE_OUT {
		kv.copyShardData(shard, reply)
	} else if state == SHRD_INVALID {
		kv.clearShardData(shard)
	}
}

/*
 *      +--------------------(migration done)---------------------+
 *      |                                                         |
 *      V                                                         |
 *   INVALID --(first config)--> VALID --(starts migration)--> MOVE_OUT
 *      |                          ^
 * (starts migration)              |
 *      |                          |
 *      |                          |
 *      v                          |
 *   MOVE_IN ---(migration done)---+
 */
func (kv *ShardKV) applyMigration(cmd *Op, reply *ApplyReply) {
	r := bytes.NewBuffer(cmd.Opdata)
	d := labgob.NewDecoder(r)

	var cnum int
	var state ShardState
	err := d.Decode(&cnum)
	kv.assertf(err == nil, "Failed to decode `cnum` when applying Migrate, err: %v\n", err)
	err = d.Decode(&state)
	kv.assertf(err == nil, "Failed to decode `state` when applying Migrate, err: %v\n", err)

	info := &kv.shardInfos[cmd.Shard]
	infoStr := fmt.Sprintf("info:%+v, shard:%v, cnum:%v, state:%v, clnt:%v",
		info, cmd.Shard, cnum, state, cmd.ClientId)
	if info.Cnum > cnum {
		kv.logf("Found higher cnum while applying migration, %v\n", infoStr)
		reply.err = SvHigherConfigNum
		return
	}

	if info.Cnum == cnum {
		if info.State == state {
			kv.logf("Found duplicated migrate request: %v\n", infoStr)
			// TODO:
			// 2. handle channel data races
			kv.handleDupMigrate(state, cmd.Shard, reply)
			return
		}
		kv.assertf(info.State == SHRD_MOVE_IN && state == SHRD_VALID ||
			info.State == SHRD_MOVE_OUT && state == SHRD_INVALID,
			"[applyMigration] equal assert, %v\n", infoStr)
	}

	setState := true
	setNum := true
	switch info.State {
	case SHRD_INVALID:
		if state == SHRD_VALID {
			kv.assertf(info.Cnum == 0 && cnum == 1, "[applyMigration] Invalid abort, %v\n", infoStr)
		} else if state == SHRD_MOVE_OUT {
			// The data is not there
			setState = false
			if info.Cnum > 0 {
				reply.err = SvTryOlderConfig
			}
			// if it's zero, we can mark the shard at the source side directly to VALID, reply.err
			// is already SvOK while getting into the function
		} else {
			kv.assertf(state == SHRD_MOVE_IN, "[applyMigration] INVALID abort, %v\n", infoStr)
		}
	case SHRD_VALID:
		if state == SHRD_MOVE_OUT {
			kv.copyShardData(cmd.Shard, reply)
		} else {
			// If we set it to VALID again, it must be our local request!
			kv.assertf(state == SHRD_VALID && kv.gid == -int(cmd.ClientId),
				"[applyMigration] VALID abort, %v\n", infoStr)
			setState = false
		}
	case SHRD_MOVE_IN:
		if state == SHRD_MOVE_IN {
			setState = false
			kv.logf("Found an already moved in shard, might be due to an aborted migration. %v\n",
				infoStr)
		} else {
			kv.assertf(state == SHRD_VALID, "[applyMigration] MOVE_IN abort, %v\n", infoStr)
		}
	case SHRD_MOVE_OUT:
		if state == SHRD_INVALID {
			setNum = false
			kv.clearShardData(cmd.Shard)
		} else {
			kv.assertf(state == SHRD_MOVE_IN, "[applyMigration] MOVE_OUT abort, %v\n", infoStr)
		}
	default:
		kv.assertf(false, "Invalid info.state, %v\n", infoStr)
	}

	if setState {
		info.State = state
	}
	if setNum {
		info.Cnum = cnum
	}
}

func (kv *ShardKV) applyCommand(cmd *Op, index int) {
	reply := ApplyReply{err: SvOK, opcode: cmd.Opcode, smap: nil}
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

	id, ok := kv.maxSrvdIds[cmd.ClientId]
	isdup := cmd.Opcode != OP_SHARD_MIGRATE && ok && id >= cmd.RequestId

	if !isdup {
		switch cmd.Opcode {
		case OP_GET:
			kv.applyGet(cmd, &reply)
		case OP_PUT:
			fallthrough
		case OP_APPEND:
			kv.applyPutAppend(cmd, &reply)
		case OP_SHARD_MIGRATE:
			// migrating operations are idempotent, so there will be no duplicate issues
			kv.applyMigration(cmd, &reply)
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
		kv.logf("found a duplicate request (r:%v, c:%v) in log idx:%v, ignore it!\n",
			cmd.RequestId, cmd.ClientId, index)
	} else {
		kv.logf("applied command: (r:%v, c:%v), shard:%v idx:%v, clientExists: %v, hasEntry: %v\n",
			cmd.RequestId, cmd.ClientId, cmd.Shard, index, clientExists, hasEntry)
	}

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
	e.Encode(kv.shardInfos)

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

	for i := 0; i < shardctrler.NShards; i++ {
		kv.table[i] = make(map[string]string)
	}
	err := d.Decode(&kv.table)
	kv.assertf(err == nil, "Failed to decode `table` when applying snapshot, err: %v\n", err)

	kv.maxSrvdIds = make(map[int64]uint64)
	err = d.Decode(&kv.maxSrvdIds)
	kv.assertf(err == nil, "Failed to decode `maxSrvdIds` when applying snapshot, err: %v\n", err)

	kv.lastAppIdx = 0
	err = d.Decode(&kv.lastAppIdx)
	kv.assertf(err == nil, "Failed to decode `lastAppIdx` when applying snapshot, err: %v\n", err)

	err = d.Decode(&kv.shardInfos)
	kv.assertf(err == nil, "Failed to decode `shardStates` when applying snapshot, err: %v\n", err)

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

// The caller is required to acquire kv.mu.
func (kv *ShardKV) copyLatestConfig(cfg *shardctrler.Config) {
	cfg.Num = kv.config.Num
	cfg.Shards = kv.config.Shards
	cfg.Groups = make(map[int][]string, len(kv.config.Groups))
	for g, s := range kv.config.Groups {
		cfg.Groups[g] = s
	}
}

func (kv *ShardKV) setShardData(shard int, cnum int, smap *map[string]string) {
	kv.logf("Setting shard:%v data, cnum:%v\n", shard, cnum)
	kv.mu.Lock()
	info := kv.shardInfos[shard]
	kv.assertf(info.State == SHRD_MOVE_IN && info.Cnum == cnum,
		"Invalid state in setShardData, info:%+v, shard:%v, cnum:%v\n", info, shard, cnum)
	kv.table[shard] = *smap
	kv.mu.Unlock()
}

func (kv *ShardKV) markLocalShard(shard int, cnum int, state ShardState) bool {
	args := MigrateArgs{TraceId: atomic.AddUint64(&kv.requestCounter, 1), Shard: shard, Cnum: cnum,
		Gid: kv.gid, State: state}
	for {
		reply := MigrateReply{}
		kv.logf("markShardState start, args:%+v\n", args)
		kv.ModifyShard(&args, &reply)
		kv.logf("markShardState end, reply:(tr:%v, err:%v)\n", reply.TraceId, reply.Err)

		kv.assertf(args.TraceId == reply.TraceId, "Args and reply unmatch, tr:(%v, %v), gid:%v\n",
			args.TraceId, reply.TraceId, kv.gid)
		switch reply.Err {
		case OK:
			return true
		case ErrAbort:
			fallthrough
		case ErrWrongLeader:
			return false
		case ErrReMigrate:
			kv.assertf(state == SHRD_VALID && cnum > 1,
				"ReMigrate can only happen for VALID state, but s:%v, c:%v\n", state, cnum)
			return kv.migrateShard(shard, cnum)
		case ErrTimeout:
			// retry
		default:
			kv.assertf(false, "gid:%v Invalid error code: %v in markShardState.\n",
				kv.gid, reply.Err)
		}
	}
}

// @param[in/out] oldCfg The old config we are going to use if it's valid
func (kv *ShardKV) markRemoteShard(shard int, cnum int, oldCfg *shardctrler.Config, state ShardState,
	bestEffort bool) bool {

	kv.assertf(cnum > 1, "Invaild cnum:%v\n", cnum)
	args := MigrateArgs{TraceId: atomic.AddUint64(&kv.requestCounter, 1), Shard: shard, Cnum: cnum,
		Gid: kv.gid, State: state}
	oldCnum := cnum - 1
	needQuery := oldCfg.Num == 0
	for oldCnum > 0 {
		kv.logf("[markRemoteShard] quering config number:%v\n", oldCnum)
		if needQuery {
			*oldCfg = kv.mck.Query(oldCnum)
		}
		destGid := oldCfg.Shards[shard]
		servers, ok := oldCfg.Groups[destGid]
		kv.assertf(ok, "[markRemoteShard] Invalid config:%+v\n", oldCfg)
		sid := 0
		retry := kv.gid != destGid
		for retry {
			srv := kv.make_end(servers[sid%len(servers)])
			reply := MigrateReply{}
			kv.logf("ModifyShard request start, (gid:%v, tr:%v), shard:%v, cnum:%v, oldcn:%v,"+
				" state:%v\n", kv.gid, args.TraceId, shard, cnum, oldCnum, state)
			ok := srv.Call("ShardKV.ModifyShard", &args, &reply)
			kv.logf("ModifyShard request end, (gid:%v, tr:%v), ok:%v, err:%v\n", kv.gid,
				args.TraceId, ok, reply.Err)
			kv.assertf(!ok || args.TraceId == reply.TraceId,
				"TraceId (args:%v, reply:%v) unmatch!\n", args.TraceId, reply.TraceId)

			if !ok {
				reply.Err = ErrTimeout
			}
			switch reply.Err {
			case OK:
				if state == SHRD_MOVE_OUT && len(reply.ShardData) > 0 {
					kv.setShardData(shard, cnum, &reply.ShardData)
				}
				return true
			case ErrWrongLeader:
				fallthrough
			case ErrTimeout:
				sid++
				if bestEffort && sid >= len(servers) {
					return false
				}
			case ErrAbort:
				return false
			case ErrTryOlderConfig:
				kv.assertf(!bestEffort,
					"[markRemoteShard] UNREACHABLE.shard:%v, cnum:%d, state:%v\n",
					shard, oldCnum, state)
				retry = false
			}
		}

		if oldCnum == 1 && destGid == kv.gid {
			// The shard belongs to us because we've found the very first config.
			kv.logf("Traversed all the configs, found shard:%v belongs to us, cnum:%d, state:%v\n",
				shard, oldCnum, state)
			return true
		}
		oldCnum--
	}

	// Theorectically, we will either find a higher term or where the shard data is. If we can't
	// find any way out up above, it's unacceptable and we have to complain it.
	kv.assertf(false, "markRemoteShard UNREACHABLE. s:%v, cn:%v\n", shard, cnum)
	return false
}

func (kv *ShardKV) migrateShard(shard int, cnum int) bool {
	abort := true
	newCnum := 0
	kv.mu.Lock()
	newCnum = kv.config.Num
	abort = !(cnum <= newCnum && kv.config.Shards[shard] == kv.gid)
	kv.mu.Unlock()

	if abort {
		kv.logf("Found higher config num:%v than cnum:%v, abort!\n", newCnum, cnum)
		return false
	}

	if cnum < newCnum {
		kv.logf("Found higher config num:%v than cnum:%v, but can continue.\n", newCnum, cnum)
		cnum = newCnum
	}

	// Step 1. Mark the local shard state as MOVE_IN
	if !kv.markLocalShard(shard, cnum, SHRD_MOVE_IN) {
		return false
	}
	// Step 2. Mark the destination's shard state as MOVE_OUT and pull the shard data from there
	oldCfg := shardctrler.Config{}
	if !kv.markRemoteShard(shard, cnum, &oldCfg, SHRD_MOVE_OUT, false) {
		ok := kv.markLocalShard(shard, cnum, SHRD_INVALID)
		kv.assertf(ok, "Failed to undo MOVE_IN for shard:%v, cnum:%v\n", shard, cnum)
		return false
	}
	// Step 3. Mark the shard as VALID
	ok := kv.markLocalShard(shard, cnum, SHRD_VALID)
	kv.assertf(ok, "Marking shard VALID for step 3 must succeed. (shard:%v, cnum:%v)\n",
		shard, cnum)
	if oldCfg.Shards[shard] == kv.gid {
		// If we happened to find the shard belongs to us, the step 4 will be skipped.
		return true
	}

	// Step 4. Mark the destination's shard state as INVALID and delete the data, but it executes
	// in a best effort way. Ideally, there should be a periodic GC routine on the other side to
	// ensure the stale data can be reclaimed finally.
	kv.markRemoteShard(shard, cnum, &oldCfg, SHRD_INVALID, true)
	return true
}

func (kv *ShardKV) getShardInfo(shard int) ShardInfo {
	kv.mu.Lock()
	info := kv.shardInfos[shard]
	kv.mu.Unlock()
	return info
}

func (kv *ShardKV) migrateRoutine(newCfg *shardctrler.Config) {
	kv.logf("Migration starts config:%+v\n", newCfg)
	carryOn := true
	for carryOn {
		kv.mu.Lock()
		kv.assertf(kv.isMigrating,
			"A migrating goroutine has been waken up without setting the migration flag.\n")
		if newCfg.Num != kv.config.Num {
			kv.assertf(newCfg.Num < kv.config.Num, "config num out of order! %v <? %v\n",
				newCfg.Num, kv.config.Num)
			kv.logf("Migrating goroutine whose config num:%v found a higher one:%v, will choose "+
				"the latter.\n", newCfg.Num, kv.config.Num)
			kv.copyLatestConfig(newCfg)
		}
		kv.mu.Unlock()

		condFlag := true
		for i := 0; i < shardctrler.NShards && condFlag; i++ {
			info := kv.getShardInfo(i)
			if info.Cnum > newCfg.Num || newCfg.Shards[i] != kv.gid {
				continue
			}

			kv.assertf(newCfg.Num != info.Cnum || info.State == SHRD_VALID,
				"Shard:%v info:%+v inconsistent, newCfg:%+v\n", i, info, newCfg)
			if newCfg.Num == 1 || info.State == SHRD_VALID {
				condFlag = kv.markLocalShard(i, newCfg.Num, SHRD_VALID)
			} else {
				condFlag = kv.migrateShard(i, newCfg.Num)
			}
		}

		kv.mu.Lock()
		if newCfg.Num < kv.config.Num {
			kv.logf("Migrating goroutine whose config num:%v found a higher one:%v, will do"+
				"another round of migration. flag:%v\n", newCfg.Num, kv.config.Num, condFlag)
			kv.copyLatestConfig(newCfg)
			_, carryOn = kv.rf.GetState()
			kv.isMigrating = carryOn
		} else {
			kv.isMigrating = false
			carryOn = false
		}
		kv.mu.Unlock()
	}

	kv.logf("Migration done for config:%v\n", newCfg)
}

func (kv *ShardKV) pollConfigRoutine() {
	for {
		cfg := kv.mck.Query(-1)
		newCfg := shardctrler.Config{Groups: make(map[int][]string, len(cfg.Groups))}
		needMigrate := false

		kv.mu.Lock()
		updated := cfg.Num > kv.config.Num
		if updated {
			kv.config = cfg
		}

		if !kv.isMigrating && updated {
			kv.isMigrating = true
			needMigrate = true
		}
		kv.copyLatestConfig(&newCfg)
		kv.mu.Unlock()

		_, isLeader := kv.rf.GetState()
		if isLeader && updated {
			kv.logf("updates config:%+v\n", newCfg)
		}
		if needMigrate && isLeader {
			go kv.migrateRoutine(&newCfg)
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
	labgob.Register(ShardInfo{})
	labgob.Register(map[string]string{})
	labgob.Register([shardctrler.NShards]ShardInfo{})
	labgob.Register([shardctrler.NShards]map[string]string{})

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
	kv.requestCounter = 0

	if persister.SnapshotSize() > 0 {
		kv.applySnapshot(persister.ReadSnapshot(), 0)
	} else {
		for i := 0; i < shardctrler.NShards; i++ {
			kv.table[i] = make(map[string]string)
		}
		kv.maxSrvdIds = make(map[int64]uint64)
		kv.lastAppIdx = 0
		for i := 0; i < shardctrler.NShards; i++ {
			kv.shardInfos[i].State = SHRD_INVALID
			kv.shardInfos[i].Cnum = 0
		}
	}
	kv.snpshtcond = sync.NewCond(&kv.mu)
	kv.snpshtSwitch = 0

	go kv.applyRoutine()
	if maxraftstate > 0 {
		go kv.snapshotRoutine()
	}

	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.isMigrating = false
	go kv.pollConfigRoutine()

	return kv
}
