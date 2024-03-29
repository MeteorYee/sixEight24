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

const Debug = false

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

const SV_CHAN_TIME_OUT = 1          // 1 second
const SV_SNAPSHOT_FACTOR = 1.25     // when it's SV_SNAPSHOT_FACTOR * maxraftstate, we do snapshot
const SV_APPLY_CHAN_SIZE = 32       // the apply channel buffer size
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
	SvWaitMigrate
	SvShardValid
	SvShardUninit
)

type Op struct {
	ClientId  int64
	RequestId uint64
	Shard     int
	Cnum      int
	ShrdState ShardState
	Opcode    uint8
	Opdata    []byte
}

type ApplyReply struct {
	key    string
	value  string
	err    ServerError
	opcode uint8
	state  ShardState         // the state of the current shard
	cnum   int                // the config number of the current shard
	smap   *map[string]string // used for storing shard data
	dmap   *map[int64]uint64  // used for deduplication
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
	SHRD_QUERY // used for shard state query
)

type MigrateArgs struct {
	TraceId uint64 // used to make sure the (args, reply) pair is an one-to-one match
	Shard   int
	Cnum    int
	Gid     int
	State   ShardState
	Data    []byte
}

type MigrateReply struct {
	TraceId   uint64
	Err       string
	State     ShardState // the state of the current shard
	Cnum      int        // the config number of the current shard
	ShardData map[string]string
	DedupMap  map[int64]uint64
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
	mck    *shardctrler.Clerk
	ctrlmu sync.Mutex
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

func (kv *ShardKV) setRequestChan(clientId int64, reqId uint64, ch chan ApplyReply) {
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

	ch := make(chan ApplyReply, 1)
	kv.setRequestChan(args.ClientId, args.RequestId, ch)
	// Because Get operation has got no side effects, we don't care if the request id is duplicated
	// and just go ahead to do the work.
	index, term, isLeader := kv.rf.Start(Op{ClientId: args.ClientId, RequestId: args.RequestId,
		Shard: args.Shard, Cnum: args.CfgNum, Opcode: OP_GET, Opdata: encodeGet(args.Key)})
	kv.logf("received Get (r:%v, c:%v), cnum:%v, index:%v, term:%v\n", args.RequestId,
		args.ClientId, args.CfgNum, index, term)

	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.logf("is not the leader in Get, (r:%v, c:%v)\n", args.RequestId,
			args.ClientId)
		kv.delRequestChan(args.ClientId, args.RequestId)
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

	ch := make(chan ApplyReply, 1)
	kv.setRequestChan(args.ClientId, args.RequestId, ch)

	index, term, isLeader := kv.rf.Start(Op{ClientId: args.ClientId, RequestId: args.RequestId,
		Shard: args.Shard, Cnum: args.CfgNum, Opcode: args.Opcode,
		Opdata: encodePutAppend(args.Key, args.Value)})
	kv.logf("received P/A (r:%v, c:%v), cnum:%v, index:%v, term:%v\n", args.RequestId,
		args.ClientId, args.CfgNum, index, term)

	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.logf("is not the leader in P/A, (r:%v, c:%v)\n", args.RequestId, args.ClientId)
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
	case SvWaitMigrate:
		// the shard data is under migration on the current server, need to wait
		mreply.Err = ErrWaitMigrate
	case SvShardValid:
		mreply.Err = ErrSkipMigrate
	case SvShardUninit:
		mreply.Err = ErrShardUninit
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
	ch := make(chan ApplyReply, 1)
	kv.setRequestChan(clntId, reqId, ch)

	index, term, isLeader := kv.rf.Start(Op{ClientId: clntId, RequestId: reqId,
		Shard: args.Shard, Cnum: args.Cnum, ShrdState: args.State, Opcode: OP_SHARD_MIGRATE,
		Opdata: args.Data})
	kv.logf("Received ModifyShard (gid:%v, tr:%v), (r:%v, c:%v), shard:%v, state:%v, cnum:%v, "+
		"index:%v, term:%v, dataLen:%v\n", args.Gid, args.TraceId, reqId, clntId, args.Shard,
		args.State, args.Cnum, index, term, len(args.Data))

	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.logf("is not the leader ModifyShard, (gid:%v, tr:%v)\n", args.Gid, args.TraceId)
		kv.delRequestChan(clntId, reqId)
		return
	}

	chReply := kv.timedWait(clntId, reqId, ch, SV_CHAN_TIME_OUT)
	if chReply.err == SvChanTimeOut {
		// prevent channel leak
		kv.delRequestChan(clntId, reqId)
	}
	kv.explainErr4MigrateReply(chReply.err, reply)
	reply.Cnum = chReply.cnum
	reply.State = chReply.state

	if reply.Err == OK && chReply.smap != nil && len(*chReply.smap) > 0 {
		kv.assertf(chReply.dmap != nil, "NULL dedup map,(gid:%v, tr:%v)\n", args.Gid, args.TraceId)
		if len(*chReply.dmap) > 0 {
			reply.DedupMap = *chReply.dmap
		}
		reply.ShardData = *chReply.smap
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.logf("Killing the server...\n")
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	kv.snpshtcond.Signal()
	kv.mck.Kill()

	// wait migrating routine ends
	kv.mu.Lock()
	for kv.isMigrating {
		kv.mu.Unlock()
		time.Sleep(32 * time.Millisecond)
		kv.mu.Lock()
	}
	kv.mu.Unlock()
	kv.logf("Server killed.\n")
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
func (kv *ShardKV) applyCheckState(shard int, cnum int) (err ServerError) {
	info := kv.shardInfos[shard]
	if info.Cnum > cnum {
		err = SvWrongGroup // wait for migration
	} else { // we allow the clients to have higher config number
		switch info.State {
		case SHRD_MOVE_IN:
			err = SvRetry
		case SHRD_VALID:
			err = SvOK
		case SHRD_INVALID:
			fallthrough
		case SHRD_MOVE_OUT:
			err = SvWrongGroup
		default:
			kv.assertf(false, "Illegal state:%v\n", info.State)
		}
	}
	return
}

func (kv *ShardKV) applyGet(cmd *Op, reply *ApplyReply) {
	id, ok := kv.maxSrvdIds[cmd.ClientId]
	isdup := ok && id >= cmd.RequestId
	kv.assertf(!isdup, "Found duplicates in Get. (r:%v, c:%v)\n", cmd.RequestId, cmd.ClientId)

	reply.err = kv.applyCheckState(cmd.Shard, cmd.Cnum)
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

func (kv *ShardKV) applyPutAppend(cmd *Op, reply *ApplyReply, isdup *bool) {
	id, ok := kv.maxSrvdIds[cmd.ClientId]
	*isdup = ok && id >= cmd.RequestId

	r := bytes.NewBuffer(cmd.Opdata)
	d := labgob.NewDecoder(r)

	var key string
	var value string
	err := d.Decode(&key)
	kv.assertf(err == nil, "Failed to decode `key` when applying P/A, err: %v\n", err)
	err = d.Decode(&value)
	kv.assertf(err == nil, "Failed to decode `value` when applying P/A, err: %v\n", err)

	reply.key = key
	reply.err = kv.applyCheckState(cmd.Shard, cmd.Cnum)
	if reply.err != SvOK {
		return
	}
	_, foundKey := kv.table[cmd.Shard][key]
	if *isdup {
		kv.assertf(foundKey, "Duplicated request finds no key matched! (r:%v, c:%v), id:%v",
			cmd.RequestId, cmd.ClientId, id)
		return
	}

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
	sm := make(map[string]string, len(kv.table[shard]))
	for k, v := range kv.table[shard] {
		sm[k] = v
	}
	reply.smap = &sm

	dm := make(map[int64]uint64, len(kv.maxSrvdIds))
	for k, v := range kv.maxSrvdIds {
		dm[k] = v
	}
	reply.dmap = &dm
}

func (kv *ShardKV) handleDupMigrate(state ShardState, shard int, reply *ApplyReply) {
	if state == SHRD_MOVE_OUT {
		kv.copyShardData(shard, reply)
	} else if state == SHRD_INVALID {
		kv.clearShardData(shard)
	}
}

func (kv *ShardKV) setShardData(shard int, cnum int, data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	sm := make(map[string]string)
	err := d.Decode(&sm)
	kv.assertf(err == nil, "Failed to decode `shard data` when applying Migrate, err: %v\n", err)
	dm := make(map[int64]uint64)
	err = d.Decode(&dm)
	kv.assertf(err == nil, "Failed to decode `dedup map` when applying Migrate, err: %v\n", err)

	kv.table[shard] = sm
	kv.assertf(len(dm) > 0, "Empty dedup map, shard:%v, cnum:%v\n", shard, cnum)
	for k, v := range dm {
		lv, ok := kv.maxSrvdIds[k]
		if !ok || lv < v {
			kv.maxSrvdIds[k] = v
		}
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
	cnum := cmd.Cnum
	state := cmd.ShrdState

	info := &kv.shardInfos[cmd.Shard]
	reply.cnum = info.Cnum
	reply.state = info.State
	if state == SHRD_QUERY { // query op, no side effects
		return
	}

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
			kv.handleDupMigrate(state, cmd.Shard, reply)
			return
		}
		kv.assertf(info.State == SHRD_MOVE_IN && state == SHRD_VALID || // data moved in
			info.State == SHRD_MOVE_OUT && state == SHRD_INVALID || // data moved out
			info.State == SHRD_MOVE_IN && state == SHRD_INVALID || // undo a rejected migration
			info.State == SHRD_INVALID && state == SHRD_MOVE_OUT, // the shard is not that latest
			"[applyMigration] equal assert, %v\n", infoStr)
	}

	setState := true
	setNum := true
	switch info.State {
	case SHRD_INVALID:
		if state == SHRD_VALID {
			kv.assertf(info.Cnum == 0 && cnum == 1,
				"[applyMigration] INVALID cnum abort, %v\n", infoStr)
		} else if state == SHRD_MOVE_OUT {
			// The data is not there
			setState = false
			if info.Cnum == 0 {
				reply.err = SvShardUninit
			} else {
				reply.err = SvTryOlderConfig
			}
		} else {
			kv.assertf(state == SHRD_MOVE_IN, "[applyMigration] INVALID state abort, %v\n", infoStr)
		}
	case SHRD_VALID:
		if state == SHRD_MOVE_OUT {
			kv.copyShardData(cmd.Shard, reply)
		} else if state == SHRD_MOVE_IN {
			// the shard happens to be on our node
			setState = false
			reply.err = SvShardValid
		} else {
			// If we set it to VALID again, it must be our local request!
			kv.assertf(state == SHRD_VALID && kv.gid == -int(cmd.ClientId),
				"[applyMigration] VALID abort, %v\n", infoStr)
			setState = false
		}
	case SHRD_MOVE_IN:
		if state == SHRD_MOVE_OUT {
			kv.logf("Found an underway migration, we have to wait. %v\n", infoStr)
			setState = false
			setNum = false
			reply.err = SvWaitMigrate
		} else {
			kv.assertf(info.Cnum == cnum, "[applyMigration] MOVE_IN equal assert, %v\n", infoStr)
			kv.assertf(state == SHRD_VALID || state == SHRD_INVALID,
				"[applyMigration] MOVE_IN abort, %v\n", infoStr)
		}
		// There couldn't be a MOVE_IN request with a higher config number, because we migrate each
		// single shard sequentially. What's more, if a MOVE_IN shard has been aborted, the new
		// elected leader has to pick it up and finish it. However, if we failed to do the pickup,
		// we will first modify the shard state as INVALID.

		if state == SHRD_VALID && len(cmd.Opdata) > 0 {
			kv.setShardData(cmd.Shard, cnum, cmd.Opdata)
		}
	case SHRD_MOVE_OUT:
		if state == SHRD_INVALID {
			kv.assertf(info.Cnum == cnum, "[applyMigration] MOVE_OUT->INVALID abort, %v\n", infoStr)
			kv.clearShardData(cmd.Shard)
		} else if state == SHRD_MOVE_OUT || state == SHRD_MOVE_IN {
			// If we get in here, it means there is still a migration underway, whose config
			// number is lower than the current request but is greater than info.Cnum. The reason
			// we see it is because the shard data should've been here as per a configuration
			// between (info.Cnum, request.Cnum), but it just happened to be lagged behind by the
			// info.Cnum's configuration. In conclusion, we wait the migration to finish.
			kv.logf("Found a shard moving/moved out, we wait and retry, %v\n", infoStr)
			setState = false
			setNum = false
			reply.err = SvWaitMigrate
		} else {
			kv.assertf(false, "[applyMigration] MOVE_OUT abort, %v\n", infoStr)
		}
	default:
		kv.assertf(false, "Invalid info.state, %v\n", infoStr)
	}

	stateStr := fmt.Sprintf("shard:%v applyMigration from (s:%v, n:%v)", cmd.Shard, info.State,
		info.Cnum)
	if setState {
		info.State = state
	}
	if setNum {
		info.Cnum = cnum
	}
	kv.logf("%v to (s:%v, n:%v), dataL:%v\n", stateStr, info.State, info.Cnum, len(cmd.Opdata))
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

	isdup := false
	switch cmd.Opcode {
	case OP_GET:
		kv.applyGet(cmd, &reply)
	case OP_PUT:
		fallthrough
	case OP_APPEND:
		kv.applyPutAppend(cmd, &reply, &isdup)
	case OP_SHARD_MIGRATE:
		// migrating operations are idempotent, so there will be no duplicate issues
		kv.applyMigration(cmd, &reply)
	default:
		kv.assertf(false, "The applier has got an unknown op: %v\n", cmd.Opcode)
	}

	kv.assertf(index == kv.lastAppIdx+1, "raft index out of order! prev:%v, cur:%v\n",
		kv.lastAppIdx, index)
	kv.lastAppIdx = index

	kv.mu.Unlock()

	if hasEntry {
		entry.ch <- reply
		close(entry.ch)
	}

	// if !isdup {
	// 	kv.logf("applied command: (r:%v, c:%v), shard:%v idx:%v, clientExists: %v, hasEntry: %v\n",
	// 		cmd.RequestId, cmd.ClientId, cmd.Shard, index, clientExists, hasEntry)
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

	kv.shardInfos = [shardctrler.NShards]ShardInfo{}
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

func (kv *ShardKV) encodeShardData(shard int, cnum int, reply *MigrateReply) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(reply.ShardData)
	kv.assertf(len(reply.DedupMap) > 0, "Empty dedup map, shard:%v, cnum:%v\n", shard, cnum)
	e.Encode(reply.DedupMap)
	d := w.Bytes()
	kv.logf("Encoded shard:%v dataL:%v, cnum:%v\n", shard, len(d), cnum)
	return d
}

func (kv *ShardKV) markLocalShard(shard int, cnum int, state ShardState, isValid *bool,
	data *[]byte) bool {
	args := MigrateArgs{TraceId: atomic.AddUint64(&kv.requestCounter, 1), Shard: shard, Cnum: cnum,
		Gid: kv.gid, State: state}
	if data == nil {
		args.Data = make([]byte, 0)
	} else {
		args.Data = *data
	}

	for !kv.killed() {
		reply := MigrateReply{}
		kv.logf("markLocalShard start, (gid:%v, tr:%v), shard:%v, state:%v, cnum:%v, dataLen:%v\n",
			args.Gid, args.TraceId, args.Shard, args.State, args.Cnum, len(args.Data))
		kv.ModifyShard(&args, &reply)
		kv.logf("markLocalShard end, reply:(gid:%v, tr:%v, err:%v)\n",
			args.Gid, reply.TraceId, reply.Err)

		kv.assertf(args.TraceId == reply.TraceId, "Args and reply unmatch, tr:(%v, %v), gid:%v\n",
			args.TraceId, reply.TraceId, kv.gid)
		switch reply.Err {
		case OK:
			return true
		case ErrSkipMigrate:
			kv.assertf(state == SHRD_MOVE_IN, "Skip migration abort, args:%+v\n", args)
			if isValid != nil {
				*isValid = true
			}
			return true
		case ErrAbort:
			fallthrough
		case ErrWrongLeader:
			return false
		case ErrTimeout:
			// retry
		case ErrWaitMigrate:
			kv.assertf(reply.State == SHRD_MOVE_OUT, "(gid:%v, tr:%v) invalid wait migrate, "+
				"rstate:%v, rcnum:%v\n", args.Gid, args.TraceId, reply.State, reply.Cnum)
			time.Sleep(WAIT_MIGRATE_TIME_OUT * time.Millisecond)
		default:
			kv.assertf(false, "gid:%v Invalid error code: %v in markLocalShard.\n",
				kv.gid, reply.Err)
		}
	}
	return false
}

// @param[in/out] oldCfg The old config we are going to use if it's valid
func (kv *ShardKV) markRemoteShard(shard int, cnum int, oldCfg *shardctrler.Config, data *[]byte,
	state ShardState, bestEffort bool) bool {

	kv.assertf(cnum > 1, "Invaild cnum:%v\n", cnum)
	args := MigrateArgs{TraceId: atomic.AddUint64(&kv.requestCounter, 1), Shard: shard, Cnum: cnum,
		Gid: kv.gid, State: state}
	oldCnum := cnum - 1
	needQuery := oldCfg.Num == 0
	destGid := -1
	isShardUninit := false
	for oldCnum > 0 && !kv.killed() {
		kv.logf("[markRemoteShard] quering config number:%v\n", oldCnum)
		if needQuery {
			*oldCfg = shardctrler.Config{}
			*oldCfg = kv.queryConfig(oldCnum)
			if oldCfg.Num == -1 {
				// Invalid config, the client has been killed
				return false
			}
		}
		if destGid == oldCfg.Shards[shard] {
			oldCnum--
			continue
		}

		destGid = oldCfg.Shards[shard]
		servers, ok := oldCfg.Groups[destGid]
		kv.assertf(ok, "[markRemoteShard] Invalid config:%+v\n", oldCfg)
		sid := 0
		retry := kv.gid != destGid
		for retry && !kv.killed() {
			isShardUninit = false
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
					kv.assertf(data != nil, "Invalid data address, (gid:%v, tr:%v)\n", kv.gid,
						args.TraceId)
					*data = kv.encodeShardData(shard, cnum, &reply)
				}
				return true
			case ErrShardUninit:
				// we've found an uninitialized shard state, if old cnum > 1, we have to continue
				isShardUninit = true
				retry = false
			case ErrWrongLeader:
				sid++
				if sid%len(servers) == 0 {
					// wait for leader election
					time.Sleep(time.Duration(raft.ELECTION_TIME_OUT_LO/2) * time.Millisecond)
				}
			case ErrTimeout:
				sid++
				if bestEffort && sid >= len(servers) {
					return false
				}
				if sid%len(servers) == 0 {
					// timeout wait
					time.Sleep(16 * time.Millisecond)
				}
			case ErrAbort:
				return false
			case ErrTryOlderConfig:
				kv.assertf(!bestEffort,
					"[markRemoteShard] UNREACHABLE.shard:%v, cnum:%d, state:%v\n",
					shard, oldCnum, state)
				retry = false
			case ErrWaitMigrate:
				time.Sleep(WAIT_MIGRATE_TIME_OUT * time.Millisecond)
			}
		}

		oldCnum--
	}

	if destGid == kv.gid || isShardUninit {
		// The shard belongs to us because we've found the very first config.
		kv.logf("Traversed all the configs, found shard:%v belongs to us, cnum:%d, state:%v\n",
			shard, oldCnum, state)
		return true
	}
	// Theorectically, we will either find a way out up above or be killed. Otherwise, it's
	// unacceptable and we have to complain it.
	kv.assertf(kv.killed(), "markRemoteShard UNREACHABLE. s:%v, cn:%v\n", shard, cnum)
	return false
}

func (kv *ShardKV) migrateShard(shard int, cnum int) bool {
	if kv.killed() {
		return false
	}

	isValid := false
	// Step 1. Mark the local shard state as MOVE_IN
	if !kv.markLocalShard(shard, cnum, SHRD_MOVE_IN, &isValid, nil) {
		return false
	}
	if isValid {
		// the shard is already valid, no need to do any migrations
		return true
	}

	// Step 2. Mark the destination's shard state as MOVE_OUT and pull the shard data from there
	oldCfg := shardctrler.Config{}
	shardData := make([]byte, 0)
	if !kv.markRemoteShard(shard, cnum, &oldCfg, &shardData, SHRD_MOVE_OUT, false) {
		kv.logf("Marking remote shard MOVE_OUT failed, (shard:%v, cnum:%v)\n", shard, cnum)
		kv.markLocalShard(shard, cnum, SHRD_INVALID, nil, nil) // best effort, could fail
		return false
	}
	// Step 3. Mark the shard as VALID
	if !kv.markLocalShard(shard, cnum, SHRD_VALID, nil, &shardData) {
		kv.logf("Marking shard VALID for step 3 failed. (shard:%v, cnum:%v)\n", shard, cnum)
		return false
	}
	if oldCfg.Shards[shard] == kv.gid {
		// If we happened to find the shard belongs to us, the step 4 will be skipped.
		return true
	}

	// Step 4. Mark the destination's shard state as INVALID and delete the data, but it executes
	// in a best effort way. Ideally, there should be a periodic GC routine on the other side to
	// ensure the stale data can be reclaimed finally.
	kv.markRemoteShard(shard, cnum, &oldCfg, nil, SHRD_INVALID, true)
	return true
}

func (kv *ShardKV) getShardInfo(shard int) ShardInfo {
	kv.mu.Lock()
	info := kv.shardInfos[shard]
	kv.mu.Unlock()
	return info
}

func (kv *ShardKV) fixMoveOutShard(shard int, cnum int) {
	args := MigrateArgs{TraceId: atomic.AddUint64(&kv.requestCounter, 1), Shard: shard, Cnum: cnum,
		Gid: kv.gid, State: SHRD_QUERY}
	cfg := kv.queryConfig(cnum)
	info := ShardInfo{Cnum: -1}
	for cfg.Num > 0 && !kv.killed() {
		destGid := cfg.Shards[shard]
		servers, ok := cfg.Groups[destGid]
		kv.assertf(ok, "[fixMoveOutShard] Invalid config:%+v\n", cfg)
		sid := 0
		retry := kv.gid != destGid
		for retry && !kv.killed() {
			srv := kv.make_end(servers[sid%len(servers)])
			reply := MigrateReply{}
			kv.logf("fixMoveOutShard request start, (gid:%v, tr:%v), shard:%v, cnum:%v\n",
				kv.gid, args.TraceId, shard, cfg.Num)
			ok := srv.Call("ShardKV.ModifyShard", &args, &reply)
			kv.logf("fixMoveOutShard request end, (gid:%v, tr:%v), ok:%v, err:%v\n", kv.gid,
				args.TraceId, ok, reply.Err)
			kv.assertf(!ok || args.TraceId == reply.TraceId,
				"TraceId (args:%v, reply:%v) unmatch!\n", args.TraceId, reply.TraceId)

			if !ok {
				sid++
				retry = sid%len(servers) != 0
				continue
			}
			switch reply.Err {
			case OK:
				info = ShardInfo{Cnum: reply.Cnum, State: reply.State}
				retry = false
			case ErrWrongLeader:
				sid++
				if sid%len(servers) == 0 {
					// wait for leader election
					time.Sleep(time.Duration(raft.ELECTION_TIME_OUT_LO/2) * time.Millisecond)
				}
			case ErrTimeout:
				sid++
				if sid%len(servers) == 0 {
					// timeout wait
					time.Sleep(16 * time.Millisecond)
				}
			default:
				kv.assertf(false, "gid:%v Invalid error code: %v in fixMoveOutShard.\n",
					kv.gid, reply.Err)
			}
		}

		if info.Cnum > 0 && (info.State == SHRD_VALID || info.State == SHRD_MOVE_OUT) {
			// we can safely mark the local shard as invalid, if the cnum has not been changed
			kv.assertf(info.Cnum >= cnum,
				"Invalid icnum:%v in fixMoveOutShard, cnum:%v, shard:%v\n", info.Cnum, cnum, shard)
			kv.markLocalShard(shard, cnum, SHRD_INVALID, nil, nil)
			return
		}
		cfg = kv.queryConfig(-1)
	}
}

func (kv *ShardKV) fetchShardRoutine(newCfg *shardctrler.Config, shard int, condCh chan<- interface{}) {
	success := true
	kv.logf("Migration shard:%v start with config num:%v\n", shard, newCfg.Num)
	defer func() {
		kv.logf("Migration shard:%v end with config num:%v, success:%v\n", shard, newCfg.Num, success)
		condCh <- success
	}()

	// Make sure the shard state we are going to read is sufficiently latest.
	if !kv.markLocalShard(shard, newCfg.Num, SHRD_QUERY, nil, nil) {
		kv.logf("failed to query shard:%v state, newCfg:%+v\n", shard, newCfg)
		success = false
		return
	}

	info := kv.getShardInfo(shard)
	if info.State == SHRD_MOVE_IN && info.Cnum < newCfg.Num &&
		!kv.migrateShard(shard, info.Cnum) {
		// If there are any aborted migration, we try to fix it up. Had we failed to fix
		// the state, it means the remote had not been marked as MOVE_OUT by the older
		// migration, we can still safely continue.
		kv.logf("failed to fix up shard:%v, info:%+v, newCfg:%+v\n", shard, info, newCfg)
	}
	if info.State == SHRD_MOVE_OUT {
		kv.fixMoveOutShard(shard, info.Cnum)
	}
	info = kv.getShardInfo(shard) // refresh the info
	if info.Cnum > newCfg.Num || newCfg.Shards[shard] != kv.gid ||
		(info.Cnum == newCfg.Num && info.State == SHRD_VALID) {
		return
	}

	kv.assertf(newCfg.Num > info.Cnum || info.State == SHRD_VALID || info.State == SHRD_MOVE_IN,
		"Shard:%v info:%+v inconsistent, newCfg:%+v\n", shard, info, newCfg)
	if newCfg.Num == 1 {
		success = kv.markLocalShard(shard, newCfg.Num, SHRD_VALID, nil, nil)
	} else {
		success = kv.migrateShard(shard, newCfg.Num)
	}
}

func (kv *ShardKV) migrateRoutine(newCfg *shardctrler.Config) {
	kv.logf("Migration enter with config:%+v\n", newCfg)
	carryOn := true
	for carryOn && !kv.killed() {
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

		kv.logf("Migration start with config:%+v\n", newCfg)
		// this is hacky, but it's fine for now, we only have 10 shards...
		kv.assertf(shardctrler.NShards <= 10,
			"If you changed the number of shards in total, think about the logic below\n.")
		condCh := make(chan interface{}, shardctrler.NShards)
		for i := 1; i < shardctrler.NShards; i++ {
			go kv.fetchShardRoutine(newCfg, i, condCh)
		}
		kv.fetchShardRoutine(newCfg, 0, condCh)
		for i := 0; i < shardctrler.NShards; i++ {
			<-condCh
		}
		kv.logf("Migration end with config num:%v\n", newCfg.Num)

		kv.mu.Lock()
		if newCfg.Num < kv.config.Num {
			kv.logf("Migrating goroutine whose config num:%v found a higher one:%v, will do"+
				"another round of migration.\n", newCfg.Num, kv.config.Num)
			kv.copyLatestConfig(newCfg)
			_, carryOn = kv.rf.GetState()
			kv.isMigrating = carryOn && !kv.killed()
		} else {
			kv.isMigrating = false
			carryOn = false
		}
		kv.mu.Unlock()
	}

	if kv.killed() {
		kv.mu.Lock()
		kv.isMigrating = false // make sure it's set to false
		kv.mu.Unlock()
	}

	kv.logf("Migration exit with config num:%v\n", newCfg.Num)
}

func (kv *ShardKV) pollConfigRoutine() {
	_, wasLeaderBefore := kv.rf.GetState()
	for !kv.killed() {
		cfg := kv.queryConfig(-1)
		if cfg.Num == -1 {
			// the client has been killed
			break
		}
		newCfg := shardctrler.Config{Groups: make(map[int][]string, len(cfg.Groups))}
		needMigrate := false
		_, isLeader := kv.rf.GetState()

		kv.mu.Lock()
		updated := cfg.Num > kv.config.Num
		if updated {
			kv.config = cfg
		}

		if (!kv.isMigrating && updated || !wasLeaderBefore) && isLeader {
			kv.assertf(!kv.isMigrating,
				"A migrate routine is already there, updated:%v, isLeader:%v, wasLeader:%v\n",
				updated, isLeader, wasLeaderBefore)
			kv.isMigrating = true
			needMigrate = true
		}
		kv.copyLatestConfig(&newCfg)
		kv.mu.Unlock()

		if isLeader && updated {
			kv.logf("updates config:%+v\n", newCfg)
		}
		if needMigrate {
			go kv.migrateRoutine(&newCfg)
		}
		wasLeaderBefore = isLeader
		time.Sleep(SV_POLL_CFG_TIME_OUT * time.Millisecond)
	}
}

func (kv *ShardKV) queryConfig(num int) shardctrler.Config {
	kv.ctrlmu.Lock()
	defer kv.ctrlmu.Unlock()
	return kv.mck.Query(num)
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
	labgob.Register(map[int64]uint64{})
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

	kv.logf("Server starting...\n")

	if persister.SnapshotSize() > 0 {
		kv.applySnapshot(persister.ReadSnapshot(), 0)
	} else {
		for i := 0; i < shardctrler.NShards; i++ {
			kv.table[i] = make(map[string]string)
		}
		kv.maxSrvdIds = make(map[int64]uint64)
		kv.lastAppIdx = 0
		kv.shardInfos = [shardctrler.NShards]ShardInfo{}
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
