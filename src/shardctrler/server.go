package shardctrler

import (
	"bytes"
	"fmt"
	"log"
	"sort"
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
		fmt.Printf("[%v]: "+str, time.Now().Format("01-02-2006 15:04:05.000"))
	}
	return
}

type ServerError uint8

const (
	SvOK ServerError = iota
	SvChanTimeOut
	SvChanClosed
	SvSnapshotApply
)

const (
	OP_QUERY uint8 = iota
	OP_JOIN
	OP_LEAVE
	OP_MOVE
)

const SV_CHAN_TIME_OUT = 2     // 2 seconds
const SV_SNAPSHOT_FACTOR = 0.9 // when it's SV_SNAPSHOT_FACTOR * maxraftstate, we do snapshot
const SV_APPLY_CHAN_SIZE = 32  // the apply channel buffer size
const SV_RESTART_THRESHOLD = 1
const SV_MAX_RAFT_SATE = 4096

type ApplyReply struct {
	err    ServerError
	opcode uint8
	config *Config
}

type RequestEntry struct {
	retryCnt int
	ch       chan ApplyReply
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	snpshtSwitch int32      // used to prevent redundant notifications for snapshot
	snpshtcond   *sync.Cond // snapshot conditinal varialbe
	persister    *raft.Persister
	maxraftstate int // snapshot if log grows this big

	// === Persistent States ===
	configs    []Config         // indexed by config num
	maxSrvdIds map[int64]uint64 // {clientId : maxServedId for the client}
	lastAppIdx int

	// === Volatile States ===
	reqMap map[int64](map[uint64]RequestEntry) // {clientId : {requestId : RequestEntry}}
}

type Op struct {
	ClientId  int64
	RequestId uint64
	Opcode    uint8
	Opdata    []byte
}

func (sc *ShardCtrler) timedWait(clntId int64, reqId uint64, ch <-chan ApplyReply,
	sec int) ApplyReply {

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
		DPrintf("sc:%v, (r:%v, c:%v), times out while waiting for applier!\n",
			sc.me, reqId, clntId)
		return ApplyReply{err: SvChanTimeOut}
	}
}

// `sc.mu` must be acquired while entering into the function
func (sc *ShardCtrler) checkRequestStatus(clientId int64, reqId uint64) (bool, int) {
	clientMap, ok := sc.reqMap[clientId]
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

// `sc.mu` must be acquired while entering into the function
func (sc *ShardCtrler) hasRequestServed(clientId int64, reqId uint64) bool {
	maxSvId, ok := sc.maxSrvdIds[clientId]
	return ok && maxSvId >= reqId
}

func (sc *ShardCtrler) setRequestChan(clientId int64, reqId uint64, index int, ch chan ApplyReply) {
	sc.assertf(index > 0, "Invalid index:%v\n", index)
	sc.mu.Lock()
	defer sc.mu.Unlock()

	clientMap, ok := sc.reqMap[clientId]
	if !ok {
		clientMap = make(map[uint64]RequestEntry)
		sc.reqMap[clientId] = clientMap
	}
	clientMap[reqId] = RequestEntry{retryCnt: 0, ch: ch}
}

// Return values:
// The 1st is the channel required to wait for the applier's reply;
// The 2nd is whether the request has been served, where the first return value will be nil if true
func (sc *ShardCtrler) setOrGetReqChan(clientId int64, reqId uint64, isProcessing bool, restart bool) (
	chan ApplyReply, bool) {

	sc.mu.Lock()
	defer sc.mu.Unlock()

	if sc.hasRequestServed(clientId, reqId) {
		return nil, true
	}

	clientMap, ok := sc.reqMap[clientId]
	if !ok {
		sc.assertf(!isProcessing, "buildRequestChan: ClientId:%v, RequestId:%v\n",
			clientId, reqId)
		clientMap = make(map[uint64]RequestEntry)
		sc.reqMap[clientId] = clientMap
	}

	var appch chan ApplyReply
	entry, hasEntry := clientMap[reqId]
	if hasEntry {
		sc.assertf(isProcessing, "a pre-existing request entry means a duplicated request arrived\n")
		DPrintf("sc:%v receives a duplicate (r:%v, c:%v).\n", sc.me, reqId, clientId)
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

func (sc *ShardCtrler) delRequestChan(clientId int64, reqId uint64) {
	sc.mu.Lock()
	clientMap, ok := sc.reqMap[clientId]
	if ok {
		delete(clientMap, reqId)
	}
	sc.mu.Unlock()
}

func (sc *ShardCtrler) explainErr4QueryReply(serr ServerError, qreply *QueryReply) {
	switch serr {
	case SvOK:
		qreply.Err = OK
	case SvSnapshotApply:
		// When we get the reply from the snapshoter, it means we're not the leader any more.
		qreply.Err = ErrWrongLeader
	case SvChanTimeOut:
		qreply.Err = ErrRetry
	default:
		// Query operation shall not get a SvChanClosed error because it doesn't care about
		// duplicated requests.
		sc.assertf(false, "Got an invalid error code: %v when doing Query.\n", serr)
	}
}

func (sc *ShardCtrler) explainErr4JLMReply(serr ServerError, err *Err) {
	switch serr {
	case SvOK:
		fallthrough
	case SvSnapshotApply:
		fallthrough
	case SvChanClosed:
		// a closed channel means the request has been served
		*err = OK
	case SvChanTimeOut:
		*err = ErrRetry
	default:
		sc.assertf(false, "Got an invalid error code: %v when doing J/L/M.\n", serr)
	}
}

func encodeJoin(servers map[int][]string) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(servers)
	return w.Bytes()
}

func encodeLeave(gids []int) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(gids)
	return w.Bytes()
}

func encodeMove(shard int, gid int) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(shard)
	e.Encode(gid)
	return w.Bytes()
}

func encodeQuery(num int) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(num)
	return w.Bytes()
}

// For Join/Leave/Move
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

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	reply.RequestId = args.RequestId
	sc.mu.Lock()
	isProcessing, retryCnt := sc.checkRequestStatus(args.ClientId, args.RequestId)
	hasServed := sc.hasRequestServed(args.ClientId, args.RequestId)
	sc.mu.Unlock()

	restart := retryCnt > SV_RESTART_THRESHOLD

	sc.assertf((isProcessing && !hasServed) || !isProcessing,
		"Join handler, args: %+v, new: %v, served: %v\n", *args, isProcessing, hasServed)
	if hasServed {
		reply.Err = OK
		DPrintf("sc:%v received a duplicated request (r:%v, c:%v) which has already been served.\n",
			sc.me, args.RequestId, args.ClientId)
		return
	}

	index := -1
	var term int
	var isLeader bool
	if isProcessing && !restart {
		term, isLeader = sc.rf.GetState()
	} else {
		index, term, isLeader = sc.rf.Start(Op{ClientId: args.ClientId, RequestId: args.RequestId,
			Opcode: OP_JOIN, Opdata: encodeJoin(args.Servers)})
	}
	DPrintf("sc:%v received Join (r:%v, c:%v), isProcessing:%v, hasServed:%v, restart:%v, retry:%v"+
		", index:%v, term:%v\n", sc.me, args.RequestId, args.ClientId, isProcessing, hasServed,
		restart, retryCnt, index, term)

	if !isLeader {
		reply.Err = ErrWrongLeader
		DPrintf("sc:%v is not the leader in Join, (r:%v, c:%v)\n", sc.me,
			args.RequestId, args.ClientId)
		return
	}

	ch, hasServed := sc.setOrGetReqChan(args.ClientId, args.RequestId, isProcessing, restart)
	if hasServed { // we need to check this yet again
		reply.Err = OK
		DPrintf("sc:%v received a duplicated request (args: %+v) which has already been served.\n",
			sc.me, *args)
		return
	}

	chReply := sc.timedWait(args.ClientId, args.RequestId, ch, SV_CHAN_TIME_OUT)
	sc.explainErr4JLMReply(chReply.err, &reply.Err)
	if reply.Err != OK {
		return
	}

	sc.assertf(chReply.err == SvSnapshotApply || (chReply.opcode == OP_JOIN),
		"mistaken reply in Join! (r:%v, c:%v)\n", args.RequestId, args.ClientId)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	reply.RequestId = args.RequestId
	sc.mu.Lock()
	isProcessing, retryCnt := sc.checkRequestStatus(args.ClientId, args.RequestId)
	hasServed := sc.hasRequestServed(args.ClientId, args.RequestId)
	sc.mu.Unlock()

	restart := retryCnt > SV_RESTART_THRESHOLD

	sc.assertf((isProcessing && !hasServed) || !isProcessing,
		"Leave handler, args: %+v, new: %v, served: %v\n", *args, isProcessing, hasServed)
	if hasServed {
		reply.Err = OK
		DPrintf("sc:%v received a duplicated request (r:%v, c:%v) which has already been served.\n",
			sc.me, args.RequestId, args.ClientId)
		return
	}

	index := -1
	var term int
	var isLeader bool
	if isProcessing && !restart {
		term, isLeader = sc.rf.GetState()
	} else {
		index, term, isLeader = sc.rf.Start(Op{ClientId: args.ClientId, RequestId: args.RequestId,
			Opcode: OP_LEAVE, Opdata: encodeLeave(args.GIDs)})
	}
	DPrintf("sc:%v received Leave (r:%v, c:%v), isProcessing:%v, hasServed:%v, restart:%v, retry:%v"+
		", index:%v, term:%v\n", sc.me, args.RequestId, args.ClientId, isProcessing, hasServed,
		restart, retryCnt, index, term)

	if !isLeader {
		reply.Err = ErrWrongLeader
		DPrintf("sc:%v is not the leader in Leave, (r:%v, c:%v)\n", sc.me,
			args.RequestId, args.ClientId)
		return
	}

	ch, hasServed := sc.setOrGetReqChan(args.ClientId, args.RequestId, isProcessing, restart)
	if hasServed { // we need to check this yet again
		reply.Err = OK
		DPrintf("sc:%v received a duplicated request (args: %+v) which has already been served.\n",
			sc.me, *args)
		return
	}

	chReply := sc.timedWait(args.ClientId, args.RequestId, ch, SV_CHAN_TIME_OUT)
	sc.explainErr4JLMReply(chReply.err, &reply.Err)
	if reply.Err != OK {
		return
	}

	sc.assertf(chReply.err == SvSnapshotApply || (chReply.opcode == OP_LEAVE),
		"mistaken reply in Leave! (r:%v, c:%v)\n", args.RequestId, args.ClientId)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	reply.RequestId = args.RequestId
	sc.mu.Lock()
	isProcessing, retryCnt := sc.checkRequestStatus(args.ClientId, args.RequestId)
	hasServed := sc.hasRequestServed(args.ClientId, args.RequestId)
	sc.mu.Unlock()

	restart := retryCnt > SV_RESTART_THRESHOLD

	sc.assertf((isProcessing && !hasServed) || !isProcessing,
		"Move handler, args: %+v, new: %v, served: %v\n", *args, isProcessing, hasServed)
	if hasServed {
		reply.Err = OK
		DPrintf("sc:%v received a duplicated request (r:%v, c:%v) which has already been served.\n",
			sc.me, args.RequestId, args.ClientId)
		return
	}

	index := -1
	var term int
	var isLeader bool
	if isProcessing && !restart {
		term, isLeader = sc.rf.GetState()
	} else {
		index, term, isLeader = sc.rf.Start(Op{ClientId: args.ClientId, RequestId: args.RequestId,
			Opcode: OP_MOVE, Opdata: encodeMove(args.Shard, args.GID)})
	}
	DPrintf("sc:%v received Move (r:%v, c:%v), isProcessing:%v, hasServed:%v, restart:%v, retry:%v"+
		", index:%v, term:%v\n", sc.me, args.RequestId, args.ClientId, isProcessing, hasServed,
		restart, retryCnt, index, term)

	if !isLeader {
		reply.Err = ErrWrongLeader
		DPrintf("sc:%v is not the leader in Move, (r:%v, c:%v)\n", sc.me,
			args.RequestId, args.ClientId)
		return
	}

	ch, hasServed := sc.setOrGetReqChan(args.ClientId, args.RequestId, isProcessing, restart)
	if hasServed { // we need to check this yet again
		reply.Err = OK
		DPrintf("sc:%v received a duplicated request (args: %+v) which has already been served.\n",
			sc.me, *args)
		return
	}

	chReply := sc.timedWait(args.ClientId, args.RequestId, ch, SV_CHAN_TIME_OUT)
	sc.explainErr4JLMReply(chReply.err, &reply.Err)
	if reply.Err != OK {
		return
	}

	sc.assertf(chReply.err == SvSnapshotApply || (chReply.opcode == OP_MOVE),
		"mistaken reply in Move! (r:%v, c:%v)\n", args.RequestId, args.ClientId)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	reply.RequestId = args.RequestId
	sc.mu.Lock()
	isProcessing, _ := sc.checkRequestStatus(args.ClientId, args.RequestId)
	hasServed := sc.hasRequestServed(args.ClientId, args.RequestId)
	sc.mu.Unlock()

	sc.assertf(!isProcessing && !hasServed,
		"There shall never be duplicated Query requests. args: %+v, new: %v, served: %v\n",
		*args, isProcessing, hasServed)

	// Because Query operation has got no side effects, we don't care if the request id is duplicated
	// and just go ahead to do the work.
	index, term, isLeader := sc.rf.Start(Op{ClientId: args.ClientId, RequestId: args.RequestId,
		Opcode: OP_QUERY, Opdata: encodeQuery(args.Num)})
	DPrintf("sc:%v received Query (r:%v, c:%v), index:%v, term:%v\n", sc.me, args.RequestId,
		args.ClientId, index, term)

	if !isLeader {
		reply.Err = ErrWrongLeader
		DPrintf("sc:%v is not the leader in Query, (r:%v, c:%v)\n", sc.me, args.RequestId,
			args.ClientId)
		return
	}

	// make a buffered channel of size 1 to prevent from blocking the applier
	ch := make(chan ApplyReply, 1)
	sc.setRequestChan(args.ClientId, args.RequestId, index, ch)

	chReply := sc.timedWait(args.ClientId, args.RequestId, ch, SV_CHAN_TIME_OUT)
	if chReply.err == SvChanTimeOut {
		// prevent channel leak
		sc.delRequestChan(args.ClientId, args.RequestId)
	}

	sc.explainErr4QueryReply(chReply.err, reply)
	if reply.Err != OK {
		return
	}

	sc.assertf(chReply.config != nil, "null config in the reply from Query.\n")
	sc.assertf(chReply.opcode == OP_QUERY,
		"mistaken reply in Query! (r:%v, c:%v)\n", args.RequestId, args.ClientId)
	reply.Config = *chReply.config
}

func spreadShards(config *Config) {
	mod := len(config.Groups)
	if mod == 0 {
		// zero out the shards assignment
		config.Shards = [NShards]int{}
		return
	}

	gidarr := make([]int, 0, mod)
	for gid := range config.Groups {
		gidarr = append(gidarr, gid)
	}
	// sort the gid array as the iterative order of map is undeterministic
	sort.Slice(gidarr, func(i, j int) bool {
		return gidarr[i] < gidarr[j]
	})
	// do round-robin to spread the shards
	for i := 0; i < NShards; i++ {
		config.Shards[i] = gidarr[i%mod]
	}
}

func (sc *ShardCtrler) createConfig() (newcfg *Config) {
	config := Config{Groups: make(map[int][]string)}
	num := len(sc.configs)
	curcfg := &sc.configs[num-1]
	sc.assertf(num == curcfg.Num+1, "Num out of order in configs. (%v != %v)\n",
		num, curcfg.Num+1)

	config.Num = num
	// copy the original map
	for gid, cluster := range curcfg.Groups {
		config.Groups[gid] = cluster
	}
	// copy the shard slice
	config.Shards = curcfg.Shards
	sc.configs = append(sc.configs, config)

	return &sc.configs[num]
}

func (sc *ShardCtrler) applyQuery(reply *ApplyReply, data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var cnum int
	err := d.Decode(&cnum)
	sc.assertf(err == nil, "Failed to decode `Num` when applying Query, err: %v\n", err)

	if cnum < 0 || cnum >= len(sc.configs) {
		cnum = len(sc.configs) - 1
	}
	reply.config = &sc.configs[cnum]
}

func (sc *ShardCtrler) applyJoin(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var servers map[int][]string
	err := d.Decode(&servers)
	sc.assertf(err == nil, "Failed to decode `Servers` when applying Join, err: %v\n", err)

	newcfg := sc.createConfig()
	for gid, cluster := range servers {
		_, exist := newcfg.Groups[gid]
		sc.assertf(!exist, "Found an already existing gid%v in Join.\n", gid)
		newcfg.Groups[gid] = cluster
	}
	spreadShards(newcfg)
}

func (sc *ShardCtrler) applyLeave(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var gids []int
	err := d.Decode(&gids)
	sc.assertf(err == nil, "Failed to decode `GIDS` when applying Leave, err: %v\n", err)

	newcfg := sc.createConfig()
	for _, gid := range gids {
		_, exist := newcfg.Groups[gid]
		sc.assertf(exist, "Cannot find the gid:%v to be removed.\n", gid)
		delete(newcfg.Groups, gid)
	}
	spreadShards(newcfg)
}

func (sc *ShardCtrler) applyMove(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var shard int
	var gid int
	err := d.Decode(&shard)
	sc.assertf(err == nil, "Failed to decode `Shards` when applying Move, err: %v\n", err)
	err = d.Decode(&gid)
	sc.assertf(err == nil, "Failed to decode `GID` when applying Move, err: %v\n", err)

	newcfg := sc.createConfig()
	newcfg.Shards[shard] = gid
}

func (sc *ShardCtrler) applyCommand(cmd *Op, index int) {
	reply := ApplyReply{err: SvOK, opcode: cmd.Opcode, config: nil}
	var hasEntry bool
	var entry RequestEntry

	sc.mu.Lock()

	clientMap, clientExists := sc.reqMap[cmd.ClientId]
	if clientExists {
		entry, hasEntry = clientMap[cmd.RequestId]
		if hasEntry {
			delete(clientMap, cmd.RequestId)
		}
	}

	id, ok := sc.maxSrvdIds[cmd.ClientId]
	isdup := ok && id >= cmd.RequestId

	if !isdup {
		// update the max served id if it applies
		sc.maxSrvdIds[cmd.ClientId] = cmd.RequestId

		switch cmd.Opcode {
		case OP_QUERY:
			sc.applyQuery(&reply, cmd.Opdata)
		case OP_JOIN:
			sc.applyJoin(cmd.Opdata)
		case OP_LEAVE:
			sc.applyLeave(cmd.Opdata)
		case OP_MOVE:
			sc.applyMove(cmd.Opdata)
		default:
			sc.assertf(false, "The applier has got an unknown op: %v\n", cmd.Opcode)
		}
	}

	sc.assertf(index == sc.lastAppIdx+1, "raft index out of order! prev:%v, cur:%v\n",
		sc.lastAppIdx, index)
	sc.lastAppIdx = index

	sc.mu.Unlock()

	if hasEntry {
		entry.ch <- reply
		close(entry.ch)
	}

	if isdup {
		DPrintf("sc:%v found a duplicate request (r:%v, c:%v) in log idx:%v, ignore it!\n", sc.me,
			cmd.RequestId, cmd.ClientId, index)
	}
	// else {
	// 	DPrintf("sc:%v applied command:%+v idx:%v, clientExists: %v, hasEntry: %v\n", sc.me, *cmd,
	// 		index, clientExists, hasEntry)
	// }

	if sc.time4Snapshot() && sc.turnOnSnpshtSwitch() {
		sc.snpshtcond.Signal()
		DPrintf("sc:%v signaled the snapshoter (r:%v, c:%v) at log idx:%v. raft size:%v\n", sc.me,
			cmd.RequestId, cmd.ClientId, index, sc.persister.RaftStateSize())
	}
}

func (sc *ShardCtrler) applyRoutine() {
	for msg := range sc.applyCh {
		if sc.killed() {
			break
		}

		if msg.SnapshotValid {
			DPrintf("sc:%v is applying snapshot, idx:%v, term:%v\n", sc.me, msg.SnapshotIndex,
				msg.SnapshotTerm)
			sc.applySnapshot(msg.Snapshot, msg.SnapshotIndex)
		} else if msg.CommandValid {
			cmd, ok := msg.Command.(Op)
			sc.assertf(ok, "The applier has got an invalid command: %v, index: %v\n",
				msg.Command, msg.CommandIndex)
			sc.applyCommand(&cmd, msg.CommandIndex)
		} else {
			sc.assertf(false,
				"Received an invalid apply msg, cmd: %v, cIdx: %v, sTerm: %v, sIdx: %v\n",
				msg.Command, msg.CommandIndex, msg.SnapshotTerm, msg.SnapshotIndex)
		}
	}
}

func (sc *ShardCtrler) turnOnSnpshtSwitch() bool {
	return atomic.CompareAndSwapInt32(&sc.snpshtSwitch, 0, 1)
}

func (sc *ShardCtrler) turnOffSnpshtSwitch() bool {
	return atomic.CompareAndSwapInt32(&sc.snpshtSwitch, 1, 0)
}

func (sc *ShardCtrler) isSnpshtSwitchOn() bool {
	return atomic.CompareAndSwapInt32(&sc.snpshtSwitch, 1, 1)
}

func (sc *ShardCtrler) time4Snapshot() bool {
	return sc.maxraftstate > 0 && float64(sc.persister.RaftStateSize()) >
		float64(sc.maxraftstate)*SV_SNAPSHOT_FACTOR
}

func (sc *ShardCtrler) snapshot() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(sc.configs)
	e.Encode(sc.maxSrvdIds)
	e.Encode(sc.lastAppIdx)

	sc.rf.Snapshot(sc.lastAppIdx, w.Bytes())
	DPrintf("sc:%v saving snapshot, lastAppIdx:%v, raft size:%v\n", sc.me, sc.lastAppIdx,
		sc.persister.RaftStateSize())

	isoff := sc.turnOffSnpshtSwitch()
	sc.assertf(isoff, "Some sneaky guy has switched off `snpshtSwitch`!\n")
}

func (sc *ShardCtrler) snapshotRoutine() {
	sc.assertf(sc.maxraftstate > 0, "Invalid maxraftstate: %v\n", sc.maxraftstate)
	sc.mu.Lock()
	defer sc.mu.Unlock()

	for !sc.killed() {
		if sc.time4Snapshot() && sc.isSnpshtSwitchOn() {
			DPrintf("sc:%v snapshoter waken up, raft state: %v\n", sc.me, sc.persister.RaftStateSize())
			sc.snapshot()
		} else {
			sc.snpshtcond.Wait()
		}
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	sc.snpshtcond.Signal()
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

func (sc *ShardCtrler) assertf(assertion bool, format string, a ...interface{}) {
	if assertion {
		return
	}
	str := fmt.Sprintf(format, a...)
	log.Fatalf("[server=%v][ASSERT TRAP]: "+str, sc.me)
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) applySnapshot(data []byte, sindex int) {
	sc.assertf(len(data) > 0, "Invalid data in applySnapshot!\n")

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	sc.mu.Lock()
	defer sc.mu.Unlock()

	sc.configs = make([]Config, 0)
	err := d.Decode(&sc.configs)
	sc.assertf(err == nil, "Failed to decode `table` when applying snapshot, err: %v\n", err)

	sc.maxSrvdIds = make(map[int64]uint64)
	err = d.Decode(&sc.maxSrvdIds)
	sc.assertf(err == nil, "Failed to decode `maxSrvdIds` when applying snapshot, err: %v\n", err)

	sc.lastAppIdx = 0
	err = d.Decode(&sc.lastAppIdx)
	sc.assertf(err == nil, "Failed to decode `lastAppIdx` when applying snapshot, err: %v\n", err)

	if sindex > 0 {
		sc.assertf(sindex == sc.lastAppIdx,
			"Found an inconsistent snapshot: sIdx:%v, savedIdx:%v\n", sindex, sc.lastAppIdx)
	}

	// the applier clear the request map as much as it can
	for clientId, clientMap := range sc.reqMap {
		maxSrvdId := sc.maxSrvdIds[clientId]
		for reqId, entry := range clientMap {
			if reqId <= maxSrvdId {
				entry.ch <- ApplyReply{err: SvSnapshotApply}
				close(entry.ch)
				delete(clientMap, reqId)
			}
		}
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me
	sc.maxraftstate = SV_MAX_RAFT_SATE

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.persister = persister
	sc.reqMap = make(map[int64](map[uint64]RequestEntry))

	if persister.SnapshotSize() > 0 {
		sc.applySnapshot(persister.ReadSnapshot(), 0)
	} else {
		sc.configs = make([]Config, 1)
		sc.configs[0].Groups = make(map[int][]string)
		sc.maxSrvdIds = make(map[int64]uint64)
		sc.lastAppIdx = 0
	}

	sc.snpshtcond = sync.NewCond(&sc.mu)
	sc.snpshtSwitch = 0

	go sc.applyRoutine()
	if sc.maxraftstate > 0 {
		go sc.snapshotRoutine()
	}
	return sc
}
