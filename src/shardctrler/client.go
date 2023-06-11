package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"log"
	"math/big"
	"sync/atomic"
	"time"

	"6.824/labrpc"
	"6.824/raft"
)

const CLNT_CHAN_TIME_OUT = 3 // 3 seconds
const CLNT_MAX_RETRY_CNT = 3

type Clerk struct {
	servers        []*labrpc.ClientEnd
	clientId       int64
	lastLeaderId   int
	requestCounter uint64
	clct           int // continuous leader changed times
	retryCount     uint32
	dead           int32 // set by Kill()
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.servers = servers
	ck.clientId = nrand()
	ck.lastLeaderId = 0
	ck.requestCounter = 0
	ck.clct = 0
	ck.retryCount = 0
	return ck
}

func (ck *Clerk) switchLeader() {
	ck.lastLeaderId = (ck.lastLeaderId + 1) % len(ck.servers)
	DPrintf("clnt:%v, Leader changed to %v.\n", ck.clientId, ck.lastLeaderId)
	ck.clct++
	if ck.clct != len(ck.servers) {
		return
	}
	// We have tried all the servers but none of them said it's the leader. In this case,
	// we just sleep for a while and hopefully they can elect a leader.
	time.Sleep(time.Duration(raft.ELECTION_TIME_OUT_HI*2) * time.Millisecond)
	ck.retryCount = 0
	ck.clct = 0
}

func (ck *Clerk) handleReply(rpcOK bool, err Err) (success bool) {
	if !rpcOK {
		err = ErrRetry
	}

	success = true
	switch err {
	case OK:
		ck.retryCount = 0
	case ErrWrongLeader:
		ck.switchLeader()
		success = false
	case ErrRetry:
		ck.retryCount++
		if ck.retryCount > CLNT_MAX_RETRY_CNT {
			ck.switchLeader()
		}
		success = false
	default:
		log.Fatalf("clnt:%v Unknown error code: %v in handleReply.\n", ck.clientId, err)
	}
	return
}

func (ck *Clerk) Kill() {
	atomic.StoreInt32(&ck.dead, 1)
}

func (ck *Clerk) killed() bool {
	z := atomic.LoadInt32(&ck.dead)
	return z == 1
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	args.ClientId = ck.clientId
	args.Num = num

	for !ck.killed() {
		// Query doesn't care about the duplicates.
		args.RequestId = atomic.AddUint64(&ck.requestCounter, 1)

		srv := ck.servers[ck.lastLeaderId]
		var reply QueryReply
		DPrintf("clnt:%v Query request start, (r:%v, c:%v), num:%v, clct: %v\n", args.ClientId,
			args.RequestId, args.ClientId, args.Num, ck.clct)
		ok := srv.Call("ShardCtrler.Query", args, &reply)
		DPrintf("clnt:%v Query request end, (r:%v, c:%v), ok:%v, reply:%+v\n", args.ClientId,
			args.RequestId, args.ClientId, ok, reply)
		if ok && args.RequestId != reply.RequestId {
			log.Fatalf("Unmatched request ID! args: %+v, reply: %+v", args, reply)
		}
		if ck.handleReply(ok, reply.Err) {
			return reply.Config
		}
	}

	return Config{Num: -1}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	args.ClientId = ck.clientId
	args.RequestId = atomic.AddUint64(&ck.requestCounter, 1)
	args.Servers = servers

	for !ck.killed() {
		srv := ck.servers[ck.lastLeaderId]
		var reply JoinReply
		DPrintf("clnt:%v Join request start, (r:%v, c:%v), servers:%v, clct: %v\n", args.ClientId,
			args.RequestId, args.ClientId, args.Servers, ck.clct)
		ok := srv.Call("ShardCtrler.Join", args, &reply)
		DPrintf("clnt:%v Join request end, (r:%v, c:%v), ok:%v, err:%+v\n", args.ClientId,
			args.RequestId, args.ClientId, ok, reply.Err)
		if ok && args.RequestId != reply.RequestId {
			log.Fatalf("Unmatched request ID! args: %+v, reply: %+v", args, reply)
		}
		if ck.handleReply(ok, reply.Err) {
			return
		}
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	args.ClientId = ck.clientId
	args.RequestId = atomic.AddUint64(&ck.requestCounter, 1)
	args.GIDs = gids

	for !ck.killed() {
		srv := ck.servers[ck.lastLeaderId]
		var reply LeaveReply
		DPrintf("clnt:%v Leave request start, (r:%v, c:%v), GIDs:%v, clct: %v\n", args.ClientId,
			args.RequestId, args.ClientId, args.GIDs, ck.clct)
		ok := srv.Call("ShardCtrler.Leave", args, &reply)
		DPrintf("clnt:%v Leave request end, (r:%v, c:%v), ok:%v, err:%+v\n", args.ClientId,
			args.RequestId, args.ClientId, ok, reply.Err)
		if ok && args.RequestId != reply.RequestId {
			log.Fatalf("Unmatched request ID! args: %+v, reply: %+v", args, reply)
		}
		if ck.handleReply(ok, reply.Err) {
			return
		}
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	args.ClientId = ck.clientId
	args.RequestId = atomic.AddUint64(&ck.requestCounter, 1)
	args.Shard = shard
	args.GID = gid

	for !ck.killed() {
		srv := ck.servers[ck.lastLeaderId]
		var reply MoveReply
		DPrintf("clnt:%v Move request start, (r:%v, c:%v), s:%v->g:%v, clct: %v\n", args.ClientId,
			args.RequestId, args.ClientId, args.Shard, args.GID, ck.clct)
		ok := srv.Call("ShardCtrler.Move", args, &reply)
		DPrintf("clnt:%v Move request end, (r:%v, c:%v), ok:%v, err:%+v\n", args.ClientId,
			args.RequestId, args.ClientId, ok, reply.Err)
		if ok && args.RequestId != reply.RequestId {
			log.Fatalf("Unmatched request ID! args: %+v, reply: %+v", args, reply)
		}
		if ck.handleReply(ok, reply.Err) {
			return
		}
	}
}
