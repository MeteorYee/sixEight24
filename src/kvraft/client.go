package kvraft

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

// A hacky counter used for assigning an unique ID to each client
var g_clientCounter int32 = 0

type Clerk struct {
	servers        []*labrpc.ClientEnd
	clientId       int
	shadowClntId   int // used for fake Get operations
	lastLeaderId   int
	requestCounter uint64
	clct           int // continuous leader changed times
	retryCount     uint32
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func (ck *Clerk) fakeClientId() int {
	i := int(nrand())
	for i == ck.clientId {
		i = int(nrand())
	}
	return i
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientId = int(atomic.AddInt32(&g_clientCounter, 1))
	ck.shadowClntId = ck.fakeClientId()
	ck.lastLeaderId = 0
	ck.requestCounter = 0
	ck.clct = 0
	ck.retryCount = 0
	DPrintf("Made client:%v, shadow id:%v.\n", ck.clientId, ck.shadowClntId)
	return ck
}

func (ck *Clerk) switchLeader(opcode uint8) {
	ck.lastLeaderId = (ck.lastLeaderId + 1) % len(ck.servers)
	ck.clct++
	if ck.clct%len(ck.servers) != 0 {
		return
	}
	if opcode != OP_GET {
		// There are times that the Raft leader just can't move on because it has no logs from
		// the current term. Then P/A operations will get stuck. Hence, this is why we're doing
		// a fake Get here, thanks to that Get operations can generate new log entries. However,
		// we could modify the Raft implementation by letting a leader commit a no-op log every
		// time it claims its leadership. Although the trick seems a better choice, it would
		// fail the lab2 test cases, unfortunately.
		ck.clientId, ck.shadowClntId = ck.shadowClntId, ck.clientId
		ck.Get("")
		ck.clientId, ck.shadowClntId = ck.shadowClntId, ck.clientId
	} else {
		// We have tried all the servers but none of them said it's the leader. In this case,
		// we just sleep for a while and hopefully they can elect a leader.
		time.Sleep(time.Duration(raft.ELECTION_TIME_OUT_HI*2) * time.Millisecond)
	}
	ck.retryCount = 0
}

func (ck *Clerk) handleReply(rpcOK bool, err Err, opcode uint8) bool {
	if !rpcOK {
		err = ErrRetry
	}

	success := true
	switch err {
	case OK:
		fallthrough
	case ErrNoKey:
		ck.retryCount = 0
	case ErrWrongLeader:
		ck.switchLeader(opcode)
		success = false
	case ErrRetry:
		ck.retryCount++
		if ck.retryCount > CLNT_MAX_RETRY_CNT {
			ck.switchLeader(opcode)
		}
		success = false
	default:
		log.Fatalf("clnt:%v Unknown error code: %v in handleReply.\n", ck.clientId, err)
	}
	return success
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	retry := true
	value := ""
	for retry {
		// For Get requests, we don't insist on the same request ID again and again.
		args := GetArgs{RequestId: atomic.AddUint64(&ck.requestCounter, 1), ClientId: ck.clientId,
			Key: key}
		reply := GetReply{}
		ch := make(chan bool, 1)
		go func(server *labrpc.ClientEnd) {
			DPrintf("clnt:%v Get request start, (r:%v, c:%v), , clct: %v\n", args.ClientId,
				args.RequestId, args.ClientId, ck.clct)
			ok := server.Call("KVServer.Get", &args, &reply)
			DPrintf("clnt:%v Get request ends, (r:%v, c:%v), ok:%v, err:%+v\n", args.ClientId,
				args.RequestId, args.ClientId, ok, reply.Err)
			if ok && args.RequestId != reply.RequestId {
				log.Fatalf("Unmatched request ID! args: %+v, reply: %+v", args, reply)
			}
			ch <- ok
			close(ch)
		}(ck.servers[ck.lastLeaderId])

		timer := time.After(time.Duration(CLNT_CHAN_TIME_OUT) * time.Second)
		select {
		case ok := <-ch:
			retry = !ck.handleReply(ok, reply.Err, OP_GET)
		case <-timer:
			retry = true
			DPrintf("clnt:%v Get request RPC: %v times out! Retrying...\n", ck.clientId, args)
		}

		value = reply.Value
	}
	return value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	opcode := uint8(0)
	if op == "Put" {
		opcode = OP_PUT
	} else if op == "Append" {
		opcode = OP_APPEND
	} else {
		log.Fatalf("clnt:%v Illegal op string (%v) in P/A\n", ck.clientId, op)
	}

	args := PutAppendArgs{RequestId: atomic.AddUint64(&ck.requestCounter, 1), ClientId: ck.clientId,
		Key: key, Value: value, Opcode: opcode}
	retry := true
	for retry {
		reply := PutAppendReply{}
		ch := make(chan bool, 1)
		go func(server *labrpc.ClientEnd) {
			DPrintf("clnt:%v P/A request start, (r:%v, c:%v), clct: %v\n", args.ClientId,
				args.RequestId, args.ClientId, ck.clct)
			ok := server.Call("KVServer.PutAppend", &args, &reply)
			DPrintf("clnt:%v P/A request ends, (r:%v, c:%v), ok:%v, err:%+v\n", args.ClientId,
				args.RequestId, args.ClientId, ok, reply.Err)
			if ok && args.RequestId != reply.RequestId {
				log.Fatalf("Unmatched request ID! args: %+v, reply: %+v", args, reply)
			}
			ch <- ok
			close(ch)
		}(ck.servers[ck.lastLeaderId])

		timer := time.After(time.Duration(CLNT_CHAN_TIME_OUT) * time.Second)
		select {
		case ok := <-ch:
			retry = !ck.handleReply(ok, reply.Err, opcode)
		case <-timer:
			retry = true
			DPrintf("clnt:%v P/A request RPC: %v times out! Retrying...\n", ck.clientId, args)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
