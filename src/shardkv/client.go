package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"log"
	"math/big"
	"sync/atomic"
	"time"

	"6.824/labrpc"
	"6.824/shardctrler"
)

const CLNT_MAX_RETRY_CNT = 3

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd

	clientId       int64
	requestCounter uint64
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.config = shardctrler.Config{}
	ck.make_end = make_end

	ck.clientId = nrand()
	ck.requestCounter = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	args.ClientId = ck.clientId
	args.Key = key

	ck.config = ck.sm.Query(-1)
	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		args.CfgNum = ck.config.Num
		args.Shard = shard
		abort := false
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			si := 0
			for si < len(servers) && !abort {
				args.RequestId = atomic.AddUint64(&ck.requestCounter, 1)
				srv := ck.make_end(servers[si])
				var reply GetReply
				DPrintf("clnt:%v Get request start, (r:%v, c:%v), key:%v, cnum:%v, shard:%v\n",
					args.ClientId, args.RequestId, args.ClientId, args.Key, args.CfgNum, args.Shard)
				ok := srv.Call("ShardKV.Get", &args, &reply)
				DPrintf("clnt:%v Get request ends, (r:%v, c:%v), ok:%v, err:%+v\n", args.ClientId,
					args.RequestId, args.ClientId, ok, reply.Err)
				if ok && args.RequestId != reply.RequestId {
					log.Fatalf("Unmatched request ID! args: %+v, reply: %+v", args, reply)
				}
				if !ok {
					reply.Err = ErrTimeout
				}
				switch reply.Err {
				case OK:
					fallthrough
				case ErrNoKey:
					return reply.Value
				case ErrWrongGroup:
					abort = true
				case ErrWrongLeader:
					fallthrough
				case ErrTimeout:
					si++
				case ErrRetry:
					time.Sleep(WAIT_MIGRATE_TIME_OUT * time.Millisecond)
				default:
					log.Fatalf("clnt:%v Invalid error code: %v in Get.\n", ck.clientId, reply.Err)
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	opcode := uint8(0)
	if op == "Put" {
		opcode = OP_PUT
	} else if op == "Append" {
		opcode = OP_APPEND
	} else {
		log.Fatalf("clnt:%v Illegal op string (%v) in P/A\n", ck.clientId, op)
	}

	ck.config = ck.sm.Query(-1)
	args := PutAppendArgs{RequestId: atomic.AddUint64(&ck.requestCounter, 1), ClientId: ck.clientId,
		Key: key, Value: value, Opcode: opcode}

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		args.CfgNum = ck.config.Num
		args.Shard = shard
		abort := false
		if servers, ok := ck.config.Groups[gid]; ok {
			si := 0
			for si < len(servers) && !abort {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				DPrintf(
					"clnt:%v P/A request start, (r:%v, c:%v), key:%v, value:%v, cnum:%v, shard:%v\n",
					args.ClientId, args.RequestId, args.ClientId, key, value, args.CfgNum, args.Shard)
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				DPrintf("clnt:%v P/A request ends, (r:%v, c:%v), ok:%v, err:%+v\n", args.ClientId,
					args.RequestId, args.ClientId, ok, reply.Err)
				if ok && args.RequestId != reply.RequestId {
					log.Fatalf("Unmatched request ID! args: %+v, reply: %+v", args, reply)
				}
				if !ok {
					reply.Err = ErrTimeout
				}
				switch reply.Err {
				case OK:
					return
				case ErrWrongGroup:
					abort = true
				case ErrTimeout:
					fallthrough
				case ErrWrongLeader:
					si++
				case ErrRenewRequest:
					args.RequestId = atomic.AddUint64(&ck.requestCounter, 1)
				case ErrRetry:
					time.Sleep(WAIT_MIGRATE_TIME_OUT * time.Millisecond)
				default:
					log.Fatalf("clnt:%v Invalid error code: %v in P/A.\n", ck.clientId, reply.Err)
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
