package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK                 = "OK"
	ErrNoKey           = "ErrNoKey"
	ErrWrongGroup      = "ErrWrongGroup"
	ErrWrongLeader     = "ErrWrongLeader"
	ErrTimeout         = "ErrTimeout"
	ErrRenewRequest    = "ErrRenewRequest"
	ErrRetry           = "ErrRetry"
	ErrAbort           = "ErrAbort"
	ErrTryOlderConfig  = "ErrTryOlderConfig"
	ErrTryHigherConfig = "ErrTryHigherConfig"
	ErrWaitMigrate     = "ErrWaitMigrate"
)

const WAIT_MIGRATE_TIME_OUT = 10 // millis

type Err string

const (
	OP_GET uint8 = iota
	OP_PUT
	OP_APPEND
	OP_SHARD_MIGRATE
)

type PutAppendArgs struct {
	RequestId uint64
	ClientId  int64
	Key       string
	Value     string
	CfgNum    int
	Shard     int
	Opcode    uint8 // "Put" or "Append"
}

type PutAppendReply struct {
	RequestId uint64
	Err       Err
}

type GetArgs struct {
	RequestId uint64 // used for de-duplication and identification
	ClientId  int64
	CfgNum    int
	Shard     int
	Key       string
}

type GetReply struct {
	RequestId uint64
	Err       Err
	Value     string
}
