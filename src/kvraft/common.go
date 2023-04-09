package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrRetry       = "ErrRetry"
)

const (
	OP_GET uint8 = iota
	OP_PUT
	OP_APPEND
)

type Err string

type PutAppendArgs struct {
	RequestId uint64
	ClientId  int64
	Key       string
	Value     string
	Opcode    uint8 // "Put" or "Append"
}

type PutAppendReply struct {
	RequestId uint64
	Err       Err
}

type GetArgs struct {
	RequestId uint64 // used for de-duplication and identification
	ClientId  int64
	Key       string
}

type GetReply struct {
	RequestId uint64
	Err       Err
	Value     string
}
