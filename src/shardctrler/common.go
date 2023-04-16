package shardctrler

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK             = "OK"
	ErrWrongLeader = "ErrWrongLeader"
	ErrRetry       = "ErrRetry"
)

type Err string

type JoinArgs struct {
	ClientId  int64
	RequestId uint64
	Servers   map[int][]string // new GID -> servers mappings
}

type JoinReply struct {
	RequestId uint64
	Err       Err
}

type LeaveArgs struct {
	ClientId  int64
	RequestId uint64
	GIDs      []int
}

type LeaveReply struct {
	RequestId uint64
	Err       Err
}

type MoveArgs struct {
	ClientId  int64
	RequestId uint64
	Shard     int
	GID       int
}

type MoveReply struct {
	RequestId uint64
	Err       Err
}

type QueryArgs struct {
	ClientId  int64
	RequestId uint64
	Num       int // desired config number
}

type QueryReply struct {
	RequestId uint64
	Err       Err
	Config    Config
}
