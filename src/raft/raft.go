package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const HEARTBEAT_TIME_OUT = 100
const ELECTION_TIME_OUT_LO = 500
const ELECTION_TIME_OUT_HI = 800

type RaftRole int

const (
	RAFT_FOLLOWER = iota
	RAFT_CANDIDATE
	RAFT_LEADER
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	routineId uint32

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []LogEntry

	role      RaftRole
	hasPinged bool
}

func (rf *Raft) logf(rid uint32, format string, a ...interface{}) {
	_, logTurnedOn := os.LookupEnv("LOG_VERBOSE_M6824")
	if !logTurnedOn {
		return
	}
	str := fmt.Sprintf(format, a...)
	fmt.Printf("[%v][server=%v][rid=%v]: "+str, time.Now().Format("01-02-2006 15:04:05.0000"),
		rf.me, rid)
}

func (rf *Raft) assertf(rid uint32, assertion bool, format string, a ...interface{}) {
	if assertion {
		return
	}
	str := fmt.Sprintf(format, a...)
	log.Fatalf("[%v][server=%v][rid=%v][FATAL]: "+str, time.Now().Format("01-02-2006 15:04:05.0000"),
		rf.me, rid)
}

func (rf *Raft) updateTerm(term int) {
	// The caller should make sure the following things:
	// ASSERT: term > rf.currentTerm
	// ASSERT: rf.mu.Lock() has been acquired
	rf.currentTerm = term
	rf.votedFor = -1
	rf.role = RAFT_FOLLOWER
}

func (rf *Raft) refreshTerm(term int) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm < term {
		rf.updateTerm(term)
	}
	return rf.currentTerm
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.role == RAFT_LEADER
	rf.mu.Unlock()

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Rid uint32

	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Rid uint32

	Term        int
	VoteGranted bool
}

type VoteChannelMsg struct {
	abortElection bool
	voteGranted   bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rid := args.Rid
	reply.Rid = args.Rid
	reply.VoteGranted = false
	rf.logf(rid, "Receive RequestVote request from %v.\n", args.CandidateId)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.hasPinged = true
	if args.Term < rf.currentTerm {
		rf.logf(rid, "Got an old-term request in RequestVote, req term = %v, "+
			"current term = %v\n", args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.logf(rid, "Got a greater-term request in RequestVote, req term = %v, "+
			"current term = %v\n", args.Term, rf.currentTerm)
		rf.updateTerm(args.Term)
	}

	if rf.votedFor == -1 {
		rf.votedFor = args.CandidateId
	}
	// todo: add log entry comparision...
	reply.VoteGranted = rf.votedFor == args.CandidateId
	reply.Term = rf.currentTerm
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	rid := args.Rid
	rf.logf(rid, "Begin sendRequestVote to %v, term = %v, candidate = %v, lastLogIndex = %v, "+
		"lastLogTerm = %v\n", server, args.Term, args.CandidateId, args.LastLogIndex,
		args.LastLogTerm)

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	if ok {
		rf.logf(rid, "End sendRequestVote from %v, term = %v, voteGranted = %v\n", server,
			reply.Term, reply.VoteGranted)
		rf.assertf(rid, rid == reply.Rid, "The Rid in args and reply must match. "+
			"reply.Rid = %v\n", reply.Rid)
	}
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) electionCheck(rid uint32) (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	hasPinged := rf.hasPinged
	rf.hasPinged = false

	if rf.role == RAFT_LEADER {
		return rf.currentTerm, false
	}

	launchElection := false
	if rf.role == RAFT_CANDIDATE {
		launchElection = !hasPinged || (rf.votedFor == -1)
	} else {
		rf.assertf(rid, rf.role == RAFT_FOLLOWER, "Got unknown role in validityCheck().\n")
		launchElection = !hasPinged
	}

	if launchElection {
		rf.role = RAFT_CANDIDATE
		rf.currentTerm++
		rf.votedFor = -1 // it will vote for itself when collecting the votes
	}
	return rf.currentTerm, launchElection
}

func (rf *Raft) voteForSelf(rid uint32, term int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if term != rf.currentTerm {
		rf.logf(rid, "Term has changed when launching the election. "+
			"election term = %v, current term = %v\n", term, rf.currentTerm)
		return false
	}

	if rf.votedFor != -1 {
		rf.logf(rid, "Already voted for %v when trying to vote for self.\n", rf.votedFor)
		rf.assertf(rid, rf.votedFor != rf.me,
			"The votedFor must not be me before it manages to vote for itself.\n")
		return false
	}

	// Finally, we're safe to do the self voting.
	rf.votedFor = rf.me
	return true
}

func (rf *Raft) claimLeadership(rid uint32, term int) {
	hasBecomeLeader := false

	rf.mu.Lock()
	if term != rf.currentTerm {
		// some higher term server has already claimed the leadership
		rf.assertf(rid, rf.currentTerm > term, "There couldn't be a lower term in claimLeadership.\n")
	} else {
		rf.role = RAFT_LEADER
		hasBecomeLeader = true
	}
	rf.mu.Unlock()

	if hasBecomeLeader {
		rf.logf(rid, "%v has become the leader and starts to send out heartbeats\n", rf.me)
		go rf.heartbeats(atomic.AddUint32(&rf.routineId, 1), term)
	}
}

func (rf *Raft) sendVoteRoutine(rid uint32, peer int, term int, ch chan VoteChannelMsg) {
	// todo: revise LastLogIndex and LastLogTerm
	args := RequestVoteArgs{Rid: rid, Term: term, CandidateId: rf.me, LastLogIndex: 0, LastLogTerm: 0}
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(peer, &args, &reply)
	if !ok {
		rf.logf(rid, "Server %v's vote request to %v has failed.\n", rf.me, peer)
		ch <- VoteChannelMsg{abortElection: false, voteGranted: false}
		return
	}

	if reply.Term != term {
		rf.assertf(rid, reply.Term > term, "There couldn't be a lower term in the reply.\n")
		rf.logf(rid, "collectVotes: %v found out it's no longer the leader when checking "+
			"reply's term = %v.\n", rf.me, reply.Term)
		rf.refreshTerm(reply.Term)
		ch <- VoteChannelMsg{abortElection: true, voteGranted: false}
		// term has changed, we give up competing the election
		return
	}

	// the channel is buffered so it won't be blocked
	ch <- VoteChannelMsg{abortElection: false, voteGranted: reply.VoteGranted}
}

func (rf *Raft) collectVotes(rid uint32, term int) {
	if !rf.voteForSelf(rid, term) {
		// we failed to vote for myself, abort the election now
		return
	}

	nvote := 1
	// use buffered channel to prevent goroutine leak
	ch := make(chan VoteChannelMsg, len(rf.peers)-1)
	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			continue
		}

		go rf.sendVoteRoutine(atomic.AddUint32(&rf.routineId, 1), peer, term, ch)
	}

	majority := len(rf.peers)/2 + 1
	timer := time.After(time.Duration(ELECTION_TIME_OUT_LO) * time.Millisecond)
	for i := 0; i < len(rf.peers)-1 && nvote < majority; i++ {
		select {
		case votemsg := <-ch:
			if votemsg.abortElection {
				return
			}
			if votemsg.voteGranted {
				nvote++
			}
		case <-timer:
			rf.logf(rid, "Failed to collect enough votes during the given time. "+
				"Give up the election at term %v.\n", term)
			return
		}
	}

	if nvote >= majority {
		// it won the election
		rf.logf(rid, "%v has won the election in term %v and is claiming its leadership.\n",
			rf.me, term)
		rf.claimLeadership(rid, term)
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker(rid uint32) {
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		diff := ELECTION_TIME_OUT_HI - ELECTION_TIME_OUT_LO + 1
		timeout := rand.Intn(diff) + ELECTION_TIME_OUT_LO
		time.Sleep(time.Duration(timeout) * time.Millisecond)

		term, launchElection := rf.electionCheck(rid)
		if launchElection {
			rf.logf(rid, "Launch election. term = %v, candidate = %v\n", term, rf.me)
			go rf.collectVotes(atomic.AddUint32(&rf.routineId, 1), term)
		}
	}
}

type AppendEntryArgs struct {
	Rid uint32

	Term     int
	LeaderId int

	PrevLogIndex      int
	PrevLogTerm       int
	Entries           []LogEntry
	LeaderCommitIndex int
}

type AppendEntryReply struct {
	Rid uint32

	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	rid := args.Rid
	reply.Rid = args.Rid
	reply.Success = false
	rf.logf(rid, "Receive AppendEntries request from %v.\n", args.LeaderId)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.hasPinged = true
	if args.Term < rf.currentTerm {
		rf.logf(rid, "Got an old-term request in AppendEntries, req term = %v, "+
			"current term = %v\n", args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.logf(rid, "Got a greater-term request in AppendEntries, req term = %v, "+
			"current term = %v\n", args.Term, rf.currentTerm)
		rf.updateTerm(args.Term)
	}

	reply.Term = rf.currentTerm
	if len(args.Entries) == 0 {
		// skip heartbeat messages
		reply.Success = true
		return
	}
	// todo: play with the log entries and commit index...
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	rid := args.Rid
	rf.logf(rid, "Begin sendAppendEntries to %v, term = %v, leader = %v, prevLogIndex = %v, "+
		"prevLogTerm = %v, leaderCommit = %v\n", server, args.Term, args.LeaderId, args.PrevLogIndex,
		args.PrevLogTerm, args.LeaderCommitIndex)

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	if ok {
		rf.logf(rid, "End sendAppendEntries from %v, term = %v, success = %v\n", server,
			reply.Term, reply.Success)
		rf.assertf(rid, rid == reply.Rid, "The Rid in args and reply must match. reply.Rid = %v\n",
			reply.Rid)
	}
	return ok
}

func (rf *Raft) checkLeadership(rid uint32, term int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if term != rf.currentTerm {
		// some higher term server has already claimed the leadership
		rf.assertf(rid, rf.currentTerm > term, "There couldn't be a lower term in checkLeadership.\n")
		rf.logf(rid, "%v found out it's no longer the leader before sending out heartbeats. "+
			"current term = %v\n", rf.me, rf.currentTerm)
		return false
	}

	rf.assertf(rid, rf.role == RAFT_LEADER && rf.votedFor == rf.me,
		"The leader must be me at term %v\n", term)
	return true
}

func (rf *Raft) appendEntryRoutine(rid uint32, peer int, term int, ch chan bool) {
	// todo: modify the rest of the parameters
	args := AppendEntryArgs{Rid: rid, Term: term, LeaderId: rf.me,
		Entries: make([]LogEntry, 0)}
	reply := AppendEntryReply{}
	timeBeforeCall := time.Now()

	ok := rf.sendAppendEntries(peer, &args, &reply)
	if time.Since(timeBeforeCall).Milliseconds() > HEARTBEAT_TIME_OUT {
		rf.logf(rid, "heartbeat reply from %v arrives too late, drop it\n", peer)
		return
	}

	if !ok {
		rf.logf(rid, "%v's sendAppendEntries to %v has failed.\n", rf.me, peer)
		// Although the rpc failed this time, we still don't give it up.
		ch <- true
		return
	}

	if reply.Term != term {
		rf.assertf(rid, reply.Term > term, "There couldn't be a lower term in the reply.\n")
		rf.logf(rid, "heartbeats: %v found out it's no longer the leader when checking "+
			"reply's term = %v.\n", rf.me, reply.Term)
		rf.refreshTerm(reply.Term)
		// term has changed, we give up sending more heartbeats
		ch <- false
		return
	}

	// todo: handle reply.Success
	ch <- true
}

func (rf *Raft) heartbeats(rid uint32, term int) {
	ch := make(chan bool, len(rf.peers)-1)

	for !rf.killed() {
		if !rf.checkLeadership(rid, term) {
			// I am no longer the leader, it's time to bail
			return
		}

		for peer := 0; peer < len(rf.peers); peer++ {
			if peer == rf.me {
				continue
			}

			go rf.appendEntryRoutine(atomic.AddUint32(&rf.routineId, 1), peer, term, ch)
		}

		timer := time.After(time.Duration(HEARTBEAT_TIME_OUT) * time.Millisecond)
		keepWait := true
		for keepWait {
			select {
			case ok := <-ch:
				if !ok {
					return
				}
			case <-timer:
				keepWait = false
			}
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.routineId = uint32(me) << 16

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)

	rf.role = RAFT_FOLLOWER
	rf.hasPinged = false

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rand.Seed(time.Now().UnixNano())
	// start ticker goroutine to start elections

	go rf.ticker(atomic.AddUint32(&rf.routineId, 1))

	return rf
}
