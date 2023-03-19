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
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

const HEARTBEAT_TIME_OUT = 100
const ELECTION_TIME_OUT_LO = 300
const ELECTION_TIME_OUT_HI = 600

const HEARTBEAT_CHAN_SIZE = 1

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

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

func intmin(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex // Lock to protect shared access to this peer's state
	applyCond *sync.Cond
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	routineId        uint64
	heartbeatCounter uint64
	lastHeartbeatId  uint64
	maxRcvHrtbtId    uint64

	// Persistent State
	// FIXME: need CRC check if we're writing to the real disk
	currentTerm int
	votedFor    int
	logs        []LogEntry

	// the max index the peer thinks it's been committed, may be less than leader's counterpart,
	// and it must be less than the length of the logs
	commitIndex int
	lastApplied int // the last index applied to the upper application

	// makes sense for the leader only
	nextIndex  []int
	matchIndex []int

	role      RaftRole
	hasPinged bool

	enableLog bool

	applyCh chan ApplyMsg

	hrtbtCh     chan struct{} // used for invoking a heartbeat explicitly
	hrtbtReqCnt int32         // records the number of heartbeat request issued by users
}

func (rf *Raft) logf(rid uint64, format string, a ...interface{}) {
	if !rf.enableLog {
		return
	}
	str := fmt.Sprintf(format, a...)
	fmt.Printf("[%v][server=%v][rid=%v]: "+str, time.Now().Format("01-02-2006 15:04:05.0000"),
		rf.me, rid)
}

func (rf *Raft) assertf(rid uint64, assertion bool, format string, a ...interface{}) {
	if assertion {
		return
	}
	str := fmt.Sprintf(format, a...)
	log.Fatalf("[server=%v][rid=%v][FATAL]: "+str, rf.me, rid)
}

func (rf *Raft) lastSnapshot() *LogEntry {
	return &rf.logs[0]
}

func (rf *Raft) getLogEntry(rid uint64, index int) *LogEntry {
	lastSnapshotIndex := rf.lastSnapshot().Index
	maxIndex := rf.lastLogEntry().Index
	rf.assertf(rid, index <= maxIndex, "index(%v) > maxIndex(%v)\n", index, maxIndex)
	rf.assertf(rid, index >= lastSnapshotIndex, "Inconsistent index in LogEntry getter. "+
		"index = %v, lastSnapshotIndex = %v\n", index, lastSnapshotIndex)
	entry := &rf.logs[index-lastSnapshotIndex]
	rf.assertf(rid, entry.Index == index, "Found unmatched log index: %v, passed in: %v, "+
		"lastSnapshotIndex = %v\n", entry.Index, index, lastSnapshotIndex)
	return entry
}

func (rf *Raft) lastLogEntry() *LogEntry {
	return &rf.logs[len(rf.logs)-1]
}

// ASSERT: protected by rf.mu
func (rf *Raft) persistedIndex() int {
	return rf.matchIndex[rf.me]
}

// ASSERT: protected by rf.mu
func (rf *Raft) setPersistedIndex(index int) {
	rf.matchIndex[rf.me] = index
}

// ASSERT: under rf.mu's protection
// left close right open: [from, to)
func (rf *Raft) logSlice(rid uint64, from int, to int) []LogEntry {
	lastSnapshotIndex := rf.lastSnapshot().Index
	logicalLen := rf.lastLogEntry().Index + 1
	rf.assertf(rid, from <= to, "from(%v) >= to(%v)\n", from, to)
	rf.assertf(rid, from <= logicalLen, "from(%v) >= len(%v)\n", from, logicalLen)
	rf.assertf(rid, to <= logicalLen, "to(%v) > len(%v)\n", to, logicalLen)
	rf.assertf(rid, from >= lastSnapshotIndex && to >= lastSnapshotIndex,
		"Invalid parameter in log slicing, from = %v, to = %v, lastSnapshotIndex = %v\n",
		from, to, lastSnapshotIndex)
	return rf.logs[from-lastSnapshotIndex : to-lastSnapshotIndex]
}

func (rf *Raft) updateTerm(term int) {
	// The caller should make sure the following things:
	// ASSERT: term > rf.currentTerm
	// ASSERT: rf.mu.Lock() has been acquired
	rf.currentTerm = term
	rf.votedFor = -1
	rf.role = RAFT_FOLLOWER
	rf.maxRcvHrtbtId = 0
}

func (rf *Raft) refreshTerm(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm < term {
		rf.updateTerm(term)
	}
}

// ASSERT: rf.mu.Lock() has been acquired
func (rf *Raft) checkAndSetTerm(rid uint64, term int, fname string) bool {
	if term < rf.currentTerm {
		rf.logf(rid, "Got an old-term request in %v, req term = %v, current term = %v\n",
			fname, term, rf.currentTerm)
		return false
	}

	if term > rf.currentTerm {
		rf.logf(rid, "Got a greater-term request in %v, req term = %v, current term = %v\n",
			fname, term, rf.currentTerm)
		rf.updateTerm(term)
	}
	return true
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
//
// ASSERT: rf.mu should be acquired while entering
func (rf *Raft) persist(rid uint64) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	// NOTE: There is no need to persist the log indexed at zero if we haven't appended anything
	// to logs. One might argue that we then wouldn't be able to persist the `currentTerm` and
	// the `votedFor`. Granted that, they could be volatile had no logs been generated as yet,
	// because they would always manage to re-establish a new consensus if any or all of them failed
	// and re-joined the cluster. In a nutshell, the values of `currentTerm` and `votedFor` can
	// start from scratch as they would've got no side effects if there were no logs at all.
	maxIndex := rf.lastLogEntry().Index
	lastPersistedIndex := rf.persistedIndex()

	rf.setPersistedIndex(maxIndex)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)

	rf.persister.SaveRaftState(w.Bytes())
	rf.logf(rid, "Persist succeeded, persisted index modified from %v to %v\n",
		lastPersistedIndex, maxIndex)
}

// restore previously persisted state.
// CAVEAT: this function must be called before any goroutines set off, so this is why there are no
// mutex protections.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	err := d.Decode(&rf.currentTerm)
	rf.assertf(0, err == nil, "Failed to read currentTerm from persister, errmsg: %v\n", err)

	err = d.Decode(&rf.votedFor)
	rf.assertf(0, err == nil, "Failed to read votedFor from persister, errmsg: %v\n", err)

	err = d.Decode(&rf.logs)
	rf.assertf(0, err == nil, "Failed to read logs from persister, errmsg: %v\n", err)

	rf.setPersistedIndex(rf.lastLogEntry().Index)
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// DEPRECATED as per the suggestion of the requirement of 2022's lab2D
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	rf.mu.Lock()

	lastSnapshot := rf.lastSnapshot()
	lastSnapshotIndex := lastSnapshot.Index
	// In theory, the upper level users know no more indices greater than the local committed index.
	// However, the Raft leader may or may not observe to this constraint under the hood.
	rf.assertf(0, index <= rf.lastApplied, "Invalid snapshot index: %v, lastAppliedIndex: %v, "+
		"lastSnapshot: %v\n", index, rf.lastApplied, lastSnapshotIndex)

	if index <= lastSnapshotIndex {
		rf.mu.Unlock()
		rf.logf(0, "Skip snapshot, index: %v is not greater than lastSnapshot: %v\n",
			index, lastSnapshotIndex)
		return
	}

	// update the snapshot index
	maxIndex := rf.lastLogEntry().Index
	rf.logs = append(rf.logs[:1], rf.logSlice(0, index+1, maxIndex+1)...)
	lastSnapshot.Index = index
	lastSnapshot.Term = rf.currentTerm

	lastPersistedIndex := rf.persistedIndex()
	rf.setPersistedIndex(maxIndex)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)

	rf.persister.SaveStateAndSnapshot(w.Bytes(), snapshot)
	rf.mu.Unlock()

	rf.logf(0, "User snapshot succeeded, persisted index modified from %v to %v, "+
		"snapshot index modified from %v to %v\n", lastPersistedIndex, maxIndex,
		lastSnapshotIndex, index)
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Rid uint64

	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Rid uint64

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

	termOK := rf.checkAndSetTerm(rid, args.Term, "RequestVote")
	reply.Term = rf.currentTerm
	if !termOK {
		return
	}

	if rf.votedFor == -1 {
		rf.votedFor = args.CandidateId
	}

	lastEntry := rf.lastLogEntry()
	if args.CandidateId == rf.votedFor {
		reply.VoteGranted = args.LastLogTerm > lastEntry.Term ||
			(args.LastLogTerm == lastEntry.Term && args.LastLogIndex >= lastEntry.Index)
	}
	if !rf.hasPinged {
		// We only think we're pinged by others when we successfully voted for somebody, because
		// this helps the cluster converge to a consensus quickly.
		rf.hasPinged = reply.VoteGranted
	}
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

	rf.mu.Lock()
	index = rf.lastLogEntry().Index + 1
	term = rf.currentTerm
	isLeader = rf.role == RAFT_LEADER
	if isLeader {
		rf.logs = append(rf.logs, LogEntry{Index: index, Term: term, Command: command})
		rf.persist(0)
	}
	rf.mu.Unlock()

	if isLeader {
		rf.logf(0, "Client start call on leader, index = %v, term = %v, cmd = %v\n",
			index, term, command)
		if atomic.AddInt32(&rf.hrtbtReqCnt, 1) <= HEARTBEAT_CHAN_SIZE {
			rf.hrtbtCh <- struct{}{}
		}
	}
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

func (rf *Raft) electionCheck(rid uint64) (int, bool) {
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
		rf.maxRcvHrtbtId = 0
	}
	return rf.currentTerm, launchElection
}

func (rf *Raft) voteForSelf(rid uint64, term int) bool {
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

func (rf *Raft) claimLeadership(rid uint64, term int) {
	hasBecomeLeader := false

	rf.mu.Lock()
	if term != rf.currentTerm {
		// some higher term server has already claimed the leadership
		rf.assertf(rid, rf.currentTerm > term, "There couldn't be a lower term in claimLeadership.\n")
		rf.logf(rid, "%v failed to claim the leadership due to finding out a higher term\n", rf.me)
	} else {
		rf.role = RAFT_LEADER
		hasBecomeLeader = true
		// reset the index arrays
		npeers := len(rf.peers)
		lastLogIndex := rf.lastLogEntry().Index
		for i := 0; i < npeers; i++ {
			if i == rf.me {
				continue
			}
			rf.nextIndex[i] = lastLogIndex + 1
			rf.matchIndex[i] = 0
		}
	}
	rf.mu.Unlock()

	if hasBecomeLeader {
		rf.logf(rid, "%v has become the leader and starts to send out heartbeats\n", rf.me)
		go rf.heartbeats(atomic.AddUint64(&rf.routineId, 1), term)
	}
}

func (rf *Raft) sendVoteRoutine(rid uint64, peer int, term int, ch chan VoteChannelMsg) {
	rf.mu.Lock()
	lastEntry := rf.lastLogEntry()
	curTerm := rf.currentTerm
	rf.mu.Unlock()

	if curTerm != term {
		rf.logf(rid, "Found term unmatched before sending vote request.\n")
		ch <- VoteChannelMsg{abortElection: true, voteGranted: false}
		return
	}

	args := RequestVoteArgs{
		Rid:          rid,
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: lastEntry.Index,
		LastLogTerm:  lastEntry.Term}
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(peer, &args, &reply)
	if !ok {
		rf.logf(rid, "Server %v's vote request to %v has failed.\n", rf.me, peer)
		ch <- VoteChannelMsg{abortElection: false, voteGranted: false}
		return
	}

	if reply.Term != term {
		rf.logf(rid, "collectVotes: %v found out it's no longer the leader when checking "+
			"reply's term = %v.\n", rf.me, reply.Term)
		rf.assertf(rid, reply.Term > term, "There couldn't be a lower term in the reply.\n")
		rf.refreshTerm(reply.Term)
		ch <- VoteChannelMsg{abortElection: true, voteGranted: false}
		// term has changed, we give up competing the election
		return
	}

	// the channel is buffered so it won't be blocked
	ch <- VoteChannelMsg{abortElection: false, voteGranted: reply.VoteGranted}
}

func (rf *Raft) collectVotes(rid uint64, term int) {
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

		go rf.sendVoteRoutine(atomic.AddUint64(&rf.routineId, 1), peer, term, ch)
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
func (rf *Raft) ticker(rid uint64) {
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
			go rf.collectVotes(atomic.AddUint64(&rf.routineId, 1), term)
		}
	}
}

type AppendEntryArgs struct {
	Rid  uint64
	Hbid uint64

	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry

	LeaderCommitIndex   int
	LeaderSnapshotIndex int
}

type AppendEntryReply struct {
	Rid uint64

	Term         int
	Success      bool
	NeedSnapshot bool
	Ignore       bool // if ignore this reply

	// info used for skipping unneccessary append entry RPCs
	// ``PrevTerm'' means the previous term of the conflicting term if it applies
	// ``MagicIndex'' means the last index of the previous term if the AppendEntry RPC fails, or
	// the local matchIndex if it succeeds
	PrevTerm   int
	MagicIndex int
}

// ASSERT:
// 1. this function should be protected by rf.mu
// 2. index > lastSnapshotIndex
//
// NOTE:
// The passed-in `index` and the `lastIndex` to be returned are logical indices.
// `lastSnapshotIndex` should be subtracted from them. The caller must make sure the
// `lastSnapshotIndex` value should be consistent between two peers before calling
// this method.
func (rf *Raft) binSearchPrevTerm(rid uint64, term int, index int) (prevTerm int, lastIndex int) {
	lastSnapshotIndex := rf.lastSnapshot().Index
	rf.assertf(rid, index != 0, "binSearchPrevTerm index must not be zero\n")
	rf.assertf(rid, index > lastSnapshotIndex, "Inconsistent index in binSearch. "+
		"index = %v, lastSnapshotIndex = %v\n", index, lastSnapshotIndex)
	prevTerm = 0
	lastIndex = 0
	left := 0
	right := index - lastSnapshotIndex
	var mid int

	for left <= right {
		mid = left + (right-left)/2
		if rf.logs[mid].Term != rf.logs[mid+1].Term {
			if rf.logs[mid+1].Term == term {
				prevTerm = rf.logs[mid].Term
				lastIndex = rf.logs[mid].Index
				return
			}
			rf.assertf(rid, rf.logs[mid+1].Term < term, "binSearchPrevTerm term out of order, "+
				"term not eaqual, mid entry: %v, mid+1 entry: %v\n", rf.logs[mid], rf.logs[mid+1])
			left = mid + 1
		} else if rf.logs[mid].Term == term {
			right = mid - 1
		} else {
			rf.assertf(rid, rf.logs[mid].Term < term, "binSearchPrevTerm term out of order, "+
				"term equal, mid entry: %v\n", rf.logs[mid])
			left = mid + 1
		}
	}

	// cuz we have a placeholder at index 0, the function must be able to find a way out by
	// going through the above logic
	rf.assertf(rid, false, "binSearchPrevTerm UNREACHABLE\n")
	return
}

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	rid := args.Rid
	reply.Rid = args.Rid
	reply.Success = false
	reply.NeedSnapshot = false
	reply.Ignore = false
	notifyApplier := false
	rf.logf(rid, "Receive AppendEntries request from %v.\n", args.LeaderId)

	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
		if notifyApplier {
			rf.applyCond.Signal()
		}
	}()

	rf.hasPinged = true

	termOK := rf.checkAndSetTerm(rid, args.Term, "AppendEntries")
	reply.Term = rf.currentTerm
	if !termOK {
		return
	}

	// heartbeat messages could be out of order
	if args.Hbid < rf.maxRcvHrtbtId {
		rf.logf(rid, "Received an old heartbeat id: %v, ignore it.\n", args.Hbid)
		reply.Ignore = true
		return
	}
	rf.assertf(rid, rf.maxRcvHrtbtId != args.Hbid, "There couldn't be duplicate hbid!\n")
	rf.maxRcvHrtbtId = args.Hbid

	lastSnapshotIndex := rf.lastSnapshot().Index
	if args.LeaderSnapshotIndex != lastSnapshotIndex {
		rf.logf(rid, "The follower has got an inconsistent snapshot. leader: %v, self: %v\n",
			args.LeaderSnapshotIndex, lastSnapshotIndex)
		reply.NeedSnapshot = true
		return
	}

	lastEntry := rf.lastLogEntry()
	leaderPrevIndex := args.PrevLogIndex
	if leaderPrevIndex > lastEntry.Index {
		rf.logf(rid, "The follower has not got enough log entries as the leader expected.\n")
		reply.PrevTerm = lastEntry.Term
		reply.MagicIndex = lastEntry.Index
		return
	}

	entry := rf.getLogEntry(rid, leaderPrevIndex)
	if entry.Term != args.PrevLogTerm {
		rf.logf(rid, "The follower has found conflicting term, entryTerm = %v\n", entry.Term)
		reply.PrevTerm, reply.MagicIndex = rf.binSearchPrevTerm(rid, entry.Term, leaderPrevIndex)
		return
	}

	if rf.persistedIndex() > leaderPrevIndex || len(args.Entries) > 0 {
		// we cannot append the entries directly, there may be overlapped ones
		if len(args.Entries) > 0 {
			rf.logs = append(rf.logSlice(rid, lastSnapshotIndex, leaderPrevIndex+1), args.Entries...)
		} else {
			rf.logs = rf.logSlice(rid, lastSnapshotIndex, leaderPrevIndex+1)
		}
		rf.persist(rid)
	}

	if args.LeaderCommitIndex > rf.commitIndex {
		rf.commitIndex = intmin(args.LeaderCommitIndex, rf.persistedIndex())
		notifyApplier = rf.commitIndex > rf.lastApplied && rf.commitIndex >= lastSnapshotIndex
	}

	rf.logf(rid, "AppendEntry succeeded, peer commit idx: %v, leader's one: %v, last idx: %v"+
		", lastSnapshotIndex = %v\n", rf.commitIndex, args.LeaderCommitIndex,
		rf.lastLogEntry().Index, lastSnapshotIndex)
	reply.Success = true
	reply.PrevTerm = 0
	reply.MagicIndex = rf.persistedIndex()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	rid := args.Rid
	rf.logf(rid, "Begin sendAppendEntries to %v, term = %v, leader = %v, prevLogIndex = %v, "+
		"prevLogTerm = %v, leaderCommit = %v, len(logEntries) = %v, hbid = %v\n", server, args.Term,
		args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommitIndex,
		len(args.Entries), args.Hbid)

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	if ok {
		rf.logf(rid, "End sendAppendEntries from %v, term = %v, success = %v, prevTerm = %v, "+
			"magicIndex = %v, need snapshot: %v, ignore: %v\n", server, reply.Term, reply.Success,
			reply.PrevTerm, reply.MagicIndex, reply.NeedSnapshot, reply.Ignore)
		rf.assertf(rid, rid == reply.Rid, "The Rid in args and reply must match. reply.Rid = %v\n",
			reply.Rid)
	}
	return ok
}

func (rf *Raft) constructAppEntryMsg(rid uint64, peer int, args *AppendEntryArgs) (isLeader bool,
	needSnapshot bool) {

	args.Entries = make([]LogEntry, 0)
	args.PrevLogIndex = 0
	args.PrevLogTerm = 0
	isLeader = true
	needSnapshot = false

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term != rf.currentTerm {
		rf.assertf(rid, rf.currentTerm > args.Term,
			"There couldn't be a lower term when constructing AppendEntryMsg.\n")
		rf.logf(rid, "Term unmatched when constructing AppendEntryMsg"+
			", argsTerm = %v, currentTerm = %v\n", args.Term, rf.currentTerm)
		isLeader = false
		return
	}

	nextIndex := rf.nextIndex[peer]
	lastEntry := rf.lastLogEntry()
	lastSnapshotIndex := rf.lastSnapshot().Index

	if nextIndex <= lastSnapshotIndex {
		// we can just send the snapshot directly
		needSnapshot = true
		rf.nextIndex[peer] = lastSnapshotIndex + 1
		nextIndex = lastSnapshotIndex + 1
	}

	if nextIndex != lastEntry.Index+1 {
		rf.assertf(rid, nextIndex <= lastEntry.Index,
			"The nextIndex = %v of peer %v couldn't be greater than the last index = %v, "+
				"lastSnapshotIndex = %v\n", nextIndex, peer, lastEntry.Index, lastSnapshotIndex)
		// todo: if the number of entries is too big, we might need to split them
		rf.logf(rid, "Leader appends entries, nextIndex = %v, last index = %v, "+
			"lastSnapshotIndex = %v\n", nextIndex, lastEntry.Index, lastSnapshotIndex)
		args.Entries = append(args.Entries, rf.logSlice(rid, nextIndex, lastEntry.Index+1)...)
	}

	prevEntry := rf.getLogEntry(rid, nextIndex-1)
	args.PrevLogIndex = prevEntry.Index
	args.PrevLogTerm = prevEntry.Term
	args.LeaderCommitIndex = rf.commitIndex
	args.LeaderSnapshotIndex = lastSnapshotIndex
	return
}

func (rf *Raft) handleHeartbeatReply(rid uint64, hbid uint64, peer int, term int, ch chan bool,
	args *AppendEntryArgs, reply *AppendEntryReply) {

	rf.assertf(rid, !reply.Ignore, "There couldn't be ignored replies!\n")
	rf.mu.Lock()
	if hbid < rf.lastHeartbeatId {
		// We use heartbeats id to skip a potential old reply even if there are inconsistent terms.
		rf.logf(rid, "heartbeat id (%v) is less than the latest one (%v), drop the reply\n",
			hbid, rf.lastHeartbeatId)
		rf.mu.Unlock()
		return
	}

	rf.lastHeartbeatId = hbid
	if reply.Term != term || term != rf.currentTerm {
		rf.logf(rid, "Heartbeats aborted. It's no longer the leader current term = %v "+
			"reply's term = %v, hb term = %v.\n", rf.currentTerm, reply.Term, term)
		if reply.Term != term {
			rf.assertf(rid, reply.Term > term, "There couldn't be a lower term in the hb reply.\n")
		} else {
			rf.assertf(rid, rf.currentTerm > term,
				"The currentTerm couldn't be less than the hb term.\n")
		}
		if rf.currentTerm < reply.Term {
			rf.updateTerm(reply.Term)
		}
		rf.mu.Unlock()
		// term has changed, we give up sending more heartbeats
		ch <- false
		return
	}

	curSnapshotIdx := rf.lastSnapshot().Index
	if args.LeaderSnapshotIndex != curSnapshotIdx {
		rf.assertf(rid, args.LeaderSnapshotIndex < curSnapshotIdx, "snapshot index out of"+
			" order, the one in args: %v, current one: %v\n", args.LeaderSnapshotIndex,
			curSnapshotIdx)
		// the snapshot has been updated when we're doing the RPC, we need to send the new one
		reply.NeedSnapshot = true
	}

	if reply.Success {
		// we can only decide what the matchedIndex is when we find where the peer's log
		// matches for us
		rf.matchIndex[peer] = reply.MagicIndex
		rf.nextIndex[peer] = reply.MagicIndex + 1
	} else if !reply.NeedSnapshot {
		peerPrevIndex := reply.MagicIndex
		localPrevTerm := rf.getLogEntry(rid, peerPrevIndex).Term
		if localPrevTerm != reply.PrevTerm {
			// still not the same, we can do another binary search at the leader side
			_, lastIndex := rf.binSearchPrevTerm(rid, localPrevTerm, peerPrevIndex)
			rf.nextIndex[peer] = lastIndex + 1
		} else {
			rf.nextIndex[peer] = peerPrevIndex + 1
		}
	}
	rf.mu.Unlock()
	ch <- true

	if reply.NeedSnapshot {
		go rf.sendSnapshot(atomic.AddUint64(&rf.routineId, 1), peer, term)
	}
}

func (rf *Raft) appendEntryRoutine(rid uint64, hbid uint64, peer int, term int, ch chan bool) {
	args := AppendEntryArgs{Rid: rid, Hbid: hbid, Term: term, LeaderId: rf.me}
	reply := AppendEntryReply{}
	ok := true

	isLeader, needSnapshot := rf.constructAppEntryMsg(rid, peer, &args)
	if needSnapshot {
		rf.assertf(rid, isLeader, "when needSnapshot=true, we must be the leader.\n")
		// we send the snapshot directly and then go on the heartbeat ping
		isLeader, ok = rf.sendSnapshot(rid, peer, term)
	}
	if !isLeader || !ok {
		ch <- isLeader
		return
	}

	timeBeforeCall := time.Now()
	ok = rf.sendAppendEntries(peer, &args, &reply)
	if time.Since(timeBeforeCall).Milliseconds() > HEARTBEAT_TIME_OUT || reply.Ignore {
		rf.logf(rid, "heartbeat reply from %v arrives too late, drop it\n", peer)
		return
	}

	if !ok {
		rf.logf(rid, "%v's sendAppendEntries to %v has failed.\n", rf.me, peer)
		// Although the rpc failed this time, we still don't give it up.
		ch <- true
		return
	}

	rf.handleHeartbeatReply(rid, hbid, peer, term, ch, &args, &reply)
}

func (rf *Raft) refreshLeaderInfo(rid uint64, term int) (int, bool) {
	npeers := len(rf.peers)
	matcharr := make([]int, npeers)
	notifyApplier := false

	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
		if notifyApplier {
			rf.applyCond.Signal()
		}
	}()

	if term != rf.currentTerm {
		// some higher term server has already claimed the leadership
		rf.assertf(rid, rf.currentTerm > term, "There couldn't be a lower term in refreshLeaderInfo.\n")
		rf.logf(rid, "%v found out it's no longer the leader before sending out heartbeats. "+
			"current term = %v\n", rf.me, rf.currentTerm)
		return rf.commitIndex, false
	}

	rf.assertf(rid, rf.role == RAFT_LEADER && rf.votedFor == rf.me,
		"The leader must be me at term %v\n", term)

	copiedLen := copy(matcharr, rf.matchIndex)
	rf.assertf(rid, copiedLen == npeers, "Length does not match when copying the matchIndex array.\n")

	sort.Slice(matcharr, func(i, j int) bool {
		return matcharr[i] > matcharr[j] // descending order
	})

	// get the value that is equal to or less than the majority's match index
	commitIndex := matcharr[npeers/2]
	if commitIndex > rf.commitIndex && rf.getLogEntry(rid, commitIndex).Term == term {
		rf.logf(rid, "leader change commitIndex from %v to %v, term: %v\n",
			rf.commitIndex, commitIndex, term)
		rf.commitIndex = commitIndex
	}
	// A SPEEDUP
	// Although the Raft paper requires the commitIndex be modified only when it's the leader's
	// current term, we can conclude that the log at a certain index must be committed without
	// regard to the term, if all the peers have their logs passed over the index. The trick is
	// indicated in section 5.4 of the paper.
	if matcharr[npeers-1] > rf.commitIndex {
		rf.logf(rid, "The min match index > commitIndex, change from %v to %v, term: %v\n",
			rf.commitIndex, commitIndex, term)
		rf.commitIndex = matcharr[npeers-1]
	}
	notifyApplier = rf.commitIndex > rf.lastApplied && rf.commitIndex >= rf.lastSnapshot().Index
	return rf.commitIndex, true
}

func (rf *Raft) heartbeats(rid uint64, term int) {
	chansz := (len(rf.peers) - 1) * (HEARTBEAT_CHAN_SIZE + 1)
	ch := make(chan bool, chansz)

	commitIndex, isLeader := rf.refreshLeaderInfo(rid, term)
	for !rf.killed() && isLeader {
		rf.logf(rid, "Term %v, commitIndex = %v, leader heartbeats...\n", term, commitIndex)
		heartbeatId := atomic.AddUint64(&rf.heartbeatCounter, 1)
		for peer := 0; peer < len(rf.peers); peer++ {
			if peer == rf.me {
				continue
			}

			go rf.appendEntryRoutine(atomic.AddUint64(&rf.routineId, 1), heartbeatId, peer, term, ch)
		}

		timer := time.After(time.Duration(HEARTBEAT_TIME_OUT) * time.Millisecond)
		keepWait := true
		for keepWait {
			select {
			case ok := <-ch:
				if !ok {
					return
				}
				commitIndex, isLeader = rf.refreshLeaderInfo(rid, term)
				// if it's still the leader, we keep waiting
				keepWait = isLeader
			case <-rf.hrtbtCh:
				reqcnt := atomic.AddInt32(&rf.hrtbtReqCnt, -1)
				rf.assertf(rid, reqcnt >= 0, "Negative heartbeat request count!\n")
				keepWait = false
			case <-timer:
				keepWait = false
			}
		}
	}
}

func (rf *Raft) getNextApplyInfo(rid uint64, msg *ApplyMsg) (commitIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for rf.commitIndex <= rf.lastApplied || rf.commitIndex < rf.lastSnapshot().Index {
		rf.applyCond.Wait()
	}

	*msg = ApplyMsg{} // zero out the msg
	snapshot := rf.lastSnapshot()
	commitIndex = rf.commitIndex
	if rf.lastApplied < snapshot.Index {
		msg.SnapshotValid = true
		msg.Snapshot = rf.persister.ReadSnapshot()
		msg.SnapshotTerm = snapshot.Term
		msg.SnapshotIndex = snapshot.Index
		rf.lastApplied = snapshot.Index
		return
	}

	// ASSERT
	// 1. commitIndex <= lastEntry.Index;
	// 2. commitIndex > rf.lastApplied
	rf.lastApplied++
	msg.CommandIndex = rf.lastApplied
	entry := rf.getLogEntry(rid, msg.CommandIndex)
	msg.Command = entry.Command
	rf.assertf(rid, entry.Index == msg.CommandIndex, "Found inconsistent index in Applier. "+
		"entry's index: %v, cmdIndex: %v\n", entry.Index, msg.CommandIndex)
	msg.CommandValid = true
	return
}

func (rf *Raft) applyRoutine(rid uint64) {
	for !rf.killed() {
		msg := ApplyMsg{}
		commitIndex := rf.getNextApplyInfo(rid, &msg)
		if msg.SnapshotValid {
			rf.logf(rid, "Applying snapshot: idx:%v, term:%v\n", msg.SnapshotIndex, msg.SnapshotTerm)
		} else {
			rf.assertf(rid, msg.Command != nil,
				"The command must not be nil when we have something to apply\n")
			rf.logf(rid, "Applier got next apply info, cmdIndex = %v, commitIndex = %v, command = %v\n",
				msg.CommandIndex, commitIndex, msg.Command)
		}
		rf.applyCh <- msg
	}
}

type SendSnapshotArgs struct {
	Rid uint64

	Term     int
	LeaderId int

	// Unlink the message struct proposed in figure 13 of the Raft paper, we just send the
	// entire snapshot as a whole.
	SnapshotIndex int
	SnapshotTerm  int
	Data          []byte
}

type SendSnapshotReply struct {
	Rid uint64

	Term    int
	Success bool
}

func (rf *Raft) InstallSnapshot(args *SendSnapshotArgs, reply *SendSnapshotReply) {
	rid := args.Rid
	reply.Rid = rid
	reply.Success = false
	notifyApplier := false

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
		if notifyApplier {
			rf.applyCond.Signal()
		}
	}()

	rf.hasPinged = true

	termOK := rf.checkAndSetTerm(rid, args.Term, "InstallSnapshot")
	reply.Term = rf.currentTerm
	if !termOK {
		return
	}

	localSnapshot := rf.lastSnapshot()
	if args.SnapshotIndex <= localSnapshot.Index {
		rf.logf(rid, "Skip receiving snapshot, the requested index: %v <= lastSnapshot: %v\n",
			args.SnapshotIndex, localSnapshot.Index)
		return
	}

	// update the snapshot index
	maxIndex := rf.lastLogEntry().Index
	if maxIndex > args.SnapshotIndex {
		rf.logs = append(rf.logs[:1], rf.logSlice(0, args.SnapshotIndex+1, maxIndex+1)...)
	} else {
		rf.logs = rf.logs[:1]
		maxIndex = args.SnapshotIndex
	}
	localSnapshot.Index = args.SnapshotIndex
	localSnapshot.Term = args.SnapshotTerm

	// update the persisted index
	rf.setPersistedIndex(maxIndex)

	// update the commit index, cuz the snapshot index <= the leader's commit index
	if rf.commitIndex < args.SnapshotIndex {
		rf.commitIndex = args.SnapshotIndex
		notifyApplier = rf.commitIndex > rf.lastApplied
	}

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)

	rf.persister.SaveStateAndSnapshot(w.Bytes(), args.Data)
	reply.Success = true
}

func (rf *Raft) sendSnapshot(rid uint64, peer int, term int) (isLeader bool, ok bool) {
	args := SendSnapshotArgs{Rid: rid, LeaderId: rf.me}
	reply := SendSnapshotReply{}
	isLeader = true
	ok = false

	rf.mu.Lock()

	curTerm := rf.currentTerm
	if term != curTerm {
		rf.logf(rid, "It's no longer the leader before send snapshot.\n")
		rf.assertf(rid, term < curTerm, "term: %v cannot be greater than the current term: %v\n",
			term, curTerm)
		rf.mu.Unlock()
		isLeader = false
		return
	}
	args.Term = curTerm
	args.Data = rf.persister.ReadSnapshot()
	snapshot := *rf.lastSnapshot()

	rf.mu.Unlock()

	args.SnapshotIndex = snapshot.Index
	args.SnapshotTerm = snapshot.Term

	rf.logf(rid, "Send snapshot enter: CurrentTerm: %v, SIndex: %v, STerm: %v\n",
		args.Term, args.SnapshotIndex, args.SnapshotTerm)
	ok = rf.peers[peer].Call("Raft.InstallSnapshot", &args, &reply)
	if !ok {
		rf.logf(rid, "Send snapshot to %v failed.\n", peer)
		return
	}
	rf.logf(rid, "Send snapshot exit: PeerTerm: %v, Success: %v\n", reply.Term, reply.Success)
	rf.assertf(rid, args.Rid == reply.Rid, "Rid unmatches in replay: %v\n", reply.Rid)

	if reply.Term != term {
		rf.logf(rid, "sendSnapshot found out it's no longer the leader when checking "+
			"reply's term = %v.\n", reply.Term)
		rf.assertf(rid, reply.Term > term, "There couldn't be a lower term in the reply.\n")
		rf.refreshTerm(reply.Term)
		isLeader = false
	}
	// We wouldn't do anything here even if the follower rejected to receive the snapshot. The
	// later sending heartbeats message, if it's still the leader, will figure it out.
	return
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

	rf.applyCond = sync.NewCond(&rf.mu)

	rf.routineId = uint64(me) << 32
	rf.heartbeatCounter = 0
	rf.lastHeartbeatId = 0
	rf.maxRcvHrtbtId = 0

	rf.commitIndex = 0
	rf.lastApplied = 0

	npeers := len(peers)
	rf.nextIndex = make([]int, npeers)
	// NOTE: rf.matchIndex[rf.me] makes sense on all servers, which means the max index replicated
	// on the local server
	rf.matchIndex = make([]int, npeers)
	for i := 0; i < npeers; i++ {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}

	rf.role = RAFT_FOLLOWER
	rf.hasPinged = false

	_, rf.enableLog = os.LookupEnv("LOG_VERBOSE_M6824")

	rf.applyCh = applyCh
	rf.hrtbtCh = make(chan struct{}, HEARTBEAT_CHAN_SIZE)
	rf.hrtbtReqCnt = 0

	// note that the last snapshot index and term are hidden in logs[0]
	if persister.RaftStateSize() > 0 {
		// initialize from state persisted before a crash
		rf.readPersist(persister.ReadRaftState())
	} else {
		// start from scratch
		rf.currentTerm = 0
		rf.votedFor = -1
		// We reserve index 0 to prevent boundary checking when doing binary search. What's more,
		// we use the log entry as our special `lastSnapshot` entry.
		rf.logs = make([]LogEntry, 1)
		rf.logs[0] = LogEntry{Index: 0, Term: 0, Command: nil}
	}

	rand.Seed(time.Now().UnixNano())

	go rf.ticker(atomic.AddUint64(&rf.routineId, 1))

	go rf.applyRoutine(atomic.AddUint64(&rf.routineId, 1))

	return rf
}
