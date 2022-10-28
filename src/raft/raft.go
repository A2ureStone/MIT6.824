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
	"6.824/labgob"
	"bytes"
	//"crypto/rand"
	"math/rand"
	"sort"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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
	Command interface{}
	Term    int
}

type AppendEntryArgs struct {
	Term         int        // leader's term
	LeaderId     int        // follower use this to redirect clients
	PrevLogIndex int        // index of log entry immediately preceding
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store(empty for heartbeat)
	LeaderCommit int        // leader's commitIndex
}

type AppendEntryReply struct {
	Term    int  // current term for follower, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm

	XTerm  int // term of the conflict entry in follower, -1 represents no conflict entry
	XIndex int // index of the conflict entry in follower
	XLen   int // len of the follower log
}

type InstallSnapShotArgs struct {
	Term              int    // leader's term
	LeaderId          int    // follower use this to redirect clients
	LastIncludedIndex int    // index of log entry immediately preceding
	LastIncludedTerm  int    // term of prevLogIndex entry
	Data              []byte // log entries to store(empty for heartbeat)
}

type InstallSnapShotReply struct {
	Term int // reply term for update
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// lab2a
	// persistent state
	currTerm int        // current term
	votedFor int        // vote for which server
	log      []LogEntry // log entries, first index is 1

	// volatile state
	commitIndex int // index of the highest log entry known to be committed
	lastApplied int // index of the highest log entry applied to state machine

	// volatile state for leaders
	nextIndex  []int // for each server, index of the next log entry to send to that server
	matchIndex []int // for each server, index of the highest log entry known to be replicated on server

	// used for implement leader election
	electionTimeout time.Duration // used for starting a new election
	lastReceiveTime time.Time     // record the time since received RPC from candidate or leader
	voteNums        int           // nums for follower vote
	//reachableNums    int           // nums for reachable server, when start election, set to zero
	state            int           // the server state, 0 for follower, 1 for candidate, 2 for leader
	heartbeatTimeout time.Duration // used for leader to send heartbeat
	lastSendTime     time.Time     // record the time since send heartbeat message

	// lab2b
	updateLastApplied *sync.Cond // conditional variable for update lastApplied
	testMsg           chan ApplyMsg

	// lab2d
	SnapshotIndex int // record index of snapshot last entry
	SnapshotTerm  int // record term of snapshot last entry
	//applyTurnOn   bool // use to turn off apply channel when snapshot
	//applyChange   *sync.Cond
	snapTurnOn bool // use to turn off apply channel when snapshot
	snapChange *sync.Cond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	rf.grabLock()
	defer rf.releaseLock()

	term := rf.currTerm
	isLeader := rf.state == 2
	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.currTerm) != nil ||
		e.Encode(rf.votedFor) != nil ||
		e.Encode(rf.SnapshotTerm) != nil ||
		e.Encode(rf.SnapshotIndex) != nil ||
		e.Encode(rf.log) != nil {
		DebugPrintf(dPersist, "Encode Fail")
	}
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var voteFor int
	var snapshotTerm int
	var snapshotIndex int
	var log []LogEntry
	if err := d.Decode(&term); err != nil {
		DebugPrintf(dPersist, "Decode Fail, %v", err)
		return
	}
	if err := d.Decode(&voteFor); err != nil {
		DebugPrintf(dPersist, "Decode Fail, %v", err)
		return
	}
	if err := d.Decode(&snapshotTerm); err != nil {
		DebugPrintf(dPersist, "Decode Fail, %v", err)
		return
	}
	if err := d.Decode(&snapshotIndex); err != nil {
		DebugPrintf(dPersist, "Decode Fail, %v", err)
		return
	}
	if err := d.Decode(&log); err != nil {
		DebugPrintf(dPersist, "Decode Fail, %v", err)
		return
	}
	rf.currTerm = term
	rf.votedFor = voteFor
	rf.SnapshotTerm = snapshotTerm
	rf.SnapshotIndex = snapshotIndex
	rf.log = log
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	rf.grabLock()
	defer rf.releaseLock()

	// is an old snapshot
	if lastIncludedIndex <= rf.SnapshotIndex {
		DebugPrintf(dSnap, "S%v(T%v) Refuse Snapshot index%v(itself %v)", rf.me, rf.currTerm, lastIncludedIndex, rf.SnapshotIndex)
		return false
	}

	for !rf.snapTurnOn {
		rf.snapChange.Wait()
	}

	// TODO discard log entry
	// change volatile state
	if lastIncludedIndex >= rf.logSize() || rf.logContent(lastIncludedIndex).Term != lastIncludedTerm {
		// discard the entire log
		rf.log = make([]LogEntry, 0)
		DebugPrintf(dSnap, "S%v(T%v) Discard Entire Log", rf.me, rf.currTerm)
	} else {
		rf.log = rf.log[lastIncludedIndex+1-rf.SnapshotIndex-1:]
		DebugPrintf(dSnap, "S%v(T%v) Truncate Log", rf.me, rf.currTerm)
	}
	rf.SnapshotTerm = lastIncludedTerm
	rf.SnapshotIndex = lastIncludedIndex
	DebugPrintf(dSnap, "S%v(T%v) Snapshot At Index%v", rf.me, rf.currTerm, lastIncludedIndex)

	// at least update commit index if necessary
	if rf.commitIndex < lastIncludedIndex {
		rf.commitIndex = lastIncludedIndex
		DebugPrintf(dSnap, "S%v(T%v) Commit To %v For Snapshot", rf.me, rf.currTerm, rf.commitIndex)
		rf.updateLastApplied.Broadcast()
	}

	// do persist
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.currTerm) != nil ||
		e.Encode(rf.votedFor) != nil ||
		e.Encode(rf.SnapshotTerm) != nil ||
		e.Encode(rf.SnapshotIndex) != nil ||
		e.Encode(rf.log) != nil {
		DebugPrintf(dPersist, "Encode Fail")
		return false
	}
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, snapshot)

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	// safely truncate log
	rf.grabLock()
	defer rf.releaseLock()

	DebugPrintf(dSnap, "S%v(T%v) preparing for snapshot at %v, current snapshot index %v, log len %v", rf.me, rf.currTerm, index, rf.SnapshotIndex, len(rf.log))
	// change volatile state
	rf.SnapshotTerm = rf.logContent(index).Term
	rf.log = rf.log[index+1-rf.SnapshotIndex-1:]
	rf.SnapshotIndex = index
	DebugPrintf(dSnap, "S%v(T%v) snapshot at index%v", rf.me, rf.currTerm, index)

	// do persist
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.currTerm) != nil ||
		e.Encode(rf.votedFor) != nil ||
		e.Encode(rf.SnapshotTerm) != nil ||
		e.Encode(rf.SnapshotIndex) != nil ||
		e.Encode(rf.log) != nil {
		DebugPrintf(dPersist, "Encode Fail")
	}
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, snapshot)
}

func (rf *Raft) logSize() int {
	return rf.SnapshotIndex + 1 + len(rf.log)
}

// call this function must logic index > SnapshotIndex
func (rf *Raft) logContent(index int) *LogEntry {
	return &rf.log[index-rf.SnapshotIndex-1]
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	// lab2a
	Term        int // candidate term
	CandidateId int // candidate id for requesting vote

	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	// lab2a
	Term        int  // request server term. used for candidate to update itself
	VoteGranted bool // whether candidate received vote
}

func (rf *Raft) grabLock() {
	//DebugPrintf(dTrace, "S%v is grabbing lock\n", rf.me)
	rf.mu.Lock()
	//DebugPrintf(dTrace, "S%v gets lock\n", rf.me)
}

func (rf *Raft) releaseLock() {
	rf.mu.Unlock()
	//DebugPrintf(dTrace, "S%v release lock\n", rf.me)
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// send request vote in ticker?
	// if is elected, fire a go routine to send heartbeat periodically

	// in leader state, may receive a request vote
	rf.grabLock()
	defer rf.releaseLock()
	reply.VoteGranted = false
	reply.Term = rf.currTerm
	if args.Term < rf.currTerm {
		DebugPrintf(dVote, "S%v(T%v) Ignore RequestVote From S%v(T%v)", rf.me, rf.currTerm, args.CandidateId, args.Term)
		return
	} else if rf.currTerm < args.Term {
		DebugPrintf(dVote, "S%v(T%v) Receive RequestVote From S%v(T%v), Term Changed", rf.me, rf.currTerm, args.CandidateId, args.Term)
		rf.toFollower(args.Term)
		reply.Term = args.Term
	}
	DebugPrintf(dVote, "S%v(T%v) Receive Vote Request from S%v(T%v), log len%v, logic log len%v, snapshot index%v, candidate[%v %v]", rf.me, rf.currTerm, args.CandidateId, args.Term, len(rf.log), rf.logSize(), rf.SnapshotIndex, args.LastLogIndex, args.LastLogTerm)
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		last_idx := rf.SnapshotIndex
		last_log_term := rf.SnapshotTerm
		if rf.logSize()-1 > rf.SnapshotIndex {
			last_idx = rf.logSize() - 1
			last_log_term = rf.logContent(last_idx).Term
		}
		DebugPrintf(dVote, "S%v(T%v) Voting to S%v(at T%v), [%v %v] - [%v %v]", rf.me, rf.currTerm, args.CandidateId, args.Term, last_idx, last_log_term, args.LastLogIndex, args.LastLogTerm)
		if last_log_term == args.LastLogTerm && last_idx > args.LastLogIndex {
			//DPrintf("server %v receive request vote, reject for same-term log index inconsistency\n", rf.me)
			return
		}
		if last_log_term > args.LastLogTerm {
			//DPrintf("server %v receive request vote, reject for my log term %v, receive log term %v\n", rf.me, last_log_term, args.LastLogIndex)
			return
		}

		// reset receive time for RequestVote RPC when granting vote
		rf.freshReceiveTime()
		// need change in later lab
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persist()
		DebugPrintf(dVote, "S%v(T%v) Granting Vote to S%v(at T%v)", rf.me, rf.currTerm, args.CandidateId, args.Term)
	}
}

//
// rpc handler for AppendEntry
//
func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	// for now, only to reset timeout
	rf.grabLock()
	defer rf.releaseLock()
	reply.Term = rf.currTerm
	reply.Success = false
	DebugPrintf(dLog2, "S%v(T%v) <- S%v(T%v), Receive AppendEntry", rf.me, rf.currTerm, args.LeaderId, args.Term)
	if rf.currTerm < args.Term {
		// convert to follower
		DebugPrintf(dLog2, "S%v(T%v) <- S%v(T%v), Changed Term", rf.me, rf.currTerm, args.LeaderId, args.Term)
		rf.toFollower(args.Term)
	} else if args.Term < rf.currTerm {
		DebugPrintf(dLog2, "S%v(T%v) <- S%v(T%v), False For Higher Term", rf.me, rf.currTerm, args.LeaderId, args.Term)
		// return false
		return
	}
	// check for candidate, receive AppendEntry RPC from leader
	// even for candidate change to follower, it can still reply this append entry
	if rf.state == 1 {
		DebugPrintf(dLog2, "S%v(T%v) <- S%v(T%v), Changed To Follower From Candidate", rf.me, rf.currTerm, args.LeaderId, args.Term)
		// here is the term correctly??
		rf.toFollower(args.Term)
	}
	// reset receive time for AppendEntry RPC
	rf.freshReceiveTime()
	// only check when index > snapshot index, <= we must accept !
	if args.PrevLogIndex > rf.SnapshotIndex {
		log_sz := rf.logSize()
		if log_sz <= args.PrevLogIndex || rf.logContent(args.PrevLogIndex).Term != args.PrevLogTerm {
			// here we will return false
			// set reason for leader to fast backup
			if log_sz <= args.PrevLogIndex {
				reply.XTerm = -1
				reply.XLen = log_sz
			} else {
				reply.XTerm = rf.logContent(args.PrevLogIndex).Term
				idx := args.PrevLogIndex
				for ; idx > rf.SnapshotIndex && rf.logContent(idx).Term == reply.XTerm; idx-- {
				}
				// XIndex is the first index whose term is conflict entry term
				// what will happen if we reach snapshot index, and snapshot has required term?
				// is ok, because when leader set nextIndex to XIndex, server will accept it, it is harmless
				reply.XIndex = idx + 1
			}
			DebugPrintf(dLog2, "S%v(T%v) <- S%v(T%v), False For Log Inconsistence", rf.me, rf.currTerm, args.LeaderId, args.Term)
			return
		}
	} else {
		// case for RPC reorder, we may end up with receiving a rpc whose PrevLogIndex in the snapshot
		// truncate args entry because in commit entry, same index means same command
		if rf.SnapshotIndex+1-args.PrevLogIndex-1 < len(args.Entries) {
			args.Entries = args.Entries[rf.SnapshotIndex+1-args.PrevLogIndex-1:]
			args.PrevLogIndex = rf.SnapshotIndex
		} else {
			// no need to append entry
			reply.Success = true
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = rf.logSize() - 1
				if rf.commitIndex > args.LeaderCommit {
					rf.commitIndex = args.LeaderCommit
				}
				DebugPrintf(dCommit, "S%v(T%v) <- S%v(T%v), Commit to %v", rf.me, rf.currTerm, args.LeaderId, args.Term, rf.commitIndex)
				rf.updateLastApplied.Broadcast()
			}
			return
		}
	}

	// TODO may need truncate arg log for start in snapshot
	// no need to truncate, because here PrevLogIndex must at least Snapshot Index
	reply.Success = true
	if len(args.Entries) > 0 {
		end := rf.logSize() - 1
		if args.PrevLogIndex+len(args.Entries) < end {
			end = args.PrevLogIndex + len(args.Entries)
		}
		check_match := args.PrevLogIndex + 1
		DebugPrintf(dLog2, "S%v(T%v) <- S%v(T%v), Appending Log, check_match%v, snapshot index%v", rf.me, rf.currTerm, args.LeaderId, args.Term, check_match, rf.SnapshotIndex)
		for ; check_match <= end; check_match++ {
			arg_idx := check_match - args.PrevLogIndex - 1
			if rf.logContent(check_match).Term != args.Entries[arg_idx].Term {
				rf.log = rf.log[:check_match-rf.SnapshotIndex-1]
				//rf.log = rf.log[:check_match]
				break
			}
		}
		remain_off := check_match - args.PrevLogIndex - 1
		rf.log = append(rf.log, args.Entries[remain_off:]...)
		// when truncate, remain_of must > 0
		DebugPrintf(dLog2, "S%v(T%v) <- S%v(T%v), Append Log Len: %v", rf.me, rf.currTerm, args.LeaderId, args.Term, len(args.Entries)-remain_off)
		//if len(rf.log)-remain_off > 0 {
		//	rf.persist()
		//}
		if len(args.Entries)-remain_off > 0 {
			rf.persist()
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = rf.logSize() - 1
		if rf.commitIndex > args.LeaderCommit {
			rf.commitIndex = args.LeaderCommit
		}
		DebugPrintf(dCommit, "S%v(T%v) <- S%v(T%v), Commit to %v", rf.me, rf.currTerm, args.LeaderId, args.Term, rf.commitIndex)
		rf.updateLastApplied.Broadcast()
	}
}

//
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
//
func (rf *Raft) sendRequestVote(term int, server int) {
	args := RequestVoteArgs{}
	reply := RequestVoteReply{}
	//DPrintf("server %v sending request vote RPC to %v, turn into candidate\n", rf.me, server)
	// whether we need this??
	rf.grabLock()
	if rf.state != 1 {
		// not candidate
		//DPrintf("server %v sending request vote RPC to %v, but canceled because of state turn into:%v\n", rf.me, server, rf.state)
		rf.releaseLock()
		return
	}
	// leave log content nil
	args.Term = term
	args.CandidateId = rf.me
	//args.LastLogIndex = len(rf.log) - 1
	args.LastLogIndex = rf.logSize() - 1
	args.LastLogTerm = rf.SnapshotTerm
	// check for last entry is out of snapshot
	if args.LastLogIndex > rf.SnapshotIndex {
		//args.LastLogTerm = rf.log[args.LastLogIndex].Term
		args.LastLogTerm = rf.logContent(args.LastLogIndex).Term
	}
	rf.releaseLock()

	ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply)
	if !ok {
		//DPrintf("raft.go::ticker() send request vote to %v fail\n", server)
		return
	}

	rf.grabLock()
	defer rf.releaseLock()
	// case for time-out again
	if rf.currTerm != term {
		DebugPrintf(dVote, "S%v(T%v) <- S%v(T%v), Itself Term Changed", rf.me, rf.currTerm, server, reply.Term)
		return
	}
	// case for converting to a follower or being a leader
	if rf.state != 1 {
		DebugPrintf(dVote, "S%v(T%v) <- S%v(T%v), Is Not Candidate", rf.me, rf.currTerm, server, reply.Term)
		return
	}
	// check for reply term
	if rf.currTerm < reply.Term {
		DebugPrintf(dVote, "S%v(T%v) <- S%v(T%v), Changed To Higher Term", rf.me, rf.currTerm, server, reply.Term)
		rf.toFollower(reply.Term)
		return
	} else if rf.currTerm > reply.Term {
		// case for receive response from previous
		return
	}

	// do nothing, if already get majority votes
	if reply.VoteGranted {
		rf.voteNums++
		DebugPrintf(dVote, "S%v(T%v) <- S%v(T%v), Got Vote", rf.me, rf.currTerm, server, reply.Term)
	}
	majority := len(rf.peers) / 2
	if len(rf.peers)%2 != 0 {
		majority++
	}
	if rf.voteNums >= majority { // become leader
		DebugPrintf(dVote, "S%v(T%v) <- S%v(T%v), Change To Leader", rf.me, rf.currTerm, server, reply.Term)
		rf.state = 2
		// fire a go routine to send heartbeat
		rf.lastSendTime = time.Now()
		// initialize for new leader
		rf.nextIndex = make([]int, len(rf.peers))
		//sz := len(rf.log)
		sz := rf.logSize()
		for idx := range rf.nextIndex {
			rf.nextIndex[idx] = sz
		}
		rf.matchIndex = make([]int, len(rf.peers))
		go rf.sendHeartBeat()
	}
}

func (rf *Raft) sendAppendEntry(term int, server int) {
	rf.grabLock()
	if rf.state != 2 {
		// not a leader again
		rf.releaseLock()
		return
	}
	if rf.nextIndex[server] <= rf.SnapshotIndex {
		// send install snapshot RPC
		go rf.sendSnapShot(term, server)
		rf.releaseLock()
		return
	}
	pre_idx := rf.nextIndex[server] - 1
	entry := make([]LogEntry, 0)
	// if we have something to send, go and send it
	entry = append(entry, rf.log[pre_idx+1-rf.SnapshotIndex-1:]...)
	args := AppendEntryArgs{Term: term, LeaderId: rf.me, PrevLogIndex: pre_idx,
		Entries: entry, LeaderCommit: rf.commitIndex}
	if pre_idx == rf.SnapshotIndex {
		args.PrevLogTerm = rf.SnapshotTerm
	} else {
		args.PrevLogTerm = rf.logContent(pre_idx).Term
	}
	reply := AppendEntryReply{}
	//DebugPrintf(dLog, "S%v(T%v) -> S%v, Sending PLI: %v PLT: %v N: %v LC: %v - %v", rf.me, rf.currTerm, server, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), args.LeaderCommit, args.Entries)
	// here use term not currTerm is more readable
	DebugPrintf(dLog, "S%v(T%v) -> S%v, Sending PLI: %v PLT: %v N: %v LC: %v - [%v to %v)", rf.me, rf.currTerm, server, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), args.LeaderCommit, pre_idx+1, rf.logSize())
	rf.releaseLock()
	ok := rf.peers[server].Call("Raft.AppendEntry", &args, &reply)
	if !ok {
		//DPrintf("server %v send AppendEntry failed\n", server)
		return
	}
	//// case for rpc reorder
	//if reply.PrevLogTerm != args.PrevLogTerm && reply.PrevLogIndex != args.PrevLogIndex {
	//	return
	//}
	rf.grabLock()
	defer rf.releaseLock()
	// here if term changed, we are no longer a leader
	if rf.currTerm != term {
		//DebugPrintf(dVote, "S%v(T%v) <- S%v(T%v), Itself Term Changed", rf.me, rf.currTerm, server, reply.Term)
		return
	}
	if rf.state != 2 {
		DebugPrintf(dLog, "S%v(T%v) <- S%v(T%v), Not A Leader", rf.me, rf.currTerm, server, reply.Term)
		return
	}
	if rf.currTerm < reply.Term {
		DebugPrintf(dLog, "S%v(T%v) <- S%v(T%v), Change To Higher Term", rf.me, rf.currTerm, server, reply.Term)
		rf.toFollower(reply.Term)
		return
	}
	if !reply.Success {
		// we need update when use snapshot??
		//rf.nextIndex[server]--
		if reply.XTerm == -1 {
			rf.nextIndex[server] = reply.XLen
		} else {
			// check for leader have XTerm
			//DebugPrintf("server %v is a leader, getting appendentry reply from %v, reply fail, check for XTerm, log:%v\n", rf.me, server, len(entry))
			//rf.nextIndex[server]--
			// minus one case for no log, even have log, it will not affect result because follower reply fail
			idx := rf.nextIndex[server] - 1
			//for ; idx > 0 && rf.log[idx].Term >= reply.XTerm; idx-- {
			//	if rf.log[idx].Term == reply.XTerm {
			//		break
			//	}
			//}
			// idx must > snapshot index
			for ; idx > rf.SnapshotIndex && rf.logContent(idx).Term >= reply.XTerm; idx-- {
				if rf.logContent(idx).Term == reply.XTerm {
					break
				}
			}
			if idx > rf.SnapshotIndex && rf.logContent(idx).Term == reply.XTerm {
				// the first entry has this term
				rf.nextIndex[server] = idx + 1
				//rf.nextIndex[server]--
			} else {
				if reply.XIndex > rf.SnapshotIndex {
					rf.nextIndex[server] = reply.XIndex
				} else {
					rf.nextIndex[server] = rf.SnapshotIndex
				}
			}
		}
		DebugPrintf(dLog, "S%v(T%v) <- S%v(T%v), Reply False For Log Inconsistency", rf.me, rf.currTerm, server, reply.Term)
	} else {
		end := pre_idx + len(entry) + 1
		// update only new
		if end > rf.nextIndex[server] {
			DebugPrintf(dLog, "S%v(T%v) <- S%v(T%v), NextIndex: %v to %v", rf.me, rf.currTerm, server, reply.Term, rf.nextIndex[server], end)
			rf.nextIndex[server] = end
		}
		if end-1 > rf.matchIndex[server] {
			// here if clause may not be required
			DebugPrintf(dLog, "S%v(T%v) <- S%v(T%v), MatchIndex: %v to %v", rf.me, rf.currTerm, server, reply.Term, rf.matchIndex[server], end-1)
			rf.matchIndex[server] = end - 1
		}
		// log matching property
		// a majority match
		//rf.matchIndex[rf.me] = len(rf.log) - 1
		rf.matchIndex[rf.me] = rf.logSize() - 1
		sort_lst := make([]int, len(rf.peers))
		copy(sort_lst, rf.matchIndex)
		sort.Ints(sort_lst)
		majority := len(rf.peers) / 2
		// here ok, because rf.peers is odd
		min_match := sort_lst[majority]
		// only commit current term in case for duplicate commit
		// min_match must out of snapshot
		if min_match > rf.commitIndex && rf.logContent(min_match).Term == rf.currTerm {
			DebugPrintf(dCommit, "S%v(T%v) <- S%v(T%v), Commit To %v", rf.me, rf.currTerm, server, reply.Term, min_match)
			rf.commitIndex = min_match
			rf.updateLastApplied.Broadcast()
			//og_idx := rf.commitIndex + 1
			//for idx := og_idx; idx <= rf.commitIndex; idx++ {
			//	DebugPrintf(dCommit, "S%v(T%v) Send Msg To Channel", rf.me, rf.currTerm)
			//	rf.testMsg <- ApplyMsg{CommandValid: true, CommandIndex: idx, Command: rf.logContent(idx).Command}
			//	DebugPrintf(dCommit, "S%v(T%v) End Sending Msg To Channel", rf.me, rf.currTerm)
			//}
		}
	}
}

func (rf *Raft) sendSnapShot(term int, server int) {
	rf.grabLock()
	if rf.state != 2 {
		// not a leader again
		rf.releaseLock()
		return
	}
	DebugPrintf(dSnap, "S%v(T%v) -> S%v, Send Snapshot", rf.me, rf.currTerm, server)
	data := rf.persister.ReadSnapshot()
	args := InstallSnapShotArgs{term, rf.me, rf.SnapshotIndex, rf.SnapshotTerm, data}
	reply := InstallSnapShotReply{}
	rf.releaseLock()

	ok := rf.peers[server].Call("Raft.ReceiveSnapShot", &args, &reply)
	if !ok {
		return
	}
	rf.grabLock()
	defer rf.releaseLock()
	if rf.currTerm != term {
		//DebugPrintf(dVote, "S%v(T%v) <- S%v(T%v), Itself Term Changed", rf.me, rf.currTerm, server, reply.Term)
		return
	}
	if rf.state != 2 {
		//DebugPrintf(dSnap, "S%v(T%v) <- S%v(T%v), Not A Leader", rf.me, rf.currTerm, server, reply.Term)
		return
	}
	if rf.currTerm < reply.Term {
		//DebugPrintf(dSnap, "S%v(T%v) <- S%v(T%v), Change To Higher Term", rf.me, rf.currTerm, server, reply.Term)
		rf.toFollower(reply.Term)
		return
	}
	// update nextIndex and matchIndex, no need to commit because snapshot already commit
	rf.nextIndex[server] = args.LastIncludedIndex + 1
	rf.matchIndex[server] = args.LastIncludedIndex
}

func (rf *Raft) ReceiveSnapShot(args *InstallSnapShotArgs, reply *InstallSnapShotReply) {
	//DebugPrintf(dSnap, "S%v(T%v) <- S%v(T%v), Receive SnapShot", rf.me, rf.currTerm, args.LeaderId, args.Term)
	//return
	rf.grabLock()
	reply.Term = rf.currTerm
	DebugPrintf(dSnap, "S%v(T%v) <- S%v(T%v), Receive SnapShot", rf.me, rf.currTerm, args.LeaderId, args.Term)
	if args.Term < rf.currTerm {
		DebugPrintf(dSnap, "S%v(T%v) <- S%v(T%v), False For Higher Term", rf.me, rf.currTerm, args.LeaderId, args.Term)
		rf.releaseLock()
		return
	} else if args.Term > rf.currTerm {
		DebugPrintf(dSnap, "S%v(T%v) <- S%v(T%v), Change Term", rf.me, rf.currTerm, args.LeaderId, args.Term)
		rf.toFollower(args.Term)
	}

	// send snapshot to raft
	rf.releaseLock()

	DebugPrintf(dSnap, "S%v(T%v) Send Snapshot To Chan", rf.me, rf.currTerm)
	rf.testMsg <- ApplyMsg{CommandValid: false, SnapshotValid: true, SnapshotTerm: args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex, Snapshot: args.Data}
	DebugPrintf(dSnap, "S%v(T%v) End Sending Snapshot To Chan", rf.me, rf.currTerm)
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != 2 {
		return -1, -1, false
	}
	rf.log = append(rf.log, LogEntry{command, rf.currTerm})
	rf.persist()
	//DPrintf("server %v is a leader, check log %v\n", rf.me, rf.log)
	DebugPrintf(dLeader, "S%v(T%v) Receive Client Command %v", rf.me, rf.currTerm, command)

	return rf.logSize() - 1, rf.currTerm, true
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		// check for timeout, if not, go to sleep
		rf.grabLock()
		if rf.state != 2 {
			if time.Now().Sub(rf.lastReceiveTime) >= rf.electionTimeout {
				DebugPrintf(dTimer, "S%v(T%v) ELT, To Candidate", rf.me, rf.currTerm)
				// start election, convert to candidate

				rf.currTerm++
				rf.state = 1
				rf.votedFor = rf.me
				rf.persist()
				rf.resetTimer()
				rf.voteNums++

				// make a copy for term in case of another rpc handler to update currTerm
				copy_term := rf.currTerm
				rf.releaseLock()

				// fire up send remote rpc
				for idx, _ := range rf.peers {
					if idx != rf.me {
						// this go routine used for request vote rpc
						go rf.sendRequestVote(copy_term, idx)
					}
				}
			} else {
				rf.releaseLock()
			}
		} else {
			rf.releaseLock()
		}
		//DPrintf("server %v goes to sleep\n", rf.me)
		time.Sleep(20 * time.Millisecond)
	}
}

// when call this function, must hold rf.mu unless the first initialize
func (rf *Raft) resetTimer() {
	rf.electionTimeout = time.Duration(rand.Intn(400)+200) * time.Millisecond
	rf.lastReceiveTime = time.Now()
	rf.voteNums = 0
}

// do something when convert to follower, and change term, must hold rf.mu
func (rf *Raft) toFollower(term int) {
	rf.currTerm = term
	rf.state = 0
	rf.votedFor = -1
	rf.persist()
}

// when call this function, must hold rf.mu unless the first initialize
func (rf *Raft) freshReceiveTime() {
	rf.lastReceiveTime = time.Now()
}

//
// a go routine to send single heartbeat rpc
//
func (rf *Raft) sendHeartBeat() {
	for rf.killed() == false {
		rf.grabLock()
		if rf.state == 2 {
			if (time.Now().Sub(rf.lastSendTime)) >= rf.heartbeatTimeout {
				DebugPrintf(dTimer, "S%v(T%v) Leader Trigger HeartBeat Timeout", rf.me, rf.currTerm)
				term := rf.currTerm
				rf.releaseLock()
				for idx, _ := range rf.peers {
					if idx != rf.me {
						go rf.sendAppendEntry(term, idx)
					}
				}
			} else {
				rf.releaseLock()
			}
		} else {
			rf.releaseLock()
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
}

//
// a go routine to update lastApplied
//
func (rf *Raft) applyEntry() {
	for rf.killed() == false {
		rf.grabLock()
		for rf.lastApplied == rf.commitIndex {
			rf.updateLastApplied.Wait()
		}
		if rf.lastApplied < rf.SnapshotIndex {
			rf.lastApplied = rf.SnapshotIndex
			rf.releaseLock()
			continue
		}
		rf.snapTurnOn = false
		rf.lastApplied++
		entry := rf.logContent(rf.lastApplied).Command
		rf.releaseLock()
		// single update, nowhere to apply entry
		//DPrintf("server %v apply entry %v\n", rf.me, rf.commitIndex)
		DebugPrintf(dCommit, "S%v send msg", rf.me)
		rf.testMsg <- ApplyMsg{CommandValid: true, CommandIndex: rf.lastApplied, Command: entry}
		DebugPrintf(dCommit, "S%v end sending msg", rf.me)

		rf.grabLock()
		rf.snapTurnOn = true
		rf.snapChange.Broadcast()
		rf.releaseLock()
	}
}

// Make
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currTerm = 0
	// -1 means none
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1)
	// insert random data
	//rf.log[0].Term = -10
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = 0
	rf.resetTimer()
	rf.heartbeatTimeout = time.Duration(125) * time.Millisecond
	rf.updateLastApplied = sync.NewCond(&rf.mu)
	rf.testMsg = applyCh
	rf.SnapshotIndex = -1
	rf.SnapshotTerm = -1
	rf.snapTurnOn = true
	rf.snapChange = sync.NewCond(&rf.mu)

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyEntry()

	return rf
}
