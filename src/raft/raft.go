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
	"log"
	//"crypto/rand"
	"math/rand"
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
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
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
	//log.Printf("server %v is grabbing lock\n", rf.me)
	rf.mu.Lock()
	//log.Printf("server %v gets lock\n", rf.me)
}

func (rf *Raft) releaseLock() {
	rf.mu.Unlock()
	//log.Printf("server %v release lock\n", rf.me)
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
	log.Printf("server %v receive RequestVote RPC\n", rf.me)
	// reset receive time for RequestVote RPC
	rf.freshReceiveTime()
	if args.Term < rf.currTerm {
		log.Printf("server %v receive request vote which term less than itself, return false\n", rf.me)
		reply.Term = rf.currTerm
		reply.VoteGranted = false
		return
	} else if rf.currTerm < args.Term {
		log.Printf("server %v receive request vote which term larger than itself, turn into follower\n", rf.me)
		rf.toFollower(args.Term)
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// need change in later lab
		log.Printf("server %v receive request vote, vote for %v\n", rf.me, args.CandidateId)
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		reply.Term = rf.currTerm
	}
}

// rpc handler for AppendEntry
func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	// for now, only to reset timeout
	rf.grabLock()
	defer rf.releaseLock()
	log.Printf("server %v receive AppendEntry RPC\n", rf.me)
	// reset receive time for AppendEntry RPC
	rf.freshReceiveTime()
	if rf.currTerm < args.Term {
		// convert to follower
		log.Printf("server %v receive AppendEntry RPC which term greater than itself, and turn into follower\n", rf.me)
		rf.toFollower(args.Term)
	} else if args.Term < rf.currTerm {
		log.Printf("server %v receive AppendEntry RPC which term less than itself, return false\n", rf.me)
		reply.Success = false
		reply.Term = rf.currTerm
		return
	}
	// check for candidate, receive AppendEntry RPC from leader
	if rf.state == 1 {
		log.Printf("server %v receive AppendEntry RPC from another leader, turn into follower\n", rf.me)
		// here is the term correctly??
		rf.toFollower(args.Term)
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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
				log.Printf("server %v election timeout, turn into candidate\n", rf.me)
				// start election, convert to candidate

				rf.currTerm++
				rf.state = 1
				rf.votedFor = rf.me
				rf.voteNums++
				rf.resetTimer()

				// make a copy for term in case of another rpc handler to update currTerm
				copy_term := rf.currTerm
				rf.releaseLock()

				// fire up send remote rpc
				for idx, _ := range rf.peers {
					if idx != rf.me {
						// this go routine used for request vote rpc
						go func(term int, server int) {
							args := RequestVoteArgs{}
							reply := RequestVoteReply{}
							//log.Printf("server %v sending request vote RPC to %v, turn into candidate\n", rf.me, server)
							// whether we need this??
							rf.grabLock()
							if rf.state != 1 {
								// not candidate
								log.Printf("server %v sending request vote RPC to %v, but canceled because of state turn into:%v\n", rf.me, server, rf.state)
								rf.releaseLock()
								return
							}
							rf.releaseLock()
							// leave log content nil
							args.Term = term
							args.CandidateId = rf.me
							ok := rf.sendRequestVote(server, &args, &reply)

							rf.grabLock()
							defer rf.releaseLock()
							if !ok {
								//log.Printf("raft.go::ticker() send request vote to %v fail\n", server)
								return
							}
							log.Printf("server %v sending request vote RPC to %v, receive reply\n", rf.me, server)
							// case for converting to a follower or being a leader
							if rf.state != 1 {
								log.Printf("server %v sending request vote RPC to %v, receive reply, but state changed to %v\n", rf.me, server, rf.state)
								return
							}
							// check for reply term
							if rf.currTerm < reply.Term {
								log.Printf("server %v sending request vote RPC to %v, receive a reply which term greater than itself\n", rf.me, server)
								rf.toFollower(reply.Term)
								return
							}

							// do nothing, if already get majority votes
							if reply.VoteGranted {
								rf.voteNums++
								log.Printf("server %v sending request vote RPC to %v, receive a reply which vote granted\n", rf.me, server)
							}
							if rf.voteNums > rf.voteNums/2 {
								// become leader
								log.Printf("server %v sending request vote RPC, become a leader\n", rf.me)
								rf.state = 2
								// fire a go routine to send heartbeat
								rf.lastSendTime = time.Now()
								go rf.sendHeartBeat()
							}

						}(copy_term, idx)
					}
				}
			} else {
				rf.releaseLock()
			}
		} else {
			rf.releaseLock()
		}
		//log.Printf("server %v goes to sleep\n", rf.me)
		time.Sleep(20 * time.Millisecond)
	}
}

//func (rf *Raft) waitForVote(wg *sync.WaitGroup) {
//	wg.Wait()
//	rf.grabLock()
//	defer rf.releaseLock()
//	log.Printf("server %v already wait for all server\n", rf.me)
//	majority := rf.reachableNums / 2
//	if rf.reachableNums%2 != 0 {
//		majority++
//	}
//	if rf.voteNums >= majority {
//		// become leader
//		log.Printf("server %v sending request vote RPC, become a leader\n", rf.me)
//		rf.state = 2
//		// fire a go routine to send heartbeat
//		rf.lastSendTime = time.Now()
//		go rf.sendHeartBeat()
//	}
//}

// when call this function, must hold rf.mu unless the first initialize
func (rf *Raft) resetTimer() {
	rf.electionTimeout = time.Duration(rand.Intn(200)+200) * time.Millisecond
	rf.lastReceiveTime = time.Now()
	rf.voteNums = 0
}

// do something when convert to follower, and change term, must hold rf.mu
func (rf *Raft) toFollower(term int) {
	rf.currTerm = term
	rf.state = 0
	rf.votedFor = -1
}

// when call this function, must hold rf.mu unless the first initialize
func (rf *Raft) freshReceiveTime() {
	rf.lastReceiveTime = time.Now()
}

func (rf *Raft) sendHeartBeat() {
	for rf.killed() == false {
		rf.grabLock()
		if rf.state == 2 {
			if (time.Now().Sub(rf.lastSendTime)) >= rf.heartbeatTimeout {
				log.Printf("server %v is a leader and trigger heartbeat timeout\n", rf.me)
				term := rf.currTerm
				rf.releaseLock()
				for idx, _ := range rf.peers {
					if idx != rf.me {
						go func(term int, server int) {
							// rf.me is read only
							//log.Printf("server %v is a leader, sending heartbeat to %v\n", rf.me, server)
							args := AppendEntryArgs{Term: term, LeaderId: rf.me}
							reply := AppendEntryReply{}
							ok := rf.sendAppendEntry(server, &args, &reply)
							if !ok {
								//log.Printf("raft.go::sendHeartBeat() send append entry fail\n")
								return
							}
							log.Printf("server %v is a leader, getting heartbeat reply from %v\n", rf.me, server)
							rf.grabLock()
							defer rf.releaseLock()
							// guard for case, do we need this?
							if rf.state != 2 {
								log.Printf("server %v is a leader, getting heartbeat reply from %v, but is not a leader now\n", rf.me, server)
								return
							}
							if rf.currTerm < reply.Term {
								log.Printf("server %v is a leader, getting heartbeat reply from %v, the reply has a higher term, turn into follower\n", rf.me, server)
								rf.toFollower(reply.Term)
							}

						}(term, idx)
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
	rf.log = nil
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = nil
	rf.matchIndex = nil
	rf.state = 0
	rf.resetTimer()
	rf.heartbeatTimeout = time.Duration(125) * time.Millisecond

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
