package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log Entries are
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

// LogEntry
// the entry of log. each entry contains command for state machine,
// and Term when entry was received by leader
type LogEntry struct {
	Term int // Term when entry was received by leader
	//TODO:
	// command for state machine
}

type Log struct {
	Entries        []LogEntry
	committedIndex int // index of the highest log entry known to be committed
	lastApplied    int // index of the highest log entry applied to state machine
}

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

//TODO: 150ms now, to be changed
const heartbeatInterval = 150

// Raft
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
	State           StateType
	isLeader        bool
	leader          int
	lastHeartbeat   time.Time     // the time when receive/send heartbeat or start election last time
	electionTimeout time.Duration // randomized election timeouts

	// persistent state on all servers
	currentTerm int // the latest Term server has seen
	votedFor    int // candidateID that received vote in current Term
	log         Log // the log of the server

	// volatile state on leaders
	nextIndex  []int // for each server, index of the next log entry to send to that server(initialized to leader last log index + 1)
	matchIndex []int // for each server, index of the highest log entry known to be replicated on server
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.isLeader
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

// RequestVoteArgs
// RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //candidate's Term
	CandidateId  int //candidate requesting vote
	LastLogIndex int //index of candidate's last log entry
	LastLogTerm  int //Term of candidate's last log entry
}

// RequestVoteReply
// RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //currentTerm, for candidate to update itself
	VoteGranted bool //true means candidate received vote
}

type AppendEntriesArgs struct {
	Term         int        // leader's Term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // Term of PrevLogIndex entry
	Entries      []LogEntry // log Entries to store(empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching PrevLogIndex and PrevLogTerm
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
// handler function on the server side does not return.  Thus, there
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
	DPrintf("peer %d send Request vote to peer %d", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
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
// Term. the third return value is true if this server believes it is
// the leader.
//

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// Kill
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

// RequestVote
// example RequestVote RPC handler.
// a peer receive a vote request from a candidate.
// and this peer should respond to the candidate.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A, 2B).
	DPrintf("peer %d receive a vote request from peer %d", rf.me, args.CandidateId)

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	if rf.State != StateFollower {
		rf.State = StateFollower
	}

	//TODO: log index and Term?
	if args.LastLogTerm > rf.currentTerm || args.LastLogTerm == rf.currentTerm && args.LastLogIndex >= rf.log.lastApplied {
		if args.Term == rf.currentTerm {
			// each follower will vote for at most one
			// candidate in a given term, on a first-come-first-served basis.
			reply.VoteGranted = rf.votedFor == -1 || rf.votedFor == args.CandidateId
		} else {
			//args.Term > rf.currentTerm

			// Voting in a bigger term doesn't
			// affect voting in this term
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
		}
	} else {
		reply.VoteGranted = false
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
	}
	if reply.VoteGranted == true {
		DPrintf("peer %d agree the vote from peer %d", rf.me, args.CandidateId)
	}

}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.State = StateCandidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.lastHeartbeat = time.Now()
	rf.electionTimeout = resetElectionTimeout()
	DPrintf("peer %d start a new round of election at Term %d", rf.me, rf.currentTerm)
	rf.mu.Unlock()

	//used to wait until every sending RequestVote goroutine over
	cnt := 1
	finished := 1
	var cntMu sync.Mutex
	cond := sync.NewCond(&cntMu)

	// used to avoid the lock in the sending RequestVote goroutine
	term := rf.currentTerm
	lastLogIndex := rf.log.lastApplied

	//send RequestVote concurrently
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		//a sending RequestVote goroutine
		go func(server int, term int, lastLogIndex int) {
			DPrintf("peer %d send RequestVote to peer %d", rf.me, server)

			args := &RequestVoteArgs{
				Term:        term,
				CandidateId: rf.me,

				//TODO: log index and Term to be changed
				LastLogIndex: lastLogIndex,
				LastLogTerm:  term,
			}

			reply := &RequestVoteReply{}
			ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
			if !ok {
				//log.Fatalf("RPC error when send RequestVote from %d to %d\n", rf.me, server)
				DPrintf("ERROR! RPC error when send RequestVote from %d to %d", rf.me, server)
			}
			cntMu.Lock()
			defer cntMu.Unlock()
			if reply.VoteGranted {
				DPrintf("peer %d get the vote from peer %d", rf.me, server)
				cnt++
			} else {
				if reply.Term != 0 && reply.Term > rf.currentTerm {
					// find a bigger term, become follower
					rf.mu.Lock()
					rf.State = StateFollower
					rf.currentTerm = reply.Term
					rf.mu.Unlock()
					return
				}
			}
			finished++
			cond.Broadcast()
		}(i, term, lastLogIndex)

	}

	//wait for the end of sending RequestVote task
	cntMu.Lock()
	for {
		rf.mu.Lock()
		if cnt >= (len(rf.peers)+1)/2 || finished == len(rf.peers) {
			rf.mu.Unlock()
			break
		}
		if rf.State == StateFollower {
			rf.mu.Unlock()
			cntMu.Unlock()
			return
		}
		rf.mu.Unlock()
		cond.Wait()
	}

	if cnt >= (len(rf.peers)+1)/2 {
		// election Success
		rf.mu.Lock()
		DPrintf("peer %d win the election in term %d", rf.me, rf.currentTerm)
		rf.State = StateLeader
		rf.isLeader = true
		rf.leader = rf.me
		rf.mu.Unlock()

		// bcast heartbeats?
		DPrintf("broadcast heartbeats after election success")
		rf.broadcastHeartbeat()
	} else {
		DPrintf("peer %d lost the election in term %d", rf.me, rf.currentTerm)
	}
	cntMu.Unlock()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(args.Entries) == 0 {
		//heartbeat
		DPrintf("peer %d receive a heartbeat from peer %d", rf.me, args.LeaderId)
		if args.Term < rf.currentTerm {
			DPrintf("leader %d is out of date, peer %d term: %d, leader %d term: %d", args.LeaderId, rf.me, rf.currentTerm, args.LeaderId, args.Term)
			reply.Success = false
			reply.Term = rf.currentTerm
			return
		}
		reply.Success = true
		if rf.State != StateFollower {
			rf.State = StateFollower
			rf.isLeader = false
			rf.leader = -1
			rf.currentTerm = args.Term
		}
		rf.leader = args.LeaderId
		rf.lastHeartbeat = time.Now()
		rf.electionTimeout = resetElectionTimeout()
	} else {
		DPrintf("peer %d receive a AppendEntriesArgs from peer %d", rf.me, args.LeaderId)
		if args.Term < rf.currentTerm {
			reply.Success = false
			return
		}
		reply.Success = true
	}

}

func (rf *Raft) broadcastHeartbeat() {
	// leader send heartbeats to followers
	rf.mu.Lock()
	term := rf.currentTerm
	rf.lastHeartbeat = time.Now()
	rf.mu.Unlock()

	DPrintf("peer %d send heartbeats to followers", rf.me)

	// wanted to use them to wait until every sending HB goroutine over
	// but don't need to wait
	//finished := 1
	//var finishMu sync.Mutex
	//cond := sync.NewCond(&finishMu)

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(server int, term int) {
			DPrintf("peer %d send heartbeat to peer %d", rf.me, server)

			args := &AppendEntriesArgs{
				Term:     term,
				LeaderId: rf.me,
				Entries:  nil,
			}
			reply := &AppendEntriesReply{}
			ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
			if !ok {
				//log.Fatalf("RPC error when send heartbeat from %d to %d\n", rf.me, server)
				DPrintf("ERROR! RPC error when send heartbeat from %d to %d", rf.me, server)
				return
			}
			if !reply.Success {
				//maybe the leader is out of date
				DPrintf("peer %d heartbeat reply fail", rf.me)
				rf.mu.Lock()
				rf.State = StateFollower
				rf.isLeader = false
				rf.leader = -1
				rf.currentTerm = reply.Term
				rf.mu.Unlock()
			}
			//finishMu.Lock()
			//defer finishMu.Unlock()
			//finished++
			//cond.Broadcast()
		}(i, term)
	}

	//finishMu.Lock()
	//for {
	//	if finished == len(rf.peers) {
	//		break
	//	}
	//	cond.Wait()
	//
	//}
	//finishMu.Unlock()
}

// The ticker go routine starts a new election if this peer hasn't received
// heartbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		//DPrintf("peer %d tick", rf.me)

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		//time.Sleep(20 * time.Millisecond)
		rf.mu.Lock()
		if rf.isLeader {
			if time.Since(rf.lastHeartbeat) < heartbeatInterval*time.Millisecond {
				rf.mu.Unlock()
				continue
			}
			rf.mu.Unlock()

			rf.broadcastHeartbeat()
			time.Sleep((heartbeatInterval / 3) * time.Millisecond)
		} else {
			// follower or candidate
			//rf.mu.Unlock()
			if time.Since(rf.lastHeartbeat) >= rf.electionTimeout {
				// election time out, start a new election
				rf.mu.Unlock()
				go rf.startElection()
			} else {
				rf.mu.Unlock()
			}
			time.Sleep((heartbeatInterval * 2) * time.Millisecond)
		}
	}
}

func resetElectionTimeout() time.Duration {
	//TODO: [3*heartbeatInterval, 6*heartbeatInterval] now, to be changed
	return time.Duration(3*heartbeatInterval+rand.Intn(3*heartbeatInterval)) * time.Millisecond
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

	// Your initialization code here (2A, 2B, 2C).
	rf.votedFor = -1
	rf.electionTimeout = resetElectionTimeout()
	rf.lastHeartbeat = time.Now()

	rand.Seed(time.Now().Unix())
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
