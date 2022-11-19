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
	"6.824/labgob"
	"bytes"
	"crypto/rand"
	"log"
	"math/big"
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
	Command interface{}
}

type Log struct {
	Entries []LogEntry

	//for all servers:
	//If commitIndex > lastApplied: increment lastApplied, apply
	//log[lastApplied] to state machine
	//so, lastApplied <= CommittedIndex

	//TODO: commit
	CommittedIndex int // index of the highest log entry known to be committed
	//lastApplied    int // index of the highest log entry applied to state machine

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

	applyCh chan ApplyMsg

	State StateType
	//isLeader        bool
	leader          int
	lastHeartbeat   time.Time     // the time when receive/send heartbeat or start election last time
	electionTimeout time.Duration // randomized election timeouts

	// persistent state on all servers
	CurrentTerm int // the latest Term server has seen
	VotedFor    int // candidateID that received vote in current Term
	Log         Log // the log of the server

	// volatile state on leaders
	nextIndex  []int // for each server, index of the next log entry to send to that server(initialized to leader last log index + 1)
	matchIndex []int // for each server, index of the highest log entry known to be replicated on server
}

// GetState return CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.CurrentTerm, rf.State == StateLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	//DPrintf("%d persist", rf.me)
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(rf.CurrentTerm)
	if err != nil {
		log.Fatalln(err)
	}
	err = e.Encode(rf.VotedFor)
	if err != nil {
		log.Fatalln(err)
	}
	err = e.Encode(rf.Log.Entries)
	if err != nil {
		log.Fatalln(err)
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
	DPrintf("%d readPersist", rf.me)
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var CurrentTerm int
	var VotedFor int
	var LogEntries []LogEntry
	if d.Decode(&CurrentTerm) != nil ||
		d.Decode(&VotedFor) != nil || d.Decode(&LogEntries) != nil {
		log.Fatalln("readPersistErr")
	} else {
		rf.mu.Lock()
		rf.CurrentTerm = CurrentTerm
		rf.VotedFor = VotedFor
		rf.Log.Entries = LogEntries
		rf.mu.Unlock()
	}
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
	Term        int  //CurrentTerm, for candidate to update itself
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
	Term     int  // CurrentTerm, for leader to update itself
	Success  bool // true if follower contained entry matching PrevLogIndex and PrevLogTerm
	XTerm    int  // Follower中与Leader冲突的Log对应的任期号
	XIndex   int  // Follower中，对应任期号为XTerm的第一条Log条目的槽位号
	XLen     int  // 如果Follower在对应位置没有Log，那么XTerm会返回-1，XLen表示空白的Log槽位数
	LogMatch bool // used for heartbeat to check the log if match
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
	DPrintf("[%d] send Request vote to peer %d", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// return the last log index
// entries[0] is null, which means index starts from 1
// so last log index == 0 means the log is empty
func (rf *Raft) lastLogIndex() int {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	if len(rf.Log.Entries) == 0 {
		return 0
	} else {
		return len(rf.Log.Entries) - 1
	}
}

func (rf *Raft) lastLogTerm() int {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	lastIdx := rf.lastLogIndex()
	if lastIdx == 0 {
		// the log is empty
		return 0
	} else {
		return rf.Log.Entries[lastIdx].Term
	}
}

func (rf *Raft) sendAppendEntries(lenServers, term int, entireLog Log, nextIndex []int) {
	//used to wait until every sending AppendEntries goroutine over
	cnt := 1      //count of successful replicate followers
	finished := 1 //count of successful replicate and disconnected followers

	// this leader is out of date or the "term" argument is smaller than the "rf.CurrentTerm"
	outOfDate := false

	var cntMu sync.Mutex
	cond := sync.NewCond(&cntMu)

	// the leader send AppendEntries to others
	for i := 0; i < lenServers; i++ {
		if i == rf.me {
			continue
		}
		go func(server int, term int, entireLog Log, nextIndex int) {
			success := false
			for !success {
				if nextIndex >= len(entireLog.Entries) {
					break
				}
				rf.mu.Lock()
				currentTerm := rf.CurrentTerm
				rf.mu.Unlock()
				if term < currentTerm {
					cntMu.Lock()
					outOfDate = true
					cond.Broadcast()
					cntMu.Unlock()
					return
				}
				//DPrintf("[%d] send AppendEntries to peer %d", rf.me, server)
				DPrintf("[%d] before send AE to %d nextIndex:%d", rf.me, server, nextIndex)
				DPrintln(entireLog)
				args := &AppendEntriesArgs{
					Term:         term,
					LeaderId:     rf.me,
					PrevLogIndex: nextIndex - 1,
					PrevLogTerm:  entireLog.Entries[nextIndex-1].Term,
					//at the first sending, only include the new entry
					Entries:      entireLog.Entries[nextIndex:],
					LeaderCommit: entireLog.CommittedIndex,
				}
				reply := &AppendEntriesReply{}
				ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
				if !ok {
					//log.Fatalf("RPC error when send heartbeat from %d to %d\n", rf.me, server)
					//DPrintf("ERROR! RPC error when send heartbeat from %d to %d", rf.me, server)
					success = false
					//TODO: send continuously?
					cntMu.Lock()
					finished++
					cond.Broadcast()
					cntMu.Unlock()
					return
				}
				success = reply.Success
				if !success {
					rf.mu.Lock()
					currentTerm = rf.CurrentTerm
					rf.mu.Unlock()
					if term < currentTerm {
						cntMu.Lock()
						outOfDate = true
						cond.Broadcast()
						cntMu.Unlock()
						return
					}
					if reply.Term != 0 && reply.Term > currentTerm {
						// find a bigger term, become follower
						rf.mu.Lock()
						DPrintf("bigger term")
						DPrintf("term: %d rf.CurrentTerm: %d reply.Term: %d", term, currentTerm, reply.Term)
						rf.State = StateFollower
						rf.CurrentTerm = reply.Term
						rf.persist()
						rf.mu.Unlock()
						cntMu.Lock()
						outOfDate = true
						cond.Broadcast()
						cntMu.Unlock()
						return
					}
					if reply.XTerm == -1 {
						//follower's log shorter
						DPrintf("[%d] follower %d's Log shorter lastLogIndex: %d XLen: %d\n", rf.me, server, nextIndex-1, reply.XLen)

						nextIndex = nextIndex - reply.XLen
						//lastLogIndex = lastLogIndex - reply.XLen
						//lastLogTerm = entireLog.Entries[lastLogIndex].Term
					} else {
						DPrintf("[%d] follower %d doesn't match", rf.me, server)
						i := len(entireLog.Entries) - 1
						flag := false
						for ; i >= 1; i-- {
							if entireLog.Entries[i].Term == reply.XTerm {
								flag = true
								break
							}
						}
						if !flag {
							DPrintf("hasn't XTerm")
							//Leader发现自己没有XTerm任期的日志
							nextIndex = reply.XIndex
							//lastLogIndex = nextIndex - 1
							//lastLogTerm = entireLog.Entries[lastLogIndex].Term
						} else {
							DPrintf("has XTerm")
							//Leader发现自己有XTerm任期的日志
							nextIndex = i + 1
							//lastLogIndex = i
							//lastLogTerm = entireLog.Entries[lastLogIndex].Term
						}
					}
				} else {
					nextIndex = len(entireLog.Entries)
				}
			}

			rf.mu.Lock()

			rf.nextIndex[server] = nextIndex
			rf.mu.Unlock()

			cntMu.Lock()
			defer cntMu.Unlock()

			cnt++
			finished++
			cond.Broadcast()

		}(i, term, entireLog, nextIndex[i])
	}

	//TODO: commit

	cntMu.Lock()
	for {
		rf.mu.Lock()
		//DPrintf("[%d] leader waiting. cnt: %d, finished: %d", rf.me, cnt, finished)
		if cnt >= (lenServers+1)/2 || finished == lenServers {
			rf.mu.Unlock()
			break
		}
		if rf.State == StateFollower {
			rf.mu.Unlock()
			cntMu.Unlock()
			return
		}

		if outOfDate {
			rf.mu.Unlock()
			cntMu.Unlock()
			return
		}

		if term < rf.CurrentTerm {
			rf.mu.Unlock()
			cntMu.Unlock()
			return
		}
		rf.mu.Unlock()
		cond.Wait()
	}

	if cnt >= (lenServers+1)/2 {
		//commit this log entry
		rf.mu.Lock()
		if rf.Log.CommittedIndex >= len(entireLog.Entries) {
			// maybe another goroutine has already finished
			// the AE task with a newer log
			rf.mu.Unlock()
			return
		}
		if term < rf.CurrentTerm {
			rf.mu.Unlock()
			return
		}
		if entireLog.Entries[len(entireLog.Entries)-1].Term != rf.CurrentTerm {
			//为了消除图 8 里描述的情况，Raft 永远不会通过计算副本数目的方式去提交一个之前任期内的日志条目。
			//只有领导人当前任期里的日志条目通过计算副本数目可以被提交
			rf.mu.Unlock()
			return
		}
		for i := rf.Log.CommittedIndex + 1; i <= len(entireLog.Entries)-1; i++ {
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      entireLog.Entries[i].Command,
				CommandIndex: i,
				//SnapshotValid: false,
				//Snapshot:      nil,
				//SnapshotTerm:  0,
				//SnapshotIndex: 0,
			}
			rf.applyCh <- applyMsg
		}
		rf.Log.CommittedIndex = len(entireLog.Entries) - 1
		DPrintf("[%d] lead commit committedIndex: %d", rf.me, rf.Log.CommittedIndex)
		DPrintln(rf.Log.Entries)
		rf.mu.Unlock()

	}
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

func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	rf.mu.Lock()
	//DPrintf("[%d] start", rf.me)

	term = rf.CurrentTerm
	isLeader = rf.State == StateLeader

	// Your code here (2B).
	if !isLeader {
		rf.mu.Unlock()
		return
	}
	entry := LogEntry{
		Term:    term,
		Command: command,
	}

	// the log entry immediately preceding new ones
	//lastLogIndex := rf.lastLogIndex()
	//lastLogTerm := rf.lastLogTerm()

	//TODO: log entry index to be changed
	if len(rf.Log.Entries) == 0 {
		// the log entry starts from index 1
		rf.Log.Entries = append(rf.Log.Entries, LogEntry{})
	}
	//DPrintf("[%d] leader append entry. index: %d", rf.me, len(rf.log.Entries))
	rf.Log.Entries = append(rf.Log.Entries, entry)
	rf.persist()
	index = len(rf.Log.Entries) - 1
	//rf.log.lastApplied++

	entireLog := rf.Log
	lenServers := len(rf.peers)
	nextIndex := append([]int{}, rf.nextIndex...)
	rf.mu.Unlock()

	go rf.sendAppendEntries(lenServers, term, entireLog, nextIndex)

	return
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
	//DPrintf("[%d] receive a vote request from peer %d", rf.me, args.CandidateId)

	if args.Term < rf.CurrentTerm {
		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm
		return
	}
	if rf.State != StateFollower {
		rf.State = StateFollower
	}
	lastLogIndex := rf.lastLogIndex()
	lastLogTerm := rf.lastLogTerm()

	//TODO: log index and Term?
	if args.LastLogTerm > lastLogTerm || args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex {
		if args.Term == rf.CurrentTerm {
			// each follower will vote for at most one
			// candidate in a given term, on a first-come-first-served basis.
			reply.VoteGranted = rf.VotedFor == -1 || rf.VotedFor == args.CandidateId
		} else {
			//args.Term > rf.CurrentTerm

			// Voting in a smaller term doesn't
			// affect voting in this term
			reply.VoteGranted = true
			rf.VotedFor = args.CandidateId
			rf.persist()
		}
	} else {
		//DPrintf("[%d] RV from peer %d log problem", rf.me, args.CandidateId)
		reply.VoteGranted = false
	}
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.persist()
	}
	if reply.VoteGranted == true {
		//DPrintf("[%d] agree the vote from peer %d", rf.me, args.CandidateId)
	}

}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.State = StateCandidate
	rf.CurrentTerm++
	rf.VotedFor = rf.me
	rf.persist()
	//DPrintf("[%d] start a new round of election at Term %d", rf.me, rf.CurrentTerm)
	DPrintf("[%d] start a new round of election at Term %d", rf.me, rf.CurrentTerm)
	DPrintf("[%d] timeout: %d", rf.me, rf.electionTimeout)
	rf.lastHeartbeat = time.Now()
	rf.electionTimeout = resetElectionTimeout()

	// used to avoid the lock in the sending RequestVote goroutine
	term := rf.CurrentTerm
	lastLogIndex := rf.lastLogIndex()
	lastLogTerm := rf.lastLogTerm()
	lenServers := len(rf.peers)
	rf.mu.Unlock()

	//used to wait until every sending RequestVote goroutine over
	cnt := 1
	finished := 1
	var cntMu sync.Mutex
	cond := sync.NewCond(&cntMu)

	//send RequestVote concurrently
	for i := 0; i < lenServers; i++ {
		if i == rf.me {
			continue
		}

		//a sending RequestVote goroutine
		go func(server int, term int, lastLogIndex int, lastLogTerm int) {
			//DPrintf("[%d] send RequestVote to peer %d", rf.me, server)

			args := &RequestVoteArgs{
				Term:        term,
				CandidateId: rf.me,

				//TODO: log index and Term to be changed
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}

			reply := &RequestVoteReply{}
			ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
			if !ok {
				//log.Fatalf("RPC error when send RequestVote from %d to %d\n", rf.me, server)
				//DPrintf("ERROR! RPC error when send RequestVote from %d to %d", rf.me, server)
				cntMu.Lock()
				finished++
				cond.Broadcast()
				cntMu.Unlock()
				return
			}
			cntMu.Lock()
			defer cntMu.Unlock()
			if reply.VoteGranted {
				//DPrintf("[%d] get the vote from peer %d", rf.me, server)
				cnt++
			} else {
				//DPrintf("[%d] receive disagree from peer [%d]", rf.me, server)
				rf.mu.Lock()
				if reply.Term != 0 && reply.Term > rf.CurrentTerm {
					// find a bigger term, become follower
					rf.State = StateFollower
					rf.CurrentTerm = reply.Term
					rf.persist()
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
			}
			finished++
			cond.Broadcast()
		}(i, term, lastLogIndex, lastLogTerm)

	}

	//wait for the end of sending RequestVote task
	cntMu.Lock()
	for {
		rf.mu.Lock()
		if cnt >= (lenServers+1)/2 || finished == lenServers {
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

	if cnt >= (lenServers+1)/2 {
		// election Success, become leader
		rf.mu.Lock()

		if term != rf.CurrentTerm {
			rf.mu.Unlock()
			cntMu.Unlock()
			return
		}

		if rf.State == StateFollower {
			rf.mu.Unlock()
			cntMu.Unlock()
			return
		}

		DPrintf("[%d] win the election in term %d", rf.me, term)
		rf.State = StateLeader
		rf.leader = rf.me

		for i := 0; i < lenServers; i++ {
			rf.nextIndex[i] = lastLogIndex + 1
			rf.matchIndex[i] = 0
		}

		rf.mu.Unlock()

		// TODO:broadcast heartbeats or not???
		//DPrintf("broadcast heartbeats after election success")

		rf.broadcastHeartbeat()
	}
	cntMu.Unlock()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(args.Entries) == 0 {
		//heartbeat
		//DPrintf("[%d] receive a heartbeat from peer %d", rf.me, args.LeaderId)
		if args.Term < rf.CurrentTerm {
			DPrintf("leader %d is out of date, peer %d term: %d, leader %d term: %d", args.LeaderId, rf.me, rf.CurrentTerm, args.LeaderId, args.Term)
			reply.Success = false
			reply.Term = rf.CurrentTerm
			return
		}
		reply.Success = true

		rf.State = StateFollower
		rf.leader = args.LeaderId
		rf.CurrentTerm = args.Term
		rf.persist()
		rf.lastHeartbeat = time.Now()
		rf.electionTimeout = resetElectionTimeout()

		if args.PrevLogIndex != rf.lastLogIndex() || args.PrevLogTerm != rf.lastLogTerm() {
			// log doesn't match
			reply.LogMatch = false
			return
		}
		reply.LogMatch = true

		prevCommittedIndex := rf.Log.CommittedIndex
		if args.LeaderCommit > rf.Log.CommittedIndex {
			rf.Log.CommittedIndex = IntMin(args.LeaderCommit, len(rf.Log.Entries)-1)
		}

		if rf.Log.CommittedIndex > prevCommittedIndex {
			DPrintf("[%d] commit in heartbeat. prevIdx: %d, now: %d", rf.me, prevCommittedIndex, rf.Log.CommittedIndex)
			for i := prevCommittedIndex + 1; i <= rf.Log.CommittedIndex; i++ {
				applyMsg := ApplyMsg{
					CommandValid: true,
					Command:      rf.Log.Entries[i].Command,
					CommandIndex: i,
					//SnapshotValid: false,
					//Snapshot:      nil,
					//SnapshotTerm:  0,
					//SnapshotIndex: 0,
				}
				rf.applyCh <- applyMsg
			}

			DPrintf("[%d] follower commit ", rf.me)
			DPrintln(rf.Log.Entries)
		}
	} else {
		//append entries
		//DPrintf("[%d] receive a AppendEntriesArgs from peer %d", rf.me, args.LeaderId)
		if args.Term < rf.CurrentTerm {
			DPrintf("leader %d is out of date, peer %d term: %d, leader %d term: %d", args.LeaderId, rf.me, rf.CurrentTerm, args.LeaderId, args.Term)
			reply.Success = false
			reply.Term = rf.CurrentTerm
			return
		}
		//reply.Success = true

		rf.State = StateFollower
		rf.leader = args.LeaderId
		rf.CurrentTerm = args.Term
		rf.persist()
		rf.lastHeartbeat = time.Now()
		rf.electionTimeout = resetElectionTimeout()
		
		if len(rf.Log.Entries) == 0 && args.PrevLogIndex != 0 {
			DPrintf("[%d] follower's Log shorter than the leader %d and follower's Log is empty", rf.me, args.LeaderId)
			reply.Success = false
			reply.XTerm = -1
			reply.XLen = args.PrevLogIndex
			return
		} else if len(rf.Log.Entries) != 0 && args.PrevLogIndex >= len(rf.Log.Entries) {
			//follower's log shorter
			DPrintf("[%d]short prevLogIndex: %d", rf.me, args.PrevLogIndex)
			DPrintln(rf.Log.Entries)
			//DPrintf("[%d] follower's log shorter than the leader %d", rf.me, args.LeaderId)
			reply.Success = false
			reply.XTerm = -1
			reply.XLen = args.PrevLogIndex + 1 - len(rf.Log.Entries)
			return
		} else if len(rf.Log.Entries) != 0 && rf.Log.Entries[args.PrevLogIndex].Term != args.PrevLogTerm {
			//follower's log at this index doesn't match
			DPrintf("[%d] follower's Log at this index doesn't match with leader %d", rf.me, args.LeaderId)
			DPrintln(rf.Log.Entries)
			DPrintf("args.PrevLogIndex:%d args.Term:%d", args.PrevLogIndex, args.Term)
			reply.Success = false
			reply.XTerm = rf.Log.Entries[args.PrevLogIndex].Term
			i := 1
			for ; i < len(rf.Log.Entries); i++ {
				if rf.Log.Entries[i].Term == reply.XTerm {
					break
				}
			}
			reply.XIndex = i
			return
		}

		if len(rf.Log.Entries) == 0 {
			rf.Log.Entries = append(rf.Log.Entries, LogEntry{})
			rf.Log.Entries = append(rf.Log.Entries, args.Entries...)
			rf.persist()
		} else {
			i := 0
			//args.PrevLogIndex = 37
			for ; i < len(args.Entries); i++ {
				if i+args.PrevLogIndex+1 < len(rf.Log.Entries) {
					//如果一个已经存在的条目和新条目（译者注：即刚刚接收到的日志条目）
					//发生了冲突（因为索引相同，任期不同），那么就删除这个已经存在的条目以及它之后的所有条目

					if rf.Log.Entries[i+args.PrevLogIndex+1].Term != args.Entries[i].Term {
						rf.Log.Entries = rf.Log.Entries[:i+args.PrevLogIndex+1]
						rf.Log.Entries = append(rf.Log.Entries, args.Entries[i])
						rf.persist()
					}
					// maybe the leader's log is shorter than the follower's
					// then do not change the follower's newer log entries
				} else if i+args.PrevLogIndex+1 == len(rf.Log.Entries) {
					// if the follower's log is shorter, append
					rf.Log.Entries = append(rf.Log.Entries, args.Entries[i])
					rf.persist()
				}
			}
		}

		DPrintf("[%d] AE from %d success follower's Log:", rf.me, args.LeaderId)
		DPrint(rf.Log.Entries)
		DPrint(" args.Log: ")
		DPrintln(args.Entries)

		prevCommittedIndex := rf.Log.CommittedIndex
		if args.LeaderCommit > rf.Log.CommittedIndex {
			rf.Log.CommittedIndex = IntMin(args.LeaderCommit, len(rf.Log.Entries)-1)
		}

		if rf.Log.CommittedIndex > prevCommittedIndex {
			//DPrintf("[%d] commit in AE. prevIdx: %d, now: %d", rf.me, prevCommittedIndex, rf.log.CommittedIndex)
			for i := prevCommittedIndex + 1; i <= rf.Log.CommittedIndex; i++ {
				applyMsg := ApplyMsg{
					CommandValid: true,
					Command:      rf.Log.Entries[i].Command,
					CommandIndex: i,
					//SnapshotValid: false,
					//Snapshot:      nil,
					//SnapshotTerm:  0,
					//SnapshotIndex: 0,
				}
				rf.applyCh <- applyMsg
			}
			DPrintf("[%d] follower commit  committedIndex: %d", rf.me, rf.Log.CommittedIndex)
			DPrintln(rf.Log.Entries)
		}

		//DPrintf("[%d] append entries to its log", rf.me)
		//DPrintln(rf.log.Entries)
		reply.Success = true

	}

}

func (rf *Raft) broadcastHeartbeat() {
	// leader send heartbeats to followers
	rf.mu.Lock()
	term := rf.CurrentTerm
	rf.lastHeartbeat = time.Now()
	lenServers := len(rf.peers)
	me := rf.me
	leaderCommit := rf.Log.CommittedIndex
	lastLogIndex := rf.lastLogIndex()
	lastLogTerm := rf.lastLogTerm()
	rf.mu.Unlock()

	//DPrintf("[%d] send heartbeats to followers", rf.me)

	// wanted to use them to wait until every sending HB goroutine over
	// but don't need to wait
	//finished := 1
	//var finishMu sync.Mutex
	//cond := sync.NewCond(&finishMu)

	for i := 0; i < lenServers; i++ {
		if i == me {
			continue
		}
		go func(server, term, leaderCommit, lastLogIndex, lastLogTerm int) {
			//DPrintf("[%d] send heartbeat to peer %d", rf.me, server)

			args := &AppendEntriesArgs{
				Term:         term,
				LeaderId:     rf.me,
				Entries:      nil,
				LeaderCommit: leaderCommit,
				PrevLogIndex: lastLogIndex,
				PrevLogTerm:  lastLogTerm,
			}
			reply := &AppendEntriesReply{}
			ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
			if !ok {
				//log.Fatalf("RPC error when send heartbeat from %d to %d\n", rf.me, server)
				//DPrintf("ERROR! RPC error when send heartbeat from %d to %d", rf.me, server)
				return
			}
			if !reply.Success {
				//maybe the leader is out of date
				//DPrintf("[%d] heartbeat reply fail", rf.me)
				rf.mu.Lock()
				rf.State = StateFollower
				rf.leader = -1
				rf.CurrentTerm = reply.Term
				rf.persist()
				rf.mu.Unlock()
			} else if !reply.LogMatch {
				// this follower's log doesn't match
				// send AE to this follower
				rf.mu.Lock()
				entireLog := rf.Log
				nextIndex := rf.nextIndex[server]
				rf.mu.Unlock()
				go func(server int, term int, entireLog Log, nextIndex int) {
					success := false
					//currentIndex := lastLogIndex + 1
					for !success {
						if nextIndex >= len(entireLog.Entries) {
							break
						}
						rf.mu.Lock()
						currentTerm := rf.CurrentTerm
						rf.mu.Unlock()
						if term < currentTerm {
							return
						}
						//DPrintf("[%d] send AppendEntries to peer %d", rf.me, server)
						DPrintf("[%d] before send AE to %d nextIndex:%d ", rf.me, server, nextIndex)
						DPrintln(entireLog)
						args := &AppendEntriesArgs{
							Term:         term,
							LeaderId:     rf.me,
							PrevLogIndex: nextIndex - 1,
							PrevLogTerm:  entireLog.Entries[nextIndex-1].Term,
							//at the first sending, only include the new entry
							Entries:      entireLog.Entries[nextIndex:],
							LeaderCommit: entireLog.CommittedIndex,
						}
						reply := &AppendEntriesReply{}
						ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
						if !ok {
							//log.Fatalf("RPC error when send heartbeat from %d to %d\n", rf.me, server)
							//DPrintf("ERROR! RPC error when send heartbeat from %d to %d", rf.me, server)
							success = false
							//TODO: send continuously?
							return

						}
						success = reply.Success
						if !success {
							rf.mu.Lock()
							currentTerm = rf.CurrentTerm
							rf.mu.Unlock()
							if term < currentTerm {
								return
							}
							if reply.Term != 0 && reply.Term > currentTerm {
								// find a bigger term, become follower

								rf.mu.Lock()
								DPrintf("bigger term")
								DPrintf("term: %d rf.CurrentTerm: %d reply.Term: %d", term, rf.CurrentTerm, reply.Term)
								rf.State = StateFollower
								rf.CurrentTerm = reply.Term
								rf.persist()
								rf.mu.Unlock()
								return
							}
							if reply.XTerm == -1 {
								//follower's log shorter
								DPrintf("[%d] follower %d's Log shorter lastLogIndex: %d XLen: %d", rf.me, server, nextIndex-1, reply.XLen)

								nextIndex = nextIndex - reply.XLen
								DPrintf("nextIndex: %d", nextIndex)
								//lastLogIndex = lastLogIndex - reply.XLen
								//lastLogTerm = entireLog.Entries[lastLogIndex].Term
							} else {
								DPrintf("[%d] follower %d doesn't match", rf.me, server)
								i := len(entireLog.Entries) - 1
								flag := false
								for ; i >= 1; i-- {
									if entireLog.Entries[i].Term == reply.XTerm {
										flag = true
										break
									}
								}
								if !flag {
									DPrintf("hasn't XTerm")
									//Leader发现自己没有XTerm任期的日志
									nextIndex = reply.XIndex
									//lastLogIndex = nextIndex - 1
									//lastLogTerm = entireLog.Entries[lastLogIndex].Term
								} else {
									DPrintf("has XTerm")
									DPrintln(entireLog)
									//Leader发现自己有XTerm任期的日志
									nextIndex = i + 1
									//lastLogIndex = i
									//lastLogTerm = entireLog.Entries[lastLogIndex].Term
								}
							}
						} else {
							nextIndex = len(entireLog.Entries)
						}
					}
					rf.mu.Lock()

					rf.nextIndex[server] = nextIndex
					rf.mu.Unlock()
				}(server, term, entireLog, nextIndex)
			}
		}(i, term, leaderCommit, lastLogIndex, lastLogTerm)
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
		if rf.State == StateLeader {
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
			//TODO: election timeout detect sleep
			time.Sleep((heartbeatInterval * 3) * time.Millisecond)
		}
	}
}

func resetElectionTimeout() time.Duration {
	//TODO: [7*heartbeatInterval, 14*heartbeatInterval] now, to be changed
	n, _ := rand.Int(rand.Reader, big.NewInt(7000))
	randF := float64(n.Int64()) / 1000
	//DPrintln(randF * heartbeatInterval)
	ret := time.Duration(7*heartbeatInterval+randF*heartbeatInterval) * time.Millisecond
	return ret
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
	rf.VotedFor = -1
	rf.electionTimeout = resetElectionTimeout()
	rf.lastHeartbeat = time.Now()

	rf.applyCh = applyCh

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
