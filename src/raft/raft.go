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

import "sync"
import (
	"labrpc"
	"time"
	"math/rand"
)

// import "bytes"
// import "encoding/gob"

type RaftState int

const (
	LEADER    RaftState = 0
	FOLLOWER  RaftState = 1
	CANDIDATE RaftState = 2
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm        int
	votedFor           int
	log                []*LogEntry
	nextIndex          []int
	matchIndex         []int
	commitIndex        int
	currentState       RaftState
	voteRequests       []*RequestVoteReply
	lastUpdate         int64
	heartbeatResponses []*AppendEntriesResponse
	appendResponses    []*AppendEntriesResponse
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.currentState == LEADER
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
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

type AppendEntriesResponse struct {
	Term    int
	Success bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PervLogTerm  int
	Entries      *LogEntry
	LeaderCommit int
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type LogEntry struct {
	Term int
	Data int
}

//
// example RequestVote RPC handler.
//

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

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	if rf.currentState == LEADER {
		var logIndex, prevLogTerm int
		if len(rf.log) == 0 {
			logIndex = -1
		} else {
			logIndex = len(rf.log) - 1
		}

		if logIndex == -1 {
			prevLogTerm = -1
		} else {
			prevLogTerm = rf.log[logIndex].Term
		}

		for peer := range rf.peers {
			entry := &LogEntry{
				Term: rf.currentTerm,
				Data: command.(int),
			}
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				Entries:      entry,
				LeaderCommit: rf.commitIndex,
				PrevLogIndex: logIndex,
				PervLogTerm:  prevLogTerm,
			}
			reply := &AppendEntriesResponse{}
			go rf.sendHeartbeat(peer, args, reply)
		}
	}

	return rf.commitIndex, rf.currentTerm, rf.currentState == LEADER
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) ResetTimer() {
	rf.lastUpdate = time.Now().UnixNano() / 1000000
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

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && (rf.votedFor != -1 && rf.votedFor != args.CandidateId)) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else {
		rf.ResetTimer()
		rf.votedFor = args.CandidateId
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.currentState = FOLLOWER
		}
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendHeartbeat(server int, args *AppendEntriesArgs, reply *AppendEntriesResponse) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesResponse) {
	rf.mu.Lock()
	if args.Entries != nil {
		if args.Term < rf.currentTerm {
			reply.Term = rf.currentTerm
			reply.Success = false
		} else {
			rf.ResetTimer()
			if args.Term != rf.currentTerm {
				rf.currentState = FOLLOWER
				rf.votedFor = -1
			}
			rf.currentTerm = args.Term
			reply.Term = rf.currentTerm
			reply.Success = true
		}
	} else {
		println("Append entry actually")
	}
	rf.mu.Unlock()
}

func TriggerElection(rf *Raft) {
	rf.currentState = CANDIDATE
	rf.voteRequests = []*RequestVoteReply{}
	rf.votedFor = rf.me
	rf.currentTerm = rf.currentTerm + 1
	for peer := range rf.peers {
		if peer == rf.me {
			reply := &RequestVoteReply{Term: rf.currentTerm, VoteGranted: true}
			rf.voteRequests = append(rf.voteRequests, reply)
		} else {
			reply := &RequestVoteReply{}
			args := &RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me}
			go rf.sendRequestVote(peer, args, reply)
			rf.voteRequests = append(rf.voteRequests, reply)
		}
	}
	rf.ResetTimer()
}

func UpdateRaft(rf *Raft) {
	for {
		waitTime := rand.Intn(900) + 300

		if rf.currentState == FOLLOWER {
			time.Sleep(time.Millisecond * time.Duration(waitTime))
			millis := time.Now().UnixNano() / 1000000

			rf.mu.Lock()
			if millis-rf.lastUpdate > int64(waitTime) {
				TriggerElection(rf)
			}
			rf.mu.Unlock()
		}

		if rf.currentState == CANDIDATE {
			time.Sleep(time.Millisecond * time.Duration(waitTime/4))
			forVotes := 0
			millis := time.Now().UnixNano() / 1000000
			if millis-rf.lastUpdate > int64(waitTime) {
				rf.mu.Lock()
				TriggerElection(rf)
				rf.mu.Unlock()
			}

			for vote := range rf.voteRequests {
				data := rf.voteRequests[vote]

				if data.Term > rf.currentTerm {
					rf.currentState = FOLLOWER
					rf.votedFor = -1
					rf.currentTerm = data.Term
					forVotes = 0
					break
				}

				if data.VoteGranted {
					forVotes++
				}
			}

			if forVotes > len(rf.peers)/2 {
				rf.currentState = LEADER
			}

		}

		if rf.currentState == LEADER {
			rf.heartbeatResponses = []*AppendEntriesResponse{}
			for peer := range rf.peers {
				if rf.me == peer {
					continue
				}
				args := &AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me}
				reply := &AppendEntriesResponse{}
				go rf.sendHeartbeat(peer, args, reply)
				rf.heartbeatResponses = append(rf.heartbeatResponses, reply)
			}

			time.Sleep(250 * time.Millisecond)

			for res := range rf.heartbeatResponses {
				if rf.heartbeatResponses[res] != nil && rf.heartbeatResponses[res].Success == false && rf.heartbeatResponses[res].Term != 0 {
					rf.mu.Lock()
					rf.currentState = FOLLOWER
					rf.votedFor = -1
					rf.ResetTimer()
					rf.currentTerm = rf.heartbeatResponses[res].Term
					rf.mu.Unlock()
				}
			}
		}
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.mu = sync.Mutex{}
	rf.votedFor = -1
	rf.currentState = FOLLOWER
	rf.ResetTimer()

	go UpdateRaft(rf)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
