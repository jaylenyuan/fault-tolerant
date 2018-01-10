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
	"encoding/gob"
	"labrpc"
	"math/rand"
	"sort"
	"sync"
	"time"
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
type Entry struct {
	Term    int
	Command interface{}
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
	CurrentTerm  int
	VoteFor      int
	votesCounter int
	Logs         []Entry

	state       int //0-follower ,1-candidate, 2-leader
	commitIndex int //known to be committed
	lastApplied int
	nextIndex   []int //next log to send
	matchIndex  []int //for each server, index of highest log entry known to be replicated on server

	heartBeat    chan bool
	grantVote    chan bool
	toggleLeader chan bool
	replicate    chan bool
	newCommit    chan bool

	chanApply chan ApplyMsg
	//last index in snapshot
	lastIndex     int
	lastIndexTerm int

	//timer        *time.Timer
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.CurrentTerm
	isleader = (rf.state == 2)
	return term, isleader
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
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	// e.Encode(rf.CurrentTerm)
	// e.Encode(rf.VoteFor)
	// e.Encode(rf.Logs)
	e.Encode(rf)
	rf.persister.SaveRaftState(w.Bytes())

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
		rf.CurrentTerm = 0
		rf.VoteFor = -1
		rf.Logs = make([]Entry, 0)
		rf.persist()
		return
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	// d.Decode(&rf.CurrentTerm)
	// d.Decode(&rf.VoteFor)
	// d.Decode(&rf.Logs)
	d.Decode(&rf)
	DPrintf("READ PERSIST: Server %v, Term %v, VoteFor %v", rf.me, rf.CurrentTerm, rf.VoteFor)
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

type AppendEntryArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}
type AppendEntryReply struct {
	Term         int
	Success      bool
	Index        int
	ConflictTerm int
}
type InstallSnaptArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

func (rf *Raft) CommitedEqual2Applied() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.lastApplied == rf.commitIndex
}

//
func (rf *Raft) ReadStateSize() int {
	return rf.persister.RaftStateSize()
}

//
func (rf *Raft) StartSnapshot(db *map[string]string) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	rf.lastIndexTerm = rf.Logs[rf.getIndexInLog(rf.lastApplied)].Term
	rf.lastIndex = rf.lastApplied
	rf.Logs = make([]Entry, 0)

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)

	e.Encode(db)
	e.Encode(rf.lastIndex)
	e.Encode(rf.lastIndexTerm)

	rf.persister.SaveSnapshot(w.Bytes())
	//modified log

}
func (rf *Raft) readSnaptShot(data []byte) {
	db := make(map[string]string)
	if data == nil || len(data) < 1 {
		rf.lastIndex = 0
		rf.lastIndexTerm = 0
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	d.Decode(&db)
	d.Decode(&rf.lastIndex)
	d.Decode(&rf.lastIndexTerm)

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(&db)
	msg := ApplyMsg{
		-1,
		nil,
		true,
		w.Bytes(),
	}
	go func() {
		DPrintf("Sever %v read snapshot, log size: %v, LastIndex: %v", rf.me, len(rf.Logs), rf.lastIndex)
		rf.chanApply <- msg
	}()
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnaptArgs, reply *int) {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if rf.state != 2 {
			return
		}
		if *reply > rf.CurrentTerm {
			if rf.state == 2 {
				rf.toggleLeader <- true
			}
			rf.state = 0
			rf.CurrentTerm = *reply
			rf.votesCounter = 0
			rf.VoteFor = -1
			rf.persist()
			return
		}
		rf.nextIndex[server] = args.LastIncludedIndex + 1
		rf.matchIndex[server] = args.LastIncludedIndex

	} else {
		newargs := &InstallSnaptArgs{
			rf.CurrentTerm,
			rf.me,
			rf.lastIndex,
			rf.lastIndexTerm,
			rf.persister.ReadSnapshot(),
		}
		var newreply int
		go rf.sendInstallSnapshot(server, newargs, &newreply)
	}

}
func (rf *Raft) InstallSnapshot(args *InstallSnaptArgs, reply *int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	*reply = rf.CurrentTerm
	if args.Term < rf.CurrentTerm {
		return
	}
	rf.heartBeat <- true
	rf.state = 0
	rf.CurrentTerm = args.Term

	if args.LastIncludedIndex <= rf.lastIndex {
		rf.persist()
		return
	}

	rf.persister.SaveSnapshot(args.Data)
	//trucate log
	lastInLog := rf.getIndexInLog(args.LastIncludedIndex)
	rf.lastIndex = args.LastIncludedIndex
	rf.lastIndexTerm = args.LastIncludedTerm
	rf.lastApplied = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex
	if lastInLog+1 >= len(rf.Logs) {
		rf.Logs = make([]Entry, 0)
	} else {
		rf.Logs = rf.Logs[lastInLog+1:]
	}
	rf.persist()

	msg := ApplyMsg{
		-1,
		nil,
		true,
		args.Data,
	}
	DPrintf("Server %v install snaptshot from %v, Term %v", rf.me, args.LeaderId, args.Term)
	go func() {

		rf.chanApply <- msg
	}()
}
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Server %v is processing RequestVote from Server %v, Term %v, argsTerm %v\n", rf.me, args.CandidateId, rf.CurrentTerm, args.Term)
	if args.Term < rf.CurrentTerm {
		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm

		DPrintf("Server %v rejects RequestVote from Server %v, Term %v, argsTerm %v\n", rf.me, args.CandidateId, rf.CurrentTerm, args.Term)
		DPrintf("Server %v, State %v\n", rf.me, rf.state)

		return
	}
	defer rf.persist()
	if rf.CurrentTerm < args.Term {
		if rf.state == 2 {
			rf.toggleLeader <- true
		}
		rf.state = 0
		rf.CurrentTerm = args.Term
		rf.votesCounter = 0
		rf.VoteFor = -1
		DPrintf("Server %v becomes follower, Term %v, argsTerm %v\n", rf.me, rf.CurrentTerm, args.Term)
		DPrintf("Server %v, State %v\n", rf.me, rf.state)
	}
	reply.Term = rf.CurrentTerm
	reply.VoteGranted = false
	if rf.VoteFor == -1 || rf.VoteFor == args.CandidateId {
		// idx := len(rf.Logs) - 1
		// logTerm := rf.Logs[idx].Term
		idx := rf.lastIndex + len(rf.Logs)
		logTerm := rf.lastIndexTerm
		if len(rf.Logs) > 0 {
			logTerm = rf.Logs[len(rf.Logs)-1].Term
		}
		if logTerm < args.LastLogTerm || (logTerm == args.LastLogTerm && idx <= args.LastLogIndex) {
			reply.VoteGranted = true
			rf.VoteFor = args.CandidateId
			DPrintf("Server %v accepts RequestVote from Server %v, Term %v, argsTerm %v\n", rf.me, args.CandidateId, rf.CurrentTerm, args.Term)
			DPrintf("Server %v, State %v\n", rf.me, rf.state)

			rf.grantVote <- true
		} else {
			DPrintf("Server %v rejects to vote, lastTerm %v, LastIndex %v\n", rf.me, logTerm, idx)
			DPrintf("Candidate %v, lastTerm %v, LastIndex %v\n", args.CandidateId, args.LastLogTerm, args.LastLogIndex)
		}
	}

}

//AppendEntry ...
func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("Server %v is processing AppendEntry from Server %v, LogSize %v, Term %v, argsTerm %v\n", rf.me, len(rf.Logs), args.LeaderId, rf.CurrentTerm, args.Term)
	if rf.CurrentTerm > args.Term {
		reply.Success = false
		reply.Term = rf.CurrentTerm
		DPrintf("Server %v rejects AppendEntry from Server %v, Term %v, argsTerm %v\n", rf.me, args.LeaderId, rf.CurrentTerm, args.Term)
		DPrintf("Server %v, State %v\n", rf.me, rf.state)
		return
	}
	defer rf.persist()
	rf.heartBeat <- true
	if rf.CurrentTerm < args.Term {
		if rf.state == 2 {
			rf.toggleLeader <- true
		}
		rf.state = 0
		rf.CurrentTerm = args.Term
	}
	rf.state = 0
	rf.votesCounter = 0
	rf.VoteFor = args.LeaderId
	//if !(args.PrevLogIndex < len(rf.Logs)) {
	if args.PrevLogIndex > rf.lastIndex+len(rf.Logs) {
		reply.Success = false
		reply.Term = rf.CurrentTerm
		//reply.Index = len(rf.Logs)
		reply.Index = rf.lastIndex + 1
		DPrintf("Inconsistency happened--Server %v\n", rf.me)
		return
	}

	// if rf.Logs[args.PrevLogIndex].Term != args.PrevLogTerm {
	idx := rf.getIndexInLog(args.PrevLogIndex)
	//Observation: no way that index not in the log (instead, in snapshot) need to append
	if idx >= 0 && rf.Logs[idx].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.CurrentTerm
		DPrintf("###Inconsistency happened at index %v--Server %v, Leader %v, HeartBeat %v\n", args.PrevLogIndex, rf.me, args.LeaderId, args.Entries == nil)
		term := rf.Logs[idx].Term
		i := idx
		min := rf.getIndexInLog(rf.commitIndex)
		for i > min {
			if rf.Logs[i].Term != term {
				i++
				break
			}
			i--
		}
		if i == min {
			i++
		}
		DPrintf("###Move Index to index %v--Server %v, Leader %v, HeartBeat %v\n", rf.getRealIndex(i), rf.me, args.LeaderId, args.Entries == nil)
		reply.Index = rf.getRealIndex(i)
		reply.ConflictTerm = term
		return
	}
	//heart beat

	if args.Entries == nil {

		rf.state = 0
		reply.Success = true
		reply.Term = rf.CurrentTerm
		//	DPrintf("Server %v accepts heartbeat from Server %v, Term %v, argsTerm %v\n", rf.me, args.LeaderId, rf.CurrentTerm, args.Term)
		//	DPrintf("Server %v, State %v\n", rf.me, rf.state)
		//rf.heartBeat <- true
	} else {
		reply.Term = rf.CurrentTerm
		reply.Success = true
		//Append Entry
		//if args.PrevLogIndex+len(args.Entries) < len(rf.Logs) && args.Term == rf.Logs[len(rf.Logs)-1].Term {
		if args.PrevLogIndex+len(args.Entries) <= rf.lastIndex+len(rf.Logs) && (len(rf.Logs) == 0 || args.Term == rf.Logs[len(rf.Logs)-1].Term) {
			return
		}
		DPrintf("Server %v gets AppendEntry from Server %v, Term %v, argsTerm %v\n", rf.me, args.LeaderId, rf.CurrentTerm, args.Term)

		rf.Logs = rf.Logs[:(idx + 1)]
		for _, entry := range args.Entries {
			rf.Logs = append(rf.Logs, entry)
		}
		DPrintf("Appending success on Server %v， Last index %v\n", rf.me, rf.lastIndex+len(rf.Logs))

	}

	// if args.LeaderCommit > rf.commitIndex && rf.commitIndex < len(rf.Logs)-1 {
	if args.LeaderCommit > rf.commitIndex && rf.commitIndex < rf.lastIndex+len(rf.Logs) {
		if args.LeaderCommit > rf.lastIndex+len(rf.Logs) {
			rf.commitIndex = rf.lastIndex + len(rf.Logs)
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		rf.newCommit <- true
		//	DPrintf("Server %v sent commit signal\n", rf.me)
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

	//	DPrintf("Server %v is sending RequestVote to Server %v, Term %v\n", args.CandidateId, server, args.Term)

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if ok {
		if rf.state != 1 {
			return ok
		}
		if args.Term != rf.CurrentTerm {
			return ok
		}
		if reply.Term > rf.CurrentTerm {
			rf.CurrentTerm = reply.Term
			rf.state = 0
			rf.VoteFor = -1
			rf.votesCounter = 0
			return ok
		}
		if reply.VoteGranted {
			rf.votesCounter++
			if rf.state == 1 && rf.votesCounter > len(rf.peers)/2 {
				rf.state = 2
				//last index + 1
				//idx := len(rf.Logs)
				idx := rf.lastIndex + len(rf.Logs) + 1
				rf.nextIndex = rf.nextIndex[:0]
				rf.matchIndex = rf.matchIndex[:0]
				for _ = range rf.peers {
					rf.nextIndex = append(rf.nextIndex, idx)
					rf.matchIndex = append(rf.matchIndex, rf.lastIndex)
				}

				DPrintf("Server %v becomes leader, Term %v\n", rf.me, rf.CurrentTerm)
				DPrintf("Server %v, State %v\n", rf.me, rf.state)

				rf.toggleLeader <- true
				//DPrintf("send signal\n")
			}
		}
	} else {
		var termIdx, idx int
		if len(rf.Logs) == 0 {
			idx = rf.lastIndex
			termIdx = rf.lastIndexTerm
		} else {
			idx = rf.getRealIndex(len(rf.Logs) - 1)
			termIdx = rf.Logs[len(rf.Logs)-1].Term
		}
		newa := &RequestVoteArgs{
			rf.CurrentTerm,
			rf.me,
			idx,
			termIdx,
		}
		go func(a *RequestVoteArgs) {
			time.Sleep(200 * time.Millisecond)
			r := &RequestVoteReply{}
			rf.sendRequestVote(server, a, r)
		}(newa)
	}

	return ok
}
func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {

	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if ok {
		if rf.state != 2 {
			return ok
		}
		if args.Term != rf.CurrentTerm {
			return ok
		}
		if reply.Term > rf.CurrentTerm {
			rf.CurrentTerm = reply.Term
			if rf.state == 2 {
				DPrintf("-----------Server %v Lose Leader because out of date\n", rf.me)
				rf.toggleLeader <- true
			}
			rf.state = 0
			rf.VoteFor = -1
			rf.votesCounter = 0
			DPrintf("Server %v loses leader, Term %v\n", rf.me, rf.CurrentTerm)
			DPrintf("Server %v, State %v\n", rf.me, rf.state)
			return ok
		}
		//inconsis
		if !reply.Success {

			rf.nextIndex[server] = reply.Index
			i := rf.getIndexInLog(reply.Index)
			if i <= -1 {
				//InstallSnaptArgs()
				snapargs := &InstallSnaptArgs{
					rf.CurrentTerm,
					rf.me,
					rf.lastIndex,
					rf.lastIndexTerm,
					rf.persister.ReadSnapshot(),
				}
				var reply int
				go rf.sendInstallSnapshot(server, snapargs, &reply)
				return ok
			}
			// i == 0
			a := &AppendEntryArgs{
				rf.CurrentTerm,
				rf.me,
				rf.lastIndex,
				//rf.Logs[reply.Index-1].Term,
				//rf.Logs[reply.Index:],
				rf.lastIndexTerm,
				rf.Logs,
				rf.commitIndex,
			}
			if i > 0 {
				a.PrevLogIndex = reply.Index - 1
				a.PrevLogTerm = rf.Logs[i-1].Term
				a.Entries = rf.Logs[i:]
			}

			r := &AppendEntryReply{}
			go rf.sendAppendEntry(server, a, r)
		} else {
			if args.Entries != nil {
				DPrintf("Sever %v sent AppendEntry to Server %v. Index from %v to %v\n", rf.me, server, args.PrevLogIndex+1, args.PrevLogIndex+len(args.Entries))
				//replicate
				DPrintf("Replicate on Server %v success\n", server)
				rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
				rf.matchIndex[server] = rf.nextIndex[server] - 1
			}
			//if rf.commitIndex < len(rf.Logs)-1 {
			if rf.commitIndex < rf.getRealIndex(len(rf.Logs)-1) {
				go func() {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					nums := make([]int, len(rf.peers))
					copy(nums, rf.matchIndex)
					sort.Ints(nums)
					idx := nums[(len(nums)-1)/2]
					//Observation: commitIndex >= rf.lastIndex
					if idx > rf.lastIndex && idx > rf.commitIndex && rf.Logs[rf.getIndexInLog(idx)].Term == rf.CurrentTerm {
						rf.commitIndex = idx
						DPrintf("Leader set commitindex %v\n", idx)
						rf.newCommit <- true
					}
				}()
			}
		}

	} else if args.Entries != nil {

		newa := &AppendEntryArgs{
			args.Term,
			args.LeaderId,
			args.PrevLogIndex,
			args.PrevLogTerm,
			args.Entries,
			args.LeaderCommit,
		}
		go func(a *AppendEntryArgs) {
			r := &AppendEntryReply{}
			time.Sleep(150 * time.Millisecond)
			rf.sendAppendEntry(server, a, r)
		}(newa)
	}

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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	term, isLeader = rf.GetState()
	if !isLeader {
		return index, term, false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index = rf.getRealIndex(len(rf.Logs))
	rf.Logs = append(rf.Logs, Entry{rf.CurrentTerm, command})
	rf.persist()
	rf.replicate <- true
	return index, term, isLeader
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

	rf.chanApply = applyCh
	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnaptShot(persister.ReadSnapshot())

	rf.votesCounter = 0
	rf.state = 0
	rf.commitIndex = rf.lastIndex
	rf.lastApplied = rf.lastIndex

	rf.heartBeat = make(chan bool, 1)
	rf.grantVote = make(chan bool, 1)
	rf.toggleLeader = make(chan bool, 1)
	rf.newCommit = make(chan bool, 1)
	rf.replicate = make(chan bool)

	go func() {
		id := rf.me
		timer := time.NewTimer(time.Duration(rand.Int()%200+330) * time.Millisecond)
		for {
			select {
			case <-timer.C:
				rf.becomeCandidate()
				timer.Reset(time.Duration(rand.Int()%200+330) * time.Millisecond)
			case <-rf.grantVote:
				renew(timer, time.Duration(rand.Int()%200+330)*time.Millisecond)
			case <-rf.heartBeat:
				renew(timer, time.Duration(rand.Int()%200+330)*time.Millisecond)
			case <-rf.toggleLeader:
				rf.beLeader(id, &applyCh)
				renew(timer, time.Duration(rand.Int()%200+330)*time.Millisecond)
			case <-rf.newCommit:
				go rf.applyTomachine(&applyCh)
			}
		}
	}()
	return rf
}
func (rf *Raft) applyTomachine(ch *chan ApplyMsg) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.commitIndex <= rf.lastApplied {
		DPrintf("#########################Not apply")
		return
	}

	DPrintf("Server %v, CommitIndex %v, length %v, LastApplied %v\n", rf.me, rf.commitIndex, len(rf.Logs), rf.lastApplied)
	for i := rf.lastApplied + 1; i <= rf.commitIndex && i <= rf.lastIndex+len(rf.Logs); i++ {
		idx := rf.getIndexInLog(i)
		msg := ApplyMsg{
			i,
			rf.Logs[idx].Command,
			false,
			nil,
		}
		rf.lastApplied = i
		*ch <- msg

		DPrintf("##########################Server %v Apply entry with index %v\n", rf.me, i)
	}

}
func renew(t *time.Timer, interval time.Duration) {
	if !t.Stop() {
		<-t.C
	}
	t.Reset(interval)
}

func (rf *Raft) becomeCandidate() {
	rf.mu.Lock()
	rf.CurrentTerm++
	rf.state = 1
	DPrintf("Server %v becomes candidate, State %v, Term %v\n", rf.me, rf.state, rf.CurrentTerm)
	rf.VoteFor = rf.me
	rf.persist()
	//DPrintf("PERSIST STATE: Server %v, State %v, Term %v, LogSize %v\n", rf.me, rf.state, rf.CurrentTerm, len(rf.Logs))
	rf.votesCounter = 1
	cur := rf.CurrentTerm
	I := rf.me
	n := len(rf.peers)
	var termIdx, idx int
	if len(rf.Logs) == 0 {
		idx = rf.lastIndex
		termIdx = rf.lastIndexTerm
	} else {
		idx = rf.getRealIndex(len(rf.Logs) - 1)
		termIdx = rf.Logs[len(rf.Logs)-1].Term
	}

	rf.mu.Unlock()
	rva := &RequestVoteArgs{
		cur,
		I,
		idx,
		termIdx,
	}
	for i := 0; i < n; i++ {
		if i != I {
			go func(c int) {
				rvr := &RequestVoteReply{}
				rf.sendRequestVote(c, rva, rvr)
			}(i)
		}
	}
}

func (rf *Raft) beLeader(id int, ch *chan ApplyMsg) {
	DPrintf("Server %v Leader time\n", id)
	go rf.sendHeartBeat()
	fail := false
	timer := time.NewTimer(50 * time.Millisecond)
	for !fail {
		select {
		case <-timer.C:
			go rf.sendHeartBeat()
			timer.Reset(50 * time.Millisecond)
		case <-rf.newCommit:
			go rf.applyTomachine(ch)
		case <-rf.replicate:
			go rf.replicateLogAndCommit()
		case fail = <-rf.heartBeat:
		case fail = <-rf.toggleLeader:

		}
	}
	if !timer.Stop() {
		<-timer.C
	}

	DPrintf("Server %v REAL lose leader\n", id)

}
func (rf *Raft) sendHeartBeat() {
	rf.mu.Lock()
	me := rf.me
	n := len(rf.peers)
	args := &AppendEntryArgs{}
	args.Term = rf.CurrentTerm
	args.LeaderId = rf.me
	args.Entries = nil
	args.LeaderCommit = rf.commitIndex
	// args.PrevLogIndex = len(rf.Logs) - 1
	// args.PrevLogTerm = rf.Logs[args.PrevLogIndex].Term
	if len(rf.Logs) == 0 {
		args.PrevLogIndex = rf.lastIndex
		args.PrevLogTerm = rf.lastIndexTerm
	} else {
		args.PrevLogIndex = rf.lastIndex + len(rf.Logs)
		args.PrevLogTerm = rf.Logs[len(rf.Logs)-1].Term
	}

	rf.mu.Unlock()
	for i := 0; i < n; i++ {
		if i != me {
			go func(idx int) {
				reply := &AppendEntryReply{}
				rf.sendAppendEntry(idx, args, reply)
			}(i)
		}
	}
}
func (rf *Raft) replicateLogAndCommit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	me := rf.me
	n := len(rf.peers)

	DPrintf("##########Leader %v is sending AppendEntry#########\n", rf.me)

	for i := 0; i < n; i++ {
		if i != me {
			//lastIndex := len(rf.Logs) - 1
			next := rf.getIndexInLog(rf.nextIndex[i])
			//if lastIndex >= rf.nextIndex[i] {
			if next < 0 {
				// snapargs := &InstallSnaptArgs{
				// 	rf.CurrentTerm,
				// 	rf.me,
				// 	rf.lastIndex,
				// 	rf.lastIndexTerm,
				// 	rf.persister.ReadSnapshot(),
				// }
				// var reply int
				// go rf.sendInstallSnapshot(i, snapargs, &reply)
				continue
			}
			if len(rf.Logs) > next {
				//	DPrintf("lastindex %v, nextIndex %v\n", lastIndex, rf.nextIndex[i])
				prev := rf.nextIndex[i] - 1
				var prevTerm int
				if next == 0 {
					prevTerm = rf.lastIndexTerm
				} else {
					prevTerm = rf.Logs[next-1].Term
				}
				entry := rf.Logs[next:]

				DPrintf("Index FROM %v To %v Go Sever %v\n", prev+1, prev+len(entry), i)
				args := &AppendEntryArgs{
					rf.CurrentTerm,
					rf.me,
					prev,
					prevTerm,
					entry,
					rf.commitIndex,
				}
				go func(a *AppendEntryArgs, idx int) {
					reply := &AppendEntryReply{}
					rf.sendAppendEntry(idx, a, reply)
				}(args, i)
				//rf.nextIndex[i] = lastIndex + 1
			}
		} else {
			rf.matchIndex[i] = rf.getRealIndex(len(rf.Logs) - 1)
			rf.nextIndex[i] = rf.matchIndex[i] + 1
		}
	}

}
func (rf *Raft) getIndexInLog(index int) int {
	return index - rf.lastIndex - 1
}
func (rf *Raft) getRealIndex(index int) int {
	return index + rf.lastIndex + 1
}
