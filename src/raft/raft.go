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
import "labrpc"
import "time"
import "math/rand"
import "fmt"

// import "bytes"
// import "encoding/gob"

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

type LogEntry struct {
	term    int
	command interface{}
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

	state   int   // 0 for follower; 1 for candidate; 2 for leader
	timeout int64 //

	recvHeartBeatCh chan int
	electCh         chan bool
	heartBeatCh     chan int

	currentLeader int

	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	fmt.Printf("check %d if leader: ", rf.me)

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	if rf.state == 2 {
		isleader = true
	} else {
		isleader = false
	}

	fmt.Printf("%t\n", isleader)
	fmt.Printf("%d state is %d, term is %d\n", rf.me, rf.state, rf.currentTerm)
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

func max(x, y int) int {
	if x < y {
		return y
	}
	return x
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	currentTerm, _ := rf.GetState()
	reply.Term = currentTerm
	fmt.Printf("args.Term is %d, currentTerm is %d\n", args.Term, currentTerm)
	if args.Term < currentTerm {
		reply.VoteGranted = false
		return
	}

	if rf.votedFor != -1 && args.Term <= rf.currentTerm {
		reply.VoteGranted = false
		rf.mu.Lock()
		rf.currentTerm = max(args.Term, currentTerm)
		rf.mu.Unlock()
	} else {
		rf.mu.Lock()
		rf.state = 0
		rf.mu.Unlock()
		reply.VoteGranted = true
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
	fmt.Printf("begin sendRequestVote\n")

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	fmt.Printf("%d request %d vote, res is %t\n", rf.me, server, reply.VoteGranted)
	if ok && reply.VoteGranted {
		rf.electCh <- true
	} else {
		rf.electCh <- false
	}
	return ok
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	fmt.Printf("sendAppendEntries: args = %p\n", args)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	fmt.Printf("AppendEntries: args = %p\n", args)
	reply.Success = true
	reply.Term = rf.currentTerm

	rf.mu.Lock()
	rf.state = 0
	rf.timeout = randomRange(200, 400)
	rf.mu.Unlock()
	rf.recvHeartBeatCh <- 1
	return

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
	} else {
		reply.Success = true
		reply.Term = rf.currentTerm

		rf.mu.Lock()
		rf.currentLeader = args.LeaderId
		rf.votedFor = args.LeaderId
		rf.state = 0
		rf.timeout = randomRange(200, 400)
		rf.mu.Unlock()
		rf.recvHeartBeatCh <- 1
	}
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

func (rf *Raft) oneRoundElection() bool {
	fmt.Print("begin one round election\n")

	rf.mu.Lock()
	rf.state = 1
	rf.votedFor = rf.me
	rf.timeout = randomRange(200, 400)
	rf.currentTerm++
	rf.mu.Unlock()

	res := false

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			args := &RequestVoteArgs{}
			args.Term = rf.currentTerm
			args.CandidateId = rf.me
			var reply RequestVoteReply
			go rf.sendRequestVote(i, args, &reply)
		}
	}

	done := 1
	for i := 0; i < len(rf.peers); i++ {
		fmt.Printf("enter %d times\n", i)
		select {
		case ok := <-rf.electCh:
			if ok {
				fmt.Printf("%d get one vote\n", rf.me)
				done++
				if rf.currentLeader > -1 {
					res = true
					rf.mu.Lock()
					rf.state = 0
					rf.mu.Unlock()
					break
				}
				if done >= (len(rf.peers)/2 + 1) {
					rf.mu.Lock()
					rf.state = 2
					rf.heartBeatCh <- 1
					rf.currentLeader = rf.me
					rf.mu.Unlock()

					_, tres := rf.GetState()
					fmt.Printf("%d is leader? %t\n", rf.me, tres)
					res = true
					fmt.Printf("last i = %d\n", i)
					break
				}
			}
		}

		if res {
			break
		}
	}

	return res
}

func (rf *Raft) election() {
	for {
		select {
		// 1. waiting for timeout
		case <-time.After(time.Duration(rf.timeout) * time.Millisecond):
			fmt.Print("waiting time out!\n")
		case <-rf.recvHeartBeatCh:
			continue
		}

		res := false
		for !res {
			res = rf.oneRoundElection()
		}

	}
}

func (rf *Raft) sendHeartBeat() {
	<-rf.heartBeatCh
	for {
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				args := &AppendEntriesArgs{}
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				var reply AppendEntriesReply

				go rf.sendAppendEntries(i, args, &reply)

			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func randomRange(min, max int64) int64 {
	return rand.Int63n(max-min) + min
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

	// Your initialization code here (2A, 2B, 2C).

	rf.state = 0 // initial with follower
	rf.currentLeader = -1
	rf.currentTerm = 0
	rf.recvHeartBeatCh = make(chan int)
	rf.electCh = make(chan bool)
	rf.heartBeatCh = make(chan int)

	rand.Seed(time.Now().UnixNano())
	rf.timeout = randomRange(200, 400)

	go rf.election()
	go rf.sendHeartBeat()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
