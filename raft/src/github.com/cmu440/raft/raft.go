//
// raft.go
// =======
// Write your code in this file
// We will use the original version of all other
// files for testing
//

package raft

//
// API
// ===
// This is an outline of the API that your raft implementation should
// expose.
//
// rf = Make(...)
//   Create a new Raft peer.
//
// rf.Start(command interface{}) (index, term, isleader)
//   Start agreement on a new log entry
//
// rf.GetState() (me, term, isLeader)
//   Ask a Raft peer for "me" (see line 58), its current term, and whether it thinks it
//   is a leader
//
// ApplyMsg
//   Each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (e.g. tester) on the
//   same peer, via the applyCh channel passed to Make()
//

import (
	"github.com/cmu440/rpc"
	"math/rand"
	"sync"
	"time"
	"fmt"
)

const (
	Follower = iota
	Candidate
	Leader
)
const (
	Null = -1
)

//
// ApplyMsg
// ========
//
// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same peer, via the applyCh passed to Make()
//
type ApplyMsg struct {
	Index   int
	Command interface{}
}

//
type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
	//add command later
}

//
// Raft struct
// ===========
//
// A Go object implementing a single Raft peer
//
type Raft struct {
	mux   sync.Mutex       // Lock to protect shared access to this peer's state
	peers []*rpc.ClientEnd // RPC end points of all peers
	me    int              // this peer's index into peers[]

	role          int //either Leader, Candidate or Follower as defined above
	votesReceived int //number of votes got so far

	//persistent state on all servers
	currentTerm int        //lastest term server has seen
	votedFor    int        //candidateId that received vote in current term
	logs        []LogEntry //log entries containing command for state machine, term received and index

	//volatile state on all servers
	commitIndex int //index of highest log entry known to be committed
	lastApplied int //index of highest log entry applied to state machine, initialized to be 0

	//volatile state on leaders
	nextIndex  []int //for each peer server, index of next log entry to send to that server, initialized to last log index +1
	matchIndex []int //for each peer server, index of highest log entry known to be replicated on server

	//>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Channels<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
	applyChan chan ApplyMsg
	//getState()
	closeChan       chan int
	stateChan       chan int
	stateReturnChan chan *ServerStates

	// AppendEntries(), when received appendEntry command
	appendLogChan       chan *AppendArgReply //take entry and potentially update log
	appendLogFinishChan chan int             //signal finished processing of append Entry, changing fields of reply

	//sendAppendEntries(), send + getting response in reply struct
	appendResponseChan chan *AppendArgReply //after got response, let mainroutine handle it

	//RequestVote(), server received requestVote() , need to vote or reject
	voteForChan        chan *VoteArgReply //let mainRoutine decide on voting or not
	voteForProcessChan chan int           //signal RequestVote that main routine processed the vote

	//sendRequestVote(), send vote to a peer, wait for response in reply struct and deal with it
	receiveVoteChan chan *VoteArgReply //tell server received Vote for the request

	//Start() related channels
	startRequestChan chan interface{}    //send command to main routine
	startRespondChan chan *startResponse //signal finished appending comman as new entry to leader's log
	//appendProcessChan chan int after done with append request, signal AppendEntries
	//receiveVoteProcessChan chan int //signal sendRequestVote that main routine processed vote

}

func makeNewRaft() *Raft {
	var logs []LogEntry
	rf := &Raft{
		currentTerm:         0,
		votedFor:            Null,
		role:                Follower,
		votesReceived:       0,
		commitIndex:         0,
		lastApplied:         0,
		logs:                logs,
		stateChan:           make(chan int),
		closeChan:           make(chan int),
		stateReturnChan:     make(chan *ServerStates),
		appendLogChan:       make(chan *AppendArgReply),
		appendLogFinishChan: make(chan int),
		appendResponseChan:  make(chan *AppendArgReply),
		voteForChan:         make(chan *VoteArgReply),
		receiveVoteChan:     make(chan *VoteArgReply),
		voteForProcessChan:  make(chan int),
		startRequestChan:    make(chan interface{}),
		startRespondChan:    make(chan *startResponse),
		//receiveVoteProcessChan: make(chan int),
	}
	return rf
}

//
// GetState()
// ==========
//
// Return "me", current term and whether this peer
// believes it is the leader
//
func (rf *Raft) GetState() (int, int, bool) {

	var me int
	var term int
	var isleader bool
	// Your code here (2A)
	rf.stateChan <- 1
	states := <-rf.stateReturnChan
	me = states.me
	term = states.term
	isleader = states.isleader
	return me, term, isleader
}

type ServerStates struct {
	me       int
	term     int
	isleader bool
}

type startResponse struct {
	term     int
	index    int
	isLeader bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int //leader's commit index
}
type AppendEntriesReply struct {
	Term      int
	Success   bool
	NextIndex int //client's next index to be sent things to
}
type AppendArgReply struct {
	args  *AppendEntriesArgs
	reply *AppendEntriesReply
	peer  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	arg_reply := &AppendArgReply{
		args:  args,
		reply: reply,
	}
	rf.appendLogChan <- arg_reply
	//fmt.Println("start processing appendLog")
	<-rf.appendLogFinishChan
}
func (rf *Raft) sendAppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// if len(args.Entries) > 0 {
	// 	fmt.Println("args has entry length: ",len(args.Entries))
	// }

	ok := rf.peers[peer].Call("Raft.AppendEntries", args, reply)

	if ok {
		//fmt.Println("sendAppendEntries: Got response for append with Entry length: ", len(args.Entries))
		arg_reply := &AppendArgReply{
			args:  args,
			reply: reply,
			peer:  peer,
		}
		rf.appendResponseChan <- arg_reply
	}

	return ok
}

//
// RequestVoteArgs
// ===============
//
// Example RequestVote RPC arguments structure
//
// Please note
// ===========
// Field names must start with capital letters!
//

type RequestVoteArgs struct {
	// Your data here (2A, 2B)
	//checkpoint
	Term        int //candidate's term
	CandidateId int //candidate id

	//complete parts
	LastLogIndex int
	LastLogTerm  int
}

//
// RequestVoteReply
// ================
//
// Example RequestVote RPC reply structure.
//
// Please note
// ===========
// Field names must start with capital letters!
//
//
type RequestVoteReply struct {
	// Your data here (2A)
	Term    int  //currentTerm of voter
	Success bool //whether voter accepted or rejected
}

type VoteArgReply struct {
	args  *RequestVoteArgs
	reply *RequestVoteReply
}

//
// RequestVote
// ===========
//
// Example RequestVote RPC handler
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B)
	arg_reply := &VoteArgReply{
		args:  args,
		reply: reply,
	}
	rf.voteForChan <- arg_reply
	<-rf.voteForProcessChan
}

//
// sendRequestVote
// ===============
//
// Example code to send a RequestVote RPC to a peer
//
// peer int -- index of the target peer in
// rf.peers[]
//
// args *RequestVoteArgs -- RPC arguments in args
//
// reply *RequestVoteReply -- RPC reply
//
// The types of args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers)
//
// The rpc package simulates a lossy network, in which peers
// may be unreachable, and in which requests and replies may be lost
//
// Call() sends a request and waits for a reply
//
// If a reply arrives within a timeout interval, Call() returns true;
// otherwise Call() returns false
//
// Thus Call() may not return for a while
//
// A false return can be caused by a dead peer, a live peer that
// can't be reached, a lost request, or a lost reply
//
// Call() is guaranteed to return (perhaps after a delay)
// *except* if the handler function on the peer side does not return
//
// Thus there
// is no need to implement your own timeouts around Call()
//
// Please look at the comments and documentation in ../rpc/rpc.go
// for more details
//
// If you are having trouble getting RPC to work, check that you have
// capitalized all field names in the struct passed over RPC, and
// that the caller passes the address of the reply struct with "&",
// not the struct itself
//
func (rf *Raft) sendRequestVote(peer int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[peer].Call("Raft.RequestVote", args, reply)
	if ok {
		arg_reply := &VoteArgReply{
			args:  args,
			reply: reply,
		}
		rf.receiveVoteChan <- arg_reply
	}

	return ok
}

//
// Start
// =====
//
// The service using Raft (e.g. a k/v peer) wants to start
// agreement on the next command to be appended to Raft's log
//
// If this peer is not the leader, return false
//
// Otherwise start the agreement and return immediately
//
// There is no guarantee that this command will ever be committed to
// the Raft log, since the leader may fail or lose an election
//
// The first return value is the index that the command will appear at
// if it is ever committed
//
// The second return value is the current term
//
// The third return value is true if this peer believes it is
// the leader
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	isLeader := false
	_, _, isLeader = rf.GetState()
	if isLeader {
		rf.startRequestChan <- command
		response := <-rf.startRespondChan
		if response.isLeader {
			//fmt.Println("Leader:", rf.me, "got log: ",response.index, "in term: ",response.term)
			return response.index, response.term, true
		} else {
			//fmt.Println("Not Leader:", rf.me, "got log: ",response.index, "in term: ",response.term,)
			return -1, -1, false
		}
	} else {
		return -1, -1, false
	}

}

//
// Kill
// ====
//
// The tester calls Kill() when a Raft instance will not
// be needed again
//
// You are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance
//
func (rf *Raft) Kill() {
	rf.closeChan <- 1
	// Your code here, if desired
}

//
// Make
// ====
//
// The service or tester wants to create a Raft peer
//
// The port numbers of all the Raft peers (including this one)
// are in peers[]
//
// This peer's port is peers[me]
//
// All the peers' peers[] arrays have the same order
//
// applyCh
// =======
//
// applyCh is a channel on which the tester or service expects
// Raft to send ApplyMsg messages
//
// Make() must return quickly, so it should start Goroutines
// for any long-running work
//
func Make(peers []*rpc.ClientEnd, me int, applyCh chan ApplyMsg) *Raft {
	rf := makeNewRaft()
	rf.peers = peers
	rf.me = me
	rf.applyChan = applyCh
	//fmt.Println("Time start is", time.Now())

	// Your initialization code here (2A, 2B)
	go rf.mainRoutine()

	return rf
}

func randElectionTimeOut() time.Duration {
	dur := rand.Int31()%101 + 300
	//fmt.Println("randElectionTimeOut is:",dur)
	return time.Duration(dur) * time.Millisecond
}
func (rf *Raft) mainRoutine() {
	for {
		select {
		case <-rf.closeChan:
			return
		case <-rf.stateChan:
			isLeader := rf.role == Leader
			states := &ServerStates{
				me:       rf.me,
				term:     rf.currentTerm,
				isleader: isLeader,
			}
			rf.stateReturnChan <- states
		default:
			//check if commitIndex > lastApplied, if so apply log[lastApplied+1]
			//to applyCh, apply multiple at a time if necessary
			for rf.commitIndex > rf.lastApplied {
				rf.lastApplied += 1
				if rf.lastApplied > len(rf.logs) {
					panic("sth wrong with commitIndex")
				}
				fmt.Println("Server: ",rf.me, "Commited Index: ", rf.lastApplied, "with term: ",rf.currentTerm)
				msg := ApplyMsg{
					Index:   rf.lastApplied,
					Command: rf.logs[rf.lastApplied-1].Command,
				}
				//fmt.Println("Server: ", rf.me, "commited Index: ", rf.lastApplied, "for command: ", rf.logs[rf.lastApplied-1].Command)
				rf.applyChan <- msg
			}
			switch rf.role {
			case Follower:
				rf.FollowerRoutine()
			case Candidate:
				rf.CandidateRoutine()
			case Leader:
				rf.LeaderRoutine()
			}
		}
		//no mutex here assuming no race condition with vote and append Handler

	}
}

func (rf *Raft) LeaderRoutine() {
	//start := time.Now()

	rf.updateCommitIndex()
	timer := time.NewTimer(time.Duration(100) * time.Millisecond)

	select {
	case <-rf.voteForChan:

		rf.voteForProcessChan <- 1
		//fmt.Println(">>>>>>>>>>>>>>>>>>>>>>>Time elapsed in voteForChan: ", time.Since(start).Seconds())

	case <-rf.receiveVoteChan:
		//fmt.Println(">>>>>>>>>>>>>>>>>>>>>>>Time elapsed in receiveVoteChan: ", time.Since(start).Seconds())
	case arg_reply := <-rf.appendLogChan:
		//reply := arg_reply.reply
		term := arg_reply.args.Term
		if term == rf.currentTerm {
			panic("Two leaders at the same time")
		}
		//reply.Term = rf.currentTerm
		rf.checkAppendLog(arg_reply)
	case arg_reply := <-rf.appendResponseChan:

		args := arg_reply.args
		reply := arg_reply.reply
		peer := arg_reply.peer
		if reply.Term > rf.currentTerm {
			//fmt.Println("TURNED TO FOLLOWER!!!!!!!!")
			rf.prepFollower(reply.Term, Null)
			return
		}
		//fmt.Println("got response:", reply.Success, " replyNextindex is: ", reply.NextIndex, " from peer: ", peer, " entry length: ",len(args.Entries))
		if reply.Success && len(args.Entries) > 0 { //log update succeed, need to change nextIndex rn
			updateIndex := args.Entries[len(args.Entries)-1].Index
			//fmt.Println("matchIndex:",updateIndex, "update for peer:", peer)
			rf.nextIndex[peer] = updateIndex + 1
			rf.matchIndex[peer] = updateIndex
		} else if reply.Success == false {
			rf.nextIndex[peer] = reply.NextIndex
		}

		//fmt.Println(">>>>>>>>>>>>>>>>>>>>>>>Time elapsed in appendResponseChan: ", time.Since(start).Seconds())
	case command := <-rf.startRequestChan:
		index := -1
		term := rf.currentTerm
		isLeader := true
		log := rf.lastLog()
		if log == nil {
			index = 1
		} else {
			index = log.Index + 1
		}

		newLog := LogEntry{
			Index:   index,
			Term:    term,
			Command: command,
		}
		rf.logs = append(rf.logs, newLog)
		//fmt.Println("At server ", rf.me, ",logs now length: ", len(rf.logs), " New Index: ", rf.logs[index-1].Index, " Term: ",  rf.logs[index-1].Term)
		response := &startResponse{
			index:    index,
			term:     term,
			isLeader: isLeader,
		}
		rf.startRespondChan <- response
		rf.sendLogOrHeartbeat()

	case <-timer.C:

		rf.sendLogOrHeartbeat()
		return

	}
}

func (rf *Raft) CandidateRoutine() {
	timer := time.NewTimer(randElectionTimeOut())
	select {
	case arg_reply := <-rf.voteForChan:
		rf.decideToVote(arg_reply)
	case arg_reply := <-rf.receiveVoteChan: //got reply for vote
		//args := arg_reply.args
		reply := arg_reply.reply

		if reply.Term > rf.currentTerm {
			rf.prepFollower(reply.Term, Null)
			return
		}

		if reply.Term == rf.currentTerm && reply.Success {
			rf.votesReceived += 1
			if rf.votesReceived*2 > len(rf.peers) {
				rf.prepLeader()
			}
		}

	case <-rf.appendResponseChan:
	case <-rf.startRequestChan: //reject start request
		response := &startResponse{}
		rf.startRespondChan <- response
	case arg_reply := <-rf.appendLogChan:
		//fmt.Println("Candidate: ", rf.me, "received appendLog")
		rf.checkAppendLog(arg_reply)
	case <-timer.C:
		//fmt.Println("split vote!")
		rf.prepCandidate() //do it again with added term number

	}
}

func (rf *Raft) FollowerRoutine() {
	timer := time.NewTimer(randElectionTimeOut())
	select {
	case arg_reply := <-rf.voteForChan:
		rf.decideToVote(arg_reply)
	case <-rf.receiveVoteChan: //ignore if receive Votes
		//rf.receiveVoteProcessChan <- 1
	case <-rf.appendResponseChan:
	case <-rf.startRequestChan: //reject start request
		response := &startResponse{}
		rf.startRespondChan <- response

	case arg_reply := <-rf.appendLogChan:
		//fmt.Println("Follower: ", rf.me, "received APPENDLog")

		rf.checkAppendLog(arg_reply)
	case <-timer.C: //become candidate itself
		//fmt.Println("time out not getting heartbeats, peer:", rf.me)
		rf.prepCandidate()
	}
}

//go through each peer to see if there are pending logs that isn't sent yet
func (rf *Raft) sendLogOrHeartbeat() {

	log := rf.lastLog()
	lastIndex := -1
	if log == nil {
		lastIndex = 0
	} else {
		lastIndex = log.Index
		//fmt.Println("sendLog: log not empty with lastLogIndex:", log.Index, "lastLogTerm: ", log.Term)
	}

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		prevTerm := -1
		prevIndex := -1
		if rf.nextIndex[i] == 0 {
			panic("sth wrong with nextIndex of peer being 0??")
		} else if rf.nextIndex[i] == 1 {
			prevIndex = 0
			prevTerm = 0
		} else {
			prevIndex = rf.nextIndex[i] - 1
			prevTerm = rf.logs[prevIndex-1].Term
		}
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			LeaderCommit: rf.commitIndex,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevTerm,
		}

		if lastIndex >= rf.nextIndex[i] {

			var entries []LogEntry
			//fmt.Println("sendLog: prevLogIndex: ", args.PrevLogIndex, "lastIndex: ", lastIndex)

			//here prevLogIndex +1 = the index of log that we try to add to entries
			// -1 to compensate for the shift in logs, resulting in prevLogIndex it self
			for i := args.PrevLogIndex; i < lastIndex; i++ {
				entries = append(entries, rf.logs[i])
			}
			//fmt.Println("entry has length: ", len(entries))
			args.Entries = entries
			reply := &AppendEntriesReply{}
			//fmt.Println("send appendLog to peer:", i)
			go rf.sendAppendEntries(i, args, reply)
		} else { //send Heartbeat
			//fmt.Println("send Heartbeat to peer:", i)
			var entries []LogEntry
			args.Entries = entries
			reply := &AppendEntriesReply{}
			go rf.sendAppendEntries(i, args, reply)
		}
	}
}

//find if there exists such N that N > commitIndex
//majority of matchIndex[i] >= N
//and log[N].term == currentTerm
//set commitIndex = N if so
func (rf *Raft) updateCommitIndex() {
	log := rf.lastLog()

	if log == nil { //no log entries yet
		if rf.commitIndex != 0 {
			panic("leader's commitIndex not 0 when no logs appended")
		}
		return //nothing to do if no logs?
	}
	//fmt.Println("updateCommitIndex: log not empty with lastLogIndex:", log.Index, "lastLogTerm: ", log.Term)

	lastIndex := log.Index
	count := 0
	for lastIndex > rf.commitIndex {
		count = 1
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			if rf.matchIndex[i] >= lastIndex && rf.logs[lastIndex-1].Term == rf.currentTerm {
				count += 1
			}
		}
		if count*2 > len(rf.peers) {
			rf.commitIndex = lastIndex
			//fmt.Println("CommitIndex for all now is: ", rf.commitIndex, " from Leader: ", rf.me)
			return
		}
		lastIndex -= 1
	}
	//if gets here, then means no bigger N found, just don't change commitIndex
}
func min(x int, y int) int {
	if x < y {
		return x
	}
	return y
}

func (rf *Raft) checkAppendLog(arg_reply *AppendArgReply) {
	log := rf.lastLog()
	index := 0
	if log != nil {
		index = log.Index
	}
	//fmt.Println("started checkAppendLog: ")
	args := arg_reply.args
	reply := arg_reply.reply
	term := args.Term

	//check term
	if term > rf.currentTerm {
		rf.prepFollower(term, Null)
	} else if term < rf.currentTerm { //append from previous Term
		reply.NextIndex = 1 + index
		reply.Success = false
		reply.Term = rf.currentTerm
		rf.appendLogFinishChan <- 1
		return
	}
	//now args.Term == rf.currentTerm
	reply.Term = rf.currentTerm
	//check log index match up

	prevLogIndex := args.PrevLogIndex
	if prevLogIndex < 0 { //sanity check, should be at least 0
		panic("prevLogIndex < 0")
	}
	if prevLogIndex > len(rf.logs) { //server's log too short
		reply.Success = false
		reply.NextIndex = 1 + index
		rf.appendLogFinishChan <- 1
		return
	} else {

		//check for term agreement in the prevLogIndex if have
		//previous entry already
		if prevLogIndex > 0 {
			myLog := rf.logs[prevLogIndex-1]
			if args.PrevLogTerm != myLog.Term { //should reject
				//find index that posses a smaller term
				//since current myLog.Term doesn't agree with leader and
				//need to be replaced, that index + 1 should be the nextIndex
				//that leader sends the future appendEntry for
				index := prevLogIndex
				//guarantee to be prevLogIndex at most to overwrite the current prevLogIndex that doesn't match term
				for i := 0; i < prevLogIndex; i++ {
					if rf.logs[i].Term == myLog.Term {
						//first same term seen, means the left bound of logs with this term that's not supposed to be in logs
						index = i + 1
						break
					}
				}
				//fmt.Println("Log mismatch for server: ",rf.me, " before term was: ", myLog.Term, " now is term: ", args.PrevLogTerm, " with nextIndex: ", index)
				reply.NextIndex = index
				rf.appendLogFinishChan <- 1
				return
			}
		}
		if len(args.Entries) == 0 { //normal heartbeat?
			log := rf.lastLog()
			lastIndex := 0
			if log != nil {
				lastIndex = log.Index
			}
			if args.LeaderCommit > rf.commitIndex { // update commitIndex
				rf.commitIndex = min(args.LeaderCommit, lastIndex)
			}
			reply.NextIndex = lastIndex + 1
			reply.Success = true
			reply.Term = rf.currentTerm
			rf.appendLogFinishChan <- 1
			return
		}
		//Now either prev Log term/index agree or don't have prevLog term/index
		// so should append/overwrite the entries

		reply.Success = true

		//recreate new log to account for potentially more than one
		//log, and potentially overwritting the old entry
		var newLog []LogEntry
		for i := 0; i < prevLogIndex; i++ {
			newLog = append(newLog, rf.logs[i])
		}
		//since heartbeat has prevLogIndex, then if term doesn't check out
		//it would cut off the log or update nextIndex correctly
		// if len(newLog)==0 && len(args.Entries) >0 {
		// 	fmt.Println("Got long append entry from leader: ", args.LeaderId, " at server: ", rf.me, "for log length: ", len(args.Entries))
		// }
		newLog = append(newLog, args.Entries...)

		rf.logs = newLog
		log := rf.lastLog() //could be nil when receive heartbeat with empty log
		if log != nil {
			reply.NextIndex = log.Index + 1
		} else {
			reply.NextIndex = 1
		}

		//fmt.Println("now newLog has length: ", len(rf.logs), " for peer: ", rf.me, "with NextIndex: ", reply.NextIndex)
		if args.LeaderCommit > rf.commitIndex { // update commitIndex
			//fmt.Println("Now commitIndex is: ", rf.commitIndex, "leaderCommitIndex is: ", args.LeaderCommit)
			// if len(newLog) == len(args.Entries){
			// 	fmt.Println("Now commitIndex is: ", rf.commitIndex, "leaderCommitIndex is: ", args.LeaderCommit, log.Index)
			// }
			rf.commitIndex = min(args.LeaderCommit, log.Index)

		}
		//fmt.Println("Now commitIndex is: ", rf.commitIndex, "leaderCommitIndex is: ", args.LeaderCommit)
		rf.appendLogFinishChan <- 1
		return
	}
}
func (rf *Raft) decideToVote(arg_reply *VoteArgReply) {
	args := arg_reply.args
	reply := arg_reply.reply

	log := rf.lastLog()
	upToDate := false
	//check candidate's log is up to date when:
	//	1. larger term for last log
	//	2. equal term but larger index
	if log != nil {

		if args.LastLogTerm == log.Term && args.LastLogIndex >= log.Index {
			upToDate = true
		}
		if args.LastLogTerm > log.Term {
			upToDate = true
		}
	} else { //trivially true since empty log for the server
		upToDate = true
	}

	if args.Term < rf.currentTerm { //requestVote not up to date
		reply.Success = false

	} else if args.Term > rf.currentTerm { //if server not up to date

		//become follower and update term no matter what
		rf.currentTerm = args.Term
		rf.votedFor = Null
		rf.role = Follower

		//only vote for it if up to date
		if upToDate {
			rf.votedFor = args.CandidateId
			reply.Success = true
		}
	} else { //equal terms
		//if not voted for anyone or voted for this candidate already
		//vote for this candidate then
		if (rf.votedFor == Null || rf.votedFor == args.CandidateId) && upToDate {
			rf.currentTerm = args.Term
			rf.votedFor = args.CandidateId
			reply.Success = true
			rf.role = Follower
			//uneccessary, do I really need it? Since candidate won't have votedFor equal to the requester's ID and leader shouldn't receive request with equal term
		}

	}
	reply.Term = rf.currentTerm
	rf.voteForProcessChan <- 1
	return
}

func (rf *Raft) lastLog() *LogEntry {
	n := len(rf.logs)
	if n == 0 {
		return nil
	}
	// if rf.logs[n-1].Index != n {
	// 	//fmt.Println("log Index: ", rf.logs[n-1].Index, "length: ", n, "for role: ", rf.me)
	// 	panic("Index not correct for log!!!!")
	// }
	return &rf.logs[n-1]
}

func (rf *Raft) prepLeader() {
	rf.role = Leader
	index := -1
	log := rf.lastLog()
	if log == nil {
		index = 0
	} else {
		index = log.Index
	}
	//fmt.Println("Server ", rf.me, "become leader, ", "in term: ", rf.currentTerm, "with length: ",index)

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = index + 1
		rf.matchIndex[i] = 0
	}
}
func (rf *Raft) prepFollower(term int, votedFor int) {
	rf.role = Follower

	rf.votedFor = votedFor
	rf.currentTerm = term
	rf.votesReceived = 0
}

//become candidate and send out vote requests once
func (rf *Raft) prepCandidate() {
	index := -1
	term := -1
	log := rf.lastLog()
	if log == nil {
		index = 0
		term = 0
	} else {
		index = log.Index
		term = log.Term
	}
	rf.role = Candidate
	//vote for itself first
	rf.votesReceived = 1
	rf.votedFor = rf.me
	rf.currentTerm += 1
	//fmt.Println("Become a candidate", rf.me, "in term: ",rf.currentTerm, "with log length: ", len(rf.logs))

	for peerIndex, _ := range rf.peers {
		if peerIndex == rf.me || rf.role != Candidate {
			continue
		}
		args := &RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: index,
			LastLogTerm:  term,
		}
		reply := &RequestVoteReply{}
		go rf.sendRequestVote(peerIndex, args, reply)

	}
}
