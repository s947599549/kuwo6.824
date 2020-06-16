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
import(
	"time"
	"math/rand"
)
// import "bytes"
// import "labgob" 

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}
type LogEntry struct {
	Command interface{}
	Term    int
}

const (
	Follower           = 1
	Candidate          = 2
	Leader             = 3
	HEART_BEAT_TIMEOUT = 100 //心跳超时，要求1秒10次，所以是100ms一次
)


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
	electionTimer  *time.Timer // 选举定时器
	heartbeatTimer *time.Timer // 心跳定时器
	state          int         // 角色
	voteCount      int         //投票数

	curTerm int//latest term server has seen (initialized to 0 on first boot, increases monotonically)
	voteFor int//candidateId that received vote in current term(or null if none)
	log     []LogEntry//log entries; each entry contains command for state machine, and term when entry was received by leader(first index is 1)

	commitIndex  int//index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied   int//index of highest log entry applied to state machine (initialized to 0, increases monotonically
	//Volatile state on leaders:(Reinitialized after election)
	nextIndex   []int //for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int //for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term=rf.curTerm
	isleader=rf.state==Leader
	rf.mu.Unlock()
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
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.



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
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term  int//candidate's term
	CandidateId  int//candidaterequesting vote
	LastLogIndex  int//index of candidiate's last log entry
	LastLogTerm    int  //term of cnadidate's last log entry
	
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term    int  //currentTerm ,for candidate to update itself
	VoteGranted   bool //true  means   candidate receiva vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Candidate[reft%v][term:%v] request vote : reft%v[%v]'s term&v,args.CandidateId,args.Term,rf.me,rf.stat")
	reply.VoteGranted=false 
	if args.Term<rf.curTerm{
		reply.Term=rf.curTerm 
		reply.VoteGranted=false;
		return

	}
	if args.Term>rf.curTerm{
		rf.curTerm=args.Term
		rf.switchStateTo(Follower)


	}
	if (rf.voteFor==-1||rf.voteFor==args.CandidateId)/*&& (rf.lastApplied == args.LastLogIndex && rf.log[rf.lastApplied].Term == args.LastLogTerm)*/{
		rf.voteFor=args.CandidateId
		reply.VoteGranted=true 

	}
	reply.Term=rf.curTerm
	rf.electionTimer.Reset(randTimeDuration())
}
type AppendEntriesArgs struct {
	Term         int        //leader’s term
	LeaderId     int        //so follower can redirect clients
	PrevLogIndex int        //index of log entry immediately preceding new ones
	PrevLogTerm  int        //term of prevLogIndex entry
	Entries      []LogEntry //log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        //leader’s commitIndex
}
type AppendEntriesReply struct {
	Term    int  //currentTerm, for leader to update itself
	Success bool //true if follower contained entry matching prevLogIndex and prevLogTerm
}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = true
	if args.Term < rf.curTerm {
		// Reply false if term < currentTerm (§5.1)
		reply.Success = false
		reply.Term = rf.curTerm 
		return
	} 
	if args.Term > rf.curTerm {
		//If RPC request or response contains term T > currentTerm:set currentTerm = T, convert to follower (§5.1)
		rf.curTerm = args.Term
		rf.switchStateTo(Follower)
	}
	rf.electionTimer.Reset(randTimeDuration())
}
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
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	DPrintf("create raft%v...", me)
	rf.state=Follower
	rf.curTerm =0
	rf.voteFor =-1
	rf.heartbeatTimer=time.NewTimer(HEART_BEAT_TIMEOUT*time.Millisecond)
	rf.electionTimer=time.NewTimer(randTimeDuration())
	go func(){

		for{
			select {
			case <-rf.electionTimer.C:
				rf.mu.Lock()
				switch rf.state{
				case Follower:
					rf.switchStateTo(Candidate)
				case Candidate:
					rf.restarElection()
				}
				rf.mu.Unlock()
			
			case <-rf.heartbeatTimer.C:
				rf.mu.Lock()
				if rf.state==Leader{
					rf.HeartBearts()
					rf.heartbeatTimer.Reset(HEART_BEAT_TIMEOUT*time.Millisecond)
				}
				rf.mu.Unlock()
				}
		}

	}()

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}

func (rf* Raft)switchStateTo(stat int){
	if stat==rf.state{
		return 
	}
	rf.state=stat 
	switch stat{
	case Follower:
			rf.heartbeatTimer.Stop()
			rf.electionTimer.Reset(randTimeDuration())
			DPrintf("raft%v become follower in term:%v\n", rf.me, rf.curTerm)
			rf.voteFor=-1

		
	case Leader:
			DPrintf("raft%v become leader in term:%v\n", rf.me, rf.curTerm)
			rf.electionTimer.Stop()
			rf.HeartBearts()
		
	case Candidate:
			rf.restarElection()
			DPrintf("raft%v become Candidate in term:%v\n", rf.me, rf.curTerm) 
			
	}
}
func (rf *Raft)HeartBearts(){
	for peer :=range rf.peers{
		if peer!=rf.me{
			go  func(peer int){
				args:=AppendEntriesArgs{}
				rf.mu.Lock()
				args.Term=rf.curTerm
				args.LeaderId=rf.me
				rf.mu.Unlock()
				reply:=AppendEntriesReply{}
				if rf.sendAppendEntries(peer,&args,&reply){
					rf.mu.Lock()
					if reply.Term>rf.curTerm{
 						
						rf.curTerm=reply.Term
						rf.switchStateTo(Follower)
					}
					rf.mu.Unlock()
				}
				
			}(peer)
		}
	}
	time.Sleep(HEART_BEAT_TIMEOUT * time.Millisecond)
}
func randTimeDuration() time.Duration {
	return time.Duration(HEART_BEAT_TIMEOUT*3+rand.Intn(HEART_BEAT_TIMEOUT)) * time.Millisecond
}
func (rf *Raft)restarElection(){
	rf.curTerm+=1
	rf.voteFor=rf.me
	rf.electionTimer.Reset(randTimeDuration())
	rf.voteCount=1
	for peer,_:=range rf.peers{
		if peer!=rf.me{
			go func(peer int){
				rf.mu.Lock()
				args:=RequestVoteArgs{}
				args.Term=rf.curTerm
				args.CandidateId=rf.me 
				rf.mu.Unlock()
				reply:=RequestVoteReply{}
				if rf.sendRequestVote(peer,&args,&reply){
					rf.mu.Lock()
					if reply.VoteGranted&&rf.state==Candidate{
						rf.voteCount+=1
						if rf.voteCount>len(rf.peers)/2{
							rf.switchStateTo(Leader)
						}
					}else if reply.Term>rf.curTerm{
						rf.curTerm=reply.Term 
						rf.switchStateTo(Follower)
					}
					rf.mu.Unlock()
				}else{

					// DPrintf("raft%v[%v] vote:raft%v no reply, currentTerm:%v\n", rf.me, rf.state, peer, rf.currentTerm)}
				}
			}(peer)
			
		}
	}
}
/*func (rf  *Raft)setCommitIndex(min int){
	rf.commitIndex=min
	//If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3)
	
	if  rf.commitIndex>rf.lastApplied{
		DPrintf("%v apply from index %d to %d", rf, rf.lastApplied+1, rf.commitIndex)
		entriesToApply := append([]LogEntry{}, rf.log[(rf.lastApplied+1):(rf.commitIndex+1)]...)
		go func(start int,entries []LogEntry){
			for idx,entry := range entries{
				var apply ApplyMsg
				apply.CommandValid=true
				apply.Command=entry.Command 
				apply.CommandIndex=start+idx
				rf.applyCh <- apply 
				// do not forget to update lastApplied index
				// this is another goroutine, so protect it with lock
				rf.mu.Lock()
				if rf.lastApplied < apply.CommandIndex {
					rf.lastApplied = apply.CommandIndex
				}
				rf.mu.Unlock()

			}
		}(rf.lastApplied+1,entriesToApply)
	}



}
func min(x, y int) int {
	if x < y {
		return x
	} else {
		return y
	}
}*/
