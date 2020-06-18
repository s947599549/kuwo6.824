package raft
//test gut
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
	applyCh        chan ApplyMsg  
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
	DPrintf("Candidate[raft%v][term:%v] request vote: raft%v[%v] 's term%v\n", args.CandidateId, args.Term, rf.me, rf.state, rf.curTerm)
	if args.Term<rf.curTerm||(args.Term == rf.curTerm && rf.voteFor != -1 && rf.voteFor != args.CandidateId){
		reply.Term=rf.curTerm 
		reply.VoteGranted=false;
		return

	}
	if args.Term>rf.curTerm{
		rf.curTerm=args.Term
		rf.switchStateTo(Follower)
	}
	LastLogIndex :=len(rf.log)-1
	if  args.LastLogTerm<rf.log[LastLogIndex].Term||
		(args.LastLogTerm==rf.log[LastLogIndex].Term &&args.LastLogIndex<(LastLogIndex)){
		reply.Term=rf.curTerm 
		reply.VoteGranted=false
		return 

	}
	rf.voteFor=args.CandidateId 
	reply.Term=rf.curTerm
	reply.VoteGranted = true
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
	DPrintf("leader[raft%v][term:%v] beat term:%v [raft%v][%v]\n", args.LeaderId, args.Term, rf.curTerm, rf.me, rf.state)
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
	// 2. Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	lastLogIndex :=len(rf.log)-1
	if lastLogIndex<args.PrevLogIndex{
		reply.Success=false
		reply.Term=rf.curTerm
		return
	}
	//3. If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	if rf.log[(args.PrevLogIndex)].Term!=args.PrevLogTerm{
		reply.Success=false
		reply.Term=rf.curTerm
		return 
	}
	// 4. Append any new entries not already in the log
	// compare from rf.log[args.PrevLogIndex + 1]
	unmatch_index := -1
	for idx:= range args.Entries{
		if len(rf.log)<(args.PrevLogIndex+2+idx) ||
			rf.log[(args.PrevLogIndex+1+idx)].Term!=args.Entries[idx].Term{
				unmatch_index=idx
				DPrintf("find diff index: %d\n",idx)
				break 
			}

		}

	if unmatch_index != -1{
		rf.log=rf.log[:(args.PrevLogIndex+1+unmatch_index)]
		rf.log=append(rf.log,args.Entries[unmatch_index:]...)
	}
	if args.LeaderCommit>rf.commitIndex{
		rf.setCommitIndex(min(args.LeaderCommit,len(rf.log)-1))
	}
	reply.Success=true  
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term=rf.curTerm
	isLeader=rf.state==Leader
	if isLeader{
		rf.log=append(rf.log,LogEntry{Command :command,Term:term})
		index=len(rf.log)-1
		rf.matchIndex[rf.me]=index
		rf.nextIndex[rf.me]=index+1
	}

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
	rf.voteFor =-1
	rf.heartbeatTimer=time.NewTimer(HEART_BEAT_TIMEOUT*time.Millisecond)
	rf.electionTimer=time.NewTimer(randTimeDuration())
	rf.applyCh=applyCh 
	rf.log=make([]LogEntry,1)
	rf.nextIndex=make([]int,len(rf.peers))
	rf.matchIndex=make([]int,len(rf.peers))
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
					rf.HeartBeartTo()
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
	DPrintf("Term %d: server %d convert from %v to %v\n", rf.curTerm, rf.me, rf.state, stat)
	rf.state=stat 
	switch stat{
	case Follower:
			rf.heartbeatTimer.Stop()
			rf.electionTimer.Reset(randTimeDuration())
			DPrintf("raft%v become follower in term:%v\n", rf.me, rf.curTerm)
			rf.voteFor=-1

		
	case Leader:
			DPrintf("raft%v become leader in term:%v\n", rf.me, rf.curTerm)
			for i:=range rf.nextIndex{
				rf.nextIndex[i]=(len(rf.log))
			}
			for i:=range rf.matchIndex{
				rf.matchIndex[i]=0
			}
			rf.electionTimer.Stop()
			rf.HeartBeartTo()
			rf.heartbeatTimer.Reset(HEART_BEAT_TIMEOUT*time.Millisecond)
		
	case Candidate:
			rf.restarElection()
			DPrintf("raft%v become Candidate in term:%v\n", rf.me, rf.curTerm) 
			
	}
}
func  (rf *Raft)HeartBeartTo(){
	for i:=range rf.peers{
		if i!=rf.me{
			go rf.HeartBearts(i)
		}
	}

}
func (rf *Raft)HeartBearts(server int){
	rf.mu.Lock()
	if rf.state!=Leader{
		rf.mu.Unlock()
		return

	}
	prevLogIndex:= rf.nextIndex[server]-1
	entries:=make([]LogEntry,len(rf.log[(prevLogIndex+1):]))
	copy(entries,rf.log[(prevLogIndex+1):])
	args:=AppendEntriesArgs{
		Term:     rf.curTerm,
		LeaderId: rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm: rf.log[(prevLogIndex)].Term,
		Entries:     entries,
		LeaderCommit:  rf.commitIndex,
	}
	rf.mu.Unlock()
	var reply AppendEntriesReply
	if rf.sendAppendEntries(server,&args,&reply){
		rf.mu.Lock()
		if rf.state!=Leader{
			rf.mu.Unlock()
			return 
		}
		if reply.Success{
			rf.matchIndex[server]=args.PrevLogIndex+len(args.Entries)
			rf.nextIndex[server]=rf.matchIndex[server]+1

			// If there exists an N such that N > commitIndex, a majority
			// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
			// set commitIndex = N (§5.3, §5.4)
			for N:=(len(rf.log)-1);N>rf.commitIndex;N--{
				count :=0
				DPrintf("Term %d: server %d recived success %d count %d", rf.curTerm, rf.me, N, count)
				for _,matchIndex:=range rf.matchIndex{
					if matchIndex>=N{
						count+=1
					}
				}
				if count>len(rf.peers)/2{
					rf.setCommitIndex(N)
					break 
				}

			}

		}else{
			if reply.Term>rf.curTerm{
				rf.curTerm=reply.Term
				rf.switchStateTo(Follower)
			}else{
				rf.nextIndex[server]=args.PrevLogIndex-1
			}
		}
		rf.mu.Unlock()
	}/*
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	prevLogIndex := rf.nextIndex[server] - 1

	// use deep copy to avoid race condition
	// when override log in AppendEntries()
	entries := make([]LogEntry, len(rf.log[(prevLogIndex+1):]))
	copy(entries, rf.log[(prevLogIndex+1):])

	args := AppendEntriesArgs{
		Term:         rf.curTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.log[(prevLogIndex)].Term,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	var reply AppendEntriesReply
	if rf.sendAppendEntries(server, &args, &reply) {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		// If last log index ≥ nextIndex for a follower: send
		// AppendEntries RPC with log entries starting at nextIndex
		// • If successful: update nextIndex and matchIndex for
		// follower (§5.3)
		// • If AppendEntries fails because of log inconsistency:
		// decrement nextIndex and retry (§5.3)
		if reply.Success {
			// successfully replicated args.Entries
			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[server] = rf.matchIndex[server] + 1

			// If there exists an N such that N > commitIndex, a majority
			// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
			// set commitIndex = N (§5.3, §5.4).
			for N := (len(rf.log) - 1); N > rf.commitIndex; N-- {
				count := 0
				for _, matchIndex := range rf.matchIndex {
					if matchIndex >= N {
						count += 1
					}
				}

				if count > len(rf.peers)/2 {
					// most of nodes agreed on rf.log[i]
					rf.setCommitIndex(N)
					break
				}
			}

		} else {
			if reply.Term > rf.curTerm {
				rf.curTerm = reply.Term
				rf.switchStateTo(Follower)
			} else {
				//如果走到这个分支，那一定是需要前推
				rf.nextIndex[server] = args.PrevLogIndex - 1
			}
		}
		rf.mu.Unlock()
	}*/
}

func randTimeDuration() time.Duration {
	return time.Duration(HEART_BEAT_TIMEOUT*3+rand.Intn(HEART_BEAT_TIMEOUT)) * time.Millisecond
}
func (rf *Raft)restarElection(){
	//DPrintf("raft%v is starting election\n", rf.me)
	rf.curTerm+=1
	rf.voteFor=rf.me
	rf.voteCount=1
	rf.electionTimer.Reset(randTimeDuration())
	for i,_:=range rf.peers{
		if i!=rf.me{
			go func(peer int){
				rf.mu.Lock()
				lastLogIndex:=len(rf.log)-1
				args:=RequestVoteArgs{
					Term: rf.curTerm,
					CandidateId: rf.me, 
					LastLogIndex: lastLogIndex,
					LastLogTerm:  rf.log[lastLogIndex].Term,
				}
			        rf.mu.Unlock()
				var reply RequestVoteReply 
				if rf.sendRequestVote(peer,&args,&reply){
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if reply.Term>rf.curTerm{
						rf.curTerm=reply.Term 
						rf.switchStateTo(Follower)
					}
					if reply.VoteGranted&&rf.state==Candidate{
						rf.voteCount++
						if rf.voteCount>len(rf.peers)/2{
							rf.switchStateTo(Leader)
						}
					}
				}
			}(i)
			
		}
	}
}
func (rf  *Raft)setCommitIndex(min int){
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
}
