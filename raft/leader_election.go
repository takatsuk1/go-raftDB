package raft

type RequestVoteArgs struct {
	Term         int
	CandidateId  string
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) sendRequestVote(server string, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) Elect() {
	rf.mu.Lock()

	rf.currentTerm++
	rf.role = Candidate
	rf.votedFor = rf.me
	rf.voteCount = 1
	rf.persist()

	voteInput := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getVirtualLogIndex(len(rf.log) - 1),
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}

	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.collectVote(i, voteInput)
	}
	return
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	if args.Term > rf.currentTerm {
		rf.votedFor = "Denied"
		rf.role = Follower
		rf.currentTerm = args.Term
	}
	rf.mu.Unlock()

	rf.mu.Lock()

	if rf.votedFor == "Denied" || rf.votedFor == args.CandidateId {
		if args.LastLogTerm > rf.log[len(rf.log)-1].Term ||
			(args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= rf.getVirtualLogIndex(len(rf.log)-1)) {
			rf.votedFor = args.CandidateId
			rf.currentTerm = args.Term
			rf.role = Follower
			reply.VoteGranted = true
			reply.Term = rf.currentTerm
			rf.mu.Unlock()
			rf.resetVoteTimer()
			return
		}
	}
	rf.mu.Unlock()

	rf.mu.Lock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	rf.mu.Unlock()
}

func (rf *Raft) collectVote(serverTo string, args *RequestVoteArgs) {
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(serverTo, args, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Candidate || args.Term != rf.currentTerm {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = "Denied"
		rf.role = Follower
		rf.persist()
	}

	if reply.VoteGranted != true {
		return
	}

	rf.voteCount++

	if rf.voteCount > len(rf.peers)/2 {
		if rf.role != Candidate || rf.currentTerm != args.Term {
			return
		}
		rf.role = Leader
		for i, _ := range rf.peers {
			rf.nextIndex[i] = rf.getVirtualLogIndex(len(rf.log))
			rf.matchIndex[i] = rf.lastIncludedIndex
		}
		go rf.sendHeartBeats()
	}
}
