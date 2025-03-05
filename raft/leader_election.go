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
	DPrintf("sendRequestVote: server %d, args %v, reply %v, ok %v\n", server, args, reply, ok)
	return ok
}
func (rf *Raft) Elect() {
	DPrintf("Elect: server %v 尝试获取锁: Elect\n", rf.me)
	rf.mu.Lock()
	DPrintf("Elect: server %v 成功获取锁: Elect\n", rf.me)

	rf.currentTerm++
	rf.role = Candidate
	rf.votedFor = rf.me
	rf.voteCount = 1
	rf.persist()
	DPrintf("Elect: currentTerm %d, role %v, votedFor %d\n", rf.currentTerm, rf.role, rf.votedFor)

	voteInput := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getVirtualLogIndex(len(rf.log) - 1),
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	DPrintf("Elect: voteInput %v\n", voteInput)

	rf.mu.Unlock()
	DPrintf("Elect: server %v 解锁: Elect\n", rf.me)

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.collectVote(i, voteInput)
	}
	return
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	DPrintf("RequestVote: server %v 尝试获取锁: RequestVote\n", rf.me)
	rf.mu.Lock()
	DPrintf("RequestVote: server %v 成功获取锁: RequestVote\n", rf.me)

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		DPrintf("RequestVote: args.Term < currentTerm, rejecting vote. Reply: %v\n", reply)
		rf.mu.Unlock()
		DPrintf("RequestVote: server %v 解锁: RequestVote\n", rf.me)
		return
	}

	if args.Term > rf.currentTerm {
		rf.votedFor = "Denied"
		rf.role = Follower
		rf.currentTerm = args.Term
		DPrintf("RequestVote: Term updated to %d, switched to Follower. Current vote: %v\n", rf.currentTerm, rf.votedFor)
	}
	rf.mu.Unlock()
	DPrintf("RequestVote: server %v 解锁: RequestVote 之后, 进入再次加锁\n", rf.me)

	rf.mu.Lock()
	DPrintf("RequestVote: server %v 再次获取锁: RequestVote\n", rf.me)

	if rf.votedFor == "Denied" || rf.votedFor == args.CandidateId {
		if args.LastLogTerm > rf.log[len(rf.log)-1].Term ||
			(args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= rf.getVirtualLogIndex(len(rf.log)-1)) {
			rf.votedFor = args.CandidateId
			rf.currentTerm = args.Term
			rf.role = Follower
			reply.VoteGranted = true
			reply.Term = rf.currentTerm
			DPrintf("RequestVote: vote granted. New vote state: %v\n", rf.votedFor)
			rf.mu.Unlock()
			DPrintf("RequestVote: server %v 解锁: RequestVote\n", rf.me)
			rf.resetVoteTimer()
			return
		}
	}
	rf.mu.Unlock()
	DPrintf("RequestVote: server %v 解锁: RequestVote 之后, 没有投票\n", rf.me)

	rf.mu.Lock()
	DPrintf("RequestVote: server %v 重新获取锁: RequestVote\n", rf.me)
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	rf.mu.Unlock()
	DPrintf("RequestVote: server %v 解锁: RequestVote\n", rf.me)
}

func (rf *Raft) collectVote(serverTo string, args *RequestVoteArgs) {
	DPrintf("collectVote: server %v 尝试获取锁: collectVote\n", rf.me)
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(serverTo, args, &reply)
	if !ok {
		DPrintf("collectVote: server %v 请求投票失败: %v\n", rf.me, serverTo)
		return
	}

	rf.mu.Lock()
	DPrintf("collectVote: server %v 成功获取锁: collectVote\n", rf.me)
	defer rf.mu.Unlock()

	if rf.role != Candidate || args.Term != rf.currentTerm {
		DPrintf("collectVote: server %v 当前角色或任期与预期不符，忽略投票\n", rf.me)
		return
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = "Denied"
		rf.role = Follower
		rf.persist()
		DPrintf("collectVote: server %v 发现更高的任期，转为 Follower\n", rf.me)
	}

	if reply.VoteGranted != true {
		DPrintf("collectVote: server %v 投票未获批准\n", rf.me)
		return
	}

	rf.voteCount++
	DPrintf("collectVote: server %v voteCount: %d\n", rf.me, rf.voteCount)

	if rf.voteCount > len(rf.peers)/2 {
		if rf.role != Candidate || rf.currentTerm != args.Term {
			DPrintf("collectVote: role or term mismatch during election. Aborting.\n")
			return
		}
		rf.role = Leader
		for i, _ := range rf.peers {
			rf.nextIndex[i] = rf.getVirtualLogIndex(len(rf.log))
			rf.matchIndex[i] = rf.lastIncludedIndex
		}
		DPrintf("collectVote: server %v elected as leader. Initializing indices.\n", rf.me)
		go rf.sendHeartBeats()
	}
	DPrintf("collectVote: server %v 解锁: collectVote\n", rf.me)
}
