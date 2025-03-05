package raft

import (
	"go-raft/types"
)

type AppendEntriesArgs struct {
	Term         int     // leader’s term
	LeaderId     string  // so follower can redirect clients
	PrevLogIndex int     // index of log entry immediately preceding new ones
	PrevLogTerm  int     // term of prevLogIndex entry
	Entries      []Entry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int     // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	XTerm   int  // Follower中与Leader冲突的Log对应的Term
	XIndex  int  // Follower中，对应Term为XTerm的第一条Log条目的索引
	XLen    int  // Follower的log的长度
}

//发送rpc不能加锁
//leader获取锁来发送rpc（在还没收到回复的时候），导致某些节点没有及时收到心跳，发起新的选举，leader这边收到vote请求又要获取锁进行处理回复
//而其他节点没收到回复不会释放锁，也无法回复上一次的心跳
//造成死锁

func (rf *Raft) sendAppendEntries(serverTo string, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[serverTo].Call("Raft.AppendEntries", args, reply)
	DPrintf("sendAppendEntries: serverTo %d, args %v, reply %v, ok %v\n", serverTo, args, reply, ok)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	DPrintf("AppendEntries: server %v 尝试获取锁: AppendEntries\n", rf.me)
	defer func() {
		rf.mu.Unlock()
		DPrintf("AppendEntries: server %v 解锁: AppendEntries\n", rf.me)
	}()
	DPrintf("AppendEntries: received args %v\n", args)

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		DPrintf("AppendEntries: args.Term < currentTerm, rejecting. Reply: %v\n", reply)
		return
	}
	rf.resetVoteTimer()

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = "Denied"
		rf.role = Follower
		rf.persist()
		DPrintf("AppendEntries: Term updated to %d, switched to Follower. Current vote: %v\n", rf.currentTerm, rf.votedFor)
	}

	// 初始化是否有冲突
	isConflict := false

	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.Success = true
		reply.Term = rf.currentTerm
		DPrintf("AppendEntries: PrevLogIndex < lastIncludedIndex, reply.success = true\n")
		return
	} else if args.PrevLogIndex >= rf.getVirtualLogIndex(len(rf.log)) {
		reply.XTerm = -1
		reply.XLen = rf.getVirtualLogIndex(len(rf.log))
		isConflict = true
		DPrintf("AppendEntries: server %v log conflict at PrevLogIndex %v, XLen %v\n", rf.me, args.PrevLogIndex, reply.XLen)
	} else if rf.log[rf.getRealLogIndex(args.PrevLogIndex)].Term != args.PrevLogTerm {
		reply.XTerm = rf.log[rf.getRealLogIndex(args.PrevLogIndex)].Term
		i := args.PrevLogIndex
		for i > rf.commitIndex && rf.log[rf.getRealLogIndex(i)].Term == reply.XTerm {
			i -= 1
		}
		reply.XIndex = i + 1
		reply.XLen = rf.getVirtualLogIndex(len(rf.log))
		isConflict = true
		DPrintf("AppendEntries: log conflict at PrevLogIndex %v, Term mismatch: args.PrevLogTerm %v vs actual term %v, reply.XTerm %v\n", args.PrevLogIndex, args.PrevLogTerm, rf.log[rf.getRealLogIndex(args.PrevLogIndex)].Term, reply.XTerm)
	}

	if isConflict {
		reply.Term = rf.currentTerm
		reply.Success = false
		DPrintf("AppendEntries: log inconsistency detected, reply.success = false\n")
		return
	}

	for idx, log := range args.Entries {
		index := rf.getRealLogIndex(args.PrevLogIndex) + 1 + idx
		if index < len(rf.log) && rf.log[index].Term != log.Term {
			rf.log = rf.log[:index]
			rf.isNodesChange(args.Entries[idx:], true)
			rf.log = append(rf.log, args.Entries[idx:]...)
			DPrintf("AppendEntries: log conflict at index %v, truncating log\n", index)
			break
		} else if index == len(rf.log) {
			rf.isNodesChange(args.Entries[idx:], true)
			rf.log = append(rf.log, args.Entries[idx:]...)
			DPrintf("AppendEntries: appending log at index %v\n", index)
			break
		}
	}
	if len(args.Entries) != 0 {
		rf.persist()
	}

	reply.Success = true
	reply.Term = rf.currentTerm
	DPrintf("AppendEntries: logs appended successfully, reply.success = true\n")

	//NodesChange

	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > rf.getVirtualLogIndex(len(rf.log)-1) {
			rf.commitIndex = rf.getVirtualLogIndex(len(rf.log) - 1)
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		rf.applyCond.Signal()
		DPrintf("AppendEntries: updated commitIndex to %v, applying logs\n", rf.commitIndex)
	}
	return
}

func (rf *Raft) processAppendEntries(peer string, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(peer, args, reply)
	if !ok {
		DPrintf("processAppendEntries: failed to send to peer %v\n", peer)
		return
	}
	rf.mu.Lock()
	DPrintf("processAppendEntries: server %v 尝试获取锁: processAppendEntries\n", rf.me)
	defer func() {
		rf.mu.Unlock()
		DPrintf("processAppendEntries: server %v 解锁: processAppendEntries\n", rf.me)
	}()

	if rf.role != Leader || args.Term != rf.currentTerm {
		DPrintf("processAppendEntries: not leader or term mismatch. role: %v, args.Term: %v, currentTerm: %v\n", rf.role, args.Term, rf.currentTerm)
		return
	}

	if reply.Success {
		newMatchIndex := args.PrevLogIndex + len(args.Entries)
		if newMatchIndex > rf.matchIndex[peer] {
			rf.matchIndex[peer] = newMatchIndex
		}
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
		DPrintf("processAppendEntries: peer %v, updated matchIndex to %v, nextIndex to %v\n", peer, rf.matchIndex[peer], rf.nextIndex[peer])

		newCommitIndex := rf.getVirtualLogIndex(len(rf.log) - 1)
		//找到第一条还没应用的日志
		for newCommitIndex > rf.commitIndex {
			count := 1
			for peerID, _ := range rf.peers {
				if peerID == rf.me {
					continue
				}
				if rf.matchIndex[peerID] >= newCommitIndex && rf.log[rf.getRealLogIndex(newCommitIndex)].Term == rf.currentTerm {
					count++
				}
			}
			if count > len(rf.peers)/2 {
				break
			}
			newCommitIndex--
		}
		isNodeChange, op := rf.isNodesChange(args.Entries, false)
		if isNodeChange && op.OpType == types.OPNodesToMidStateChange {
			if !rf.sendNewNodesGroupLog(op) {
				return
			}
		}
		rf.commitIndex = newCommitIndex
		rf.applyCond.Signal()
		DPrintf("processAppendEntries: new commitIndex %v, applying logs\n", rf.commitIndex)
		return
	}

	if reply.Term > rf.currentTerm {
		rf.role = Follower
		rf.currentTerm = reply.Term
		rf.votedFor = "Denied"
		rf.resetVoteTimer()
		rf.persist()
		DPrintf("processAppendEntries: term increased, switched to Follower. New term %v\n", rf.currentTerm)
		return
	}

	if reply.Term == rf.currentTerm && rf.role == Leader {
		if reply.XTerm == -1 {
			if rf.lastIncludedIndex >= reply.XLen {
				rf.nextIndex[peer] = rf.lastIncludedIndex
			} else {
				rf.nextIndex[peer] = reply.XLen
			}
			DPrintf("processAppendEntries: no conflict, updated nextIndex %v\n", rf.nextIndex[peer])
			return
		}

		index := rf.nextIndex[peer] - 1
		if index < rf.lastIncludedIndex {
			index = rf.lastIncludedIndex
		}
		for index > rf.lastIncludedIndex && rf.log[rf.getRealLogIndex(index)].Term > reply.XTerm {
			index--
		}
		if index == rf.lastIncludedIndex && rf.log[rf.getRealLogIndex(index)].Term > reply.XTerm {
			rf.nextIndex[peer] = rf.lastIncludedIndex
		} else if rf.log[rf.getRealLogIndex(index)].Term == reply.XTerm {
			rf.nextIndex[peer] = index + 1
		} else {
			if reply.XIndex <= rf.lastIncludedIndex {
				rf.nextIndex[peer] = rf.lastIncludedIndex
			} else {
				rf.nextIndex[peer] = reply.XIndex
			}
		}
		DPrintf("processAppendEntries: adjusted nextIndex to %v\n", rf.nextIndex[peer])
		return
	}
}

func (rf *Raft) sendHeartBeats() {
	for !rf.killed() {
		<-rf.heartTimer.C
		rf.mu.Lock()
		role := rf.role
		DPrintf("sendHeartBeats: server %v 尝试获取锁: sendHeartBeats\n", rf.me)
		rf.mu.Unlock()
		if role != Leader {
			return
		}
		if len(rf.peers) == 0 && rf.getVirtualLogIndex(len(rf.log)-1) > rf.commitIndex {
			DPrintf("sendHeartBeats: no peer to send heartbeat\n", rf.me)
			rf.commitIndex = rf.getVirtualLogIndex(len(rf.log) - 1)
			rf.applyCond.Signal()
		}
		for i, _ := range rf.peers {
			if i == rf.me {
				continue
			}
			rf.mu.Lock()
			DPrintf("sendHeartBeats: server %v 尝试获取锁: sendHeartBeats (inner)\n", rf.me)
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[i] - 1,
				LeaderCommit: rf.commitIndex,
			}
			sendSnapShotFlag := false
			if rf.nextIndex[i]-1 < rf.lastIncludedIndex {
				sendSnapShotFlag = true
				DPrintf("sendHeartBeats: sending snapshot to peer %v\n", i)
			} else if rf.getVirtualLogIndex(len(rf.log)-1) > rf.nextIndex[i]-1 {
				args.Entries = rf.log[rf.getRealLogIndex(rf.nextIndex[i]):]
			} else {
				args.Entries = make([]Entry, 0)
			}
			rf.mu.Unlock()
			if sendSnapShotFlag {
				go rf.sendSnapShot(i)
			} else {
				rf.mu.Lock()
				args.PrevLogTerm = rf.log[rf.getRealLogIndex(args.PrevLogIndex)].Term
				rf.mu.Unlock()
				go rf.processAppendEntries(i, args)
			}
		}
		rf.resetHeartTimer(50)
	}
}
