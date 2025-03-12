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
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
	}()

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	rf.resetVoteTimer()

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = "Denied"
		rf.role = Follower
		rf.persist()
	}

	// 初始化是否有冲突
	isConflict := false

	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.Success = true
		reply.Term = rf.currentTerm
		return
	} else if args.PrevLogIndex >= rf.getVirtualLogIndex(len(rf.log)) {
		reply.XTerm = -1
		reply.XLen = rf.getVirtualLogIndex(len(rf.log))
		isConflict = true
	} else if rf.log[rf.getRealLogIndex(args.PrevLogIndex)].Term != args.PrevLogTerm {
		reply.XTerm = rf.log[rf.getRealLogIndex(args.PrevLogIndex)].Term
		i := args.PrevLogIndex
		for i > rf.commitIndex && rf.log[rf.getRealLogIndex(i)].Term == reply.XTerm {
			i -= 1
		}
		reply.XIndex = i + 1
		reply.XLen = rf.getVirtualLogIndex(len(rf.log))
		isConflict = true

	}

	if isConflict {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	for idx, log := range args.Entries {
		index := rf.getRealLogIndex(args.PrevLogIndex) + 1 + idx
		if index < len(rf.log) && rf.log[index].Term != log.Term {
			rf.log = rf.log[:index]
			rf.isNodesChange(args.Entries[idx:], true)
			rf.log = append(rf.log, args.Entries[idx:]...)
			break
		} else if index == len(rf.log) {
			rf.isNodesChange(args.Entries[idx:], true)
			rf.log = append(rf.log, args.Entries[idx:]...)
			break
		}
	}
	if len(args.Entries) != 0 {
		rf.persist()
	}

	reply.Success = true
	reply.Term = rf.currentTerm

	//NodesChange

	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > rf.getVirtualLogIndex(len(rf.log)-1) {
			rf.commitIndex = rf.getVirtualLogIndex(len(rf.log) - 1)
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		rf.applyCond.Signal()
	}
	return
}

func (rf *Raft) processAppendEntries(peer string, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(peer, args, reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
	}()

	if rf.role != Leader || args.Term != rf.currentTerm {
		return
	}

	if reply.Success {
		newMatchIndex := args.PrevLogIndex + len(args.Entries)
		if newMatchIndex > rf.matchIndex[peer] {
			rf.matchIndex[peer] = newMatchIndex
		}
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1

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
		return
	}

	if reply.Term > rf.currentTerm {
		rf.role = Follower
		rf.currentTerm = reply.Term
		rf.votedFor = "Denied"
		rf.resetVoteTimer()
		rf.persist()
		return
	}

	if reply.Term == rf.currentTerm && rf.role == Leader {
		if reply.XTerm == -1 {
			if rf.lastIncludedIndex >= reply.XLen {
				rf.nextIndex[peer] = rf.lastIncludedIndex
			} else {
				rf.nextIndex[peer] = reply.XLen
			}
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
		return
	}
}

func (rf *Raft) sendHeartBeats() {
	for !rf.killed() {
		<-rf.heartTimer.C
		rf.mu.Lock()
		role := rf.role
		rf.mu.Unlock()
		if role != Leader {
			return
		}
		if len(rf.peers) == 0 && rf.getVirtualLogIndex(len(rf.log)-1) > rf.commitIndex {
			rf.commitIndex = rf.getVirtualLogIndex(len(rf.log) - 1)
			rf.applyCond.Signal()
		}
		for i, _ := range rf.peers {
			if i == rf.me {
				continue
			}
			rf.mu.Lock()
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[i] - 1,
				LeaderCommit: rf.commitIndex,
			}
			sendSnapShotFlag := false
			if rf.nextIndex[i]-1 < rf.lastIncludedIndex {
				sendSnapShotFlag = true
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
