package raft

import (
	"bytes"
	"encoding/gob"
)

type InstallSnapshotArgs struct {
	Term              int           // leader’s term
	LeaderId          string        // so follower can redirect clients
	LastIncludedIndex int           // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int           // term of lastIncludedIndex snapshot file
	Data              *SnapshotInfo //[] raw bytes of the snapshot chunk
	Done              bool
}

type SnapshotInfo struct {
	LastIndex   uint64
	LastTerm    uint64
	LastApplied uint64
	VotedFor    string
	CurrentTerm uint64
	Log         []byte
	DbData      []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) sendInstallSnapshot(serverTo string, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	ok := rf.peers[serverTo].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	//快照任期过期
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = "Denied"
	}

	rf.role = Follower
	rf.resetVoteTimer()

	//快照日志索引过期
	if args.LastIncludedIndex < rf.lastIncludedIndex || args.LastIncludedIndex < rf.commitIndex {
		reply.Term = rf.currentTerm
		return
	}

	flag := false
	index := 0
	for i := 0; i < len(rf.log); i++ {
		// 快照索引的位置存在日志，只需要截断后面的日志
		if rf.getVirtualLogIndex(i) == args.LastIncludedIndex && rf.log[i].Term == args.LastIncludedTerm {
			flag = true
			index = i
			break
		}
	}
	msg := &ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	if flag {
		rf.log = rf.log[index:]
	} else {
		rf.log = make([]Entry, 0)
		rf.log = append(rf.log, Entry{Term: rf.lastIncludedTerm, Command: args.Done})
	}

	rf.snapShot = args.Data
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm

	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	if rf.lastApplied < args.LastIncludedIndex {
		rf.lastApplied = args.LastIncludedIndex
	}

	reply.Term = rf.currentTerm
	rf.applych <- *msg
	rf.persist()
}

func (rf *Raft) Snapshot(index int) {
	// Your code here (2D).
	//想要创建快照的位置超过当前已提交日志索引或者小于已经创建快照的位置
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.commitIndex < index || index <= rf.lastIncludedIndex {
		return
	}

	rf.lastIncludedTerm = rf.log[rf.getRealLogIndex(index)].Term

	rf.log = rf.log[rf.getRealLogIndex(index):]
	rf.lastIncludedIndex = index
	if rf.lastApplied < index {
		rf.lastApplied = index
	}
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.log)

	logBytes := w.Bytes()
	// 创建快照信息
	snapshotInfo := SnapshotInfo{
		LastIndex:   uint64(rf.lastIncludedIndex),
		LastTerm:    uint64(rf.lastIncludedTerm),
		LastApplied: uint64(rf.lastApplied),
		VotedFor:    rf.votedFor,
		CurrentTerm: uint64(rf.currentTerm),
		Log:         logBytes,
	}

	// 序列化整个快照信息
	snapshotBuffer := new(bytes.Buffer)
	snapshotEncoder := gob.NewEncoder(snapshotBuffer)
	if err := snapshotEncoder.Encode(snapshotInfo); err != nil {
		return
	}
	snapMsg := SnapshotMsg{
		SnapData: snapshotBuffer.Bytes(),
	}

	// 发送序列化后的快照数据到 snapshotCh
	rf.snapshotCh <- snapMsg
	rf.persist()
}

func (rf *Raft) sendSnapShot(serverTo string) {
	reply := &InstallSnapshotReply{}

	rf.mu.Lock()
	// DPrintf("server %v handleInstallSnapshot 获取锁mu", rf.me)

	if rf.role != Leader {
		// 自己已经不是Lader了, 返回
		rf.mu.Unlock()
		return
	}

	args := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.snapShot,
	}

	rf.mu.Unlock()
	// DPrintf("server %v handleInstallSnapshot 释放锁mu", rf.me)

	// 发送RPC时不要持有锁
	// 这是由于如果发送请求持有锁，节点 B 收到请求并尝试回复，但是它也需要获取锁来处理并发送这个回复。
	ok := rf.sendInstallSnapshot(serverTo, args, reply)
	if !ok {
		// RPC发送失败, 下次再触发即可
		return
	}

	rf.mu.Lock()
	// DPrintf("server %v handleInstallSnapshot 获取锁mu", rf.me)
	defer func() {
		// DPrintf("server %v handleInstallSnapshot 释放锁mu", rf.me)
		rf.mu.Unlock()
	}()

	if rf.role != Leader || rf.currentTerm != args.Term {
		// 已经不是Leader或者是过期的Leader
		return
	}

	if reply.Term > rf.currentTerm {
		// 自己是旧Leader
		rf.currentTerm = reply.Term
		rf.role = Follower
		rf.votedFor = "Denied"
		rf.resetVoteTimer()
		rf.persist()
		return
	}

	if rf.matchIndex[serverTo] < args.LastIncludedIndex {
		rf.matchIndex[serverTo] = args.LastIncludedIndex
	}
	rf.nextIndex[serverTo] = rf.matchIndex[serverTo] + 1
}
