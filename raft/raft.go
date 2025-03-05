package raft

import (
	"bytes"
	"encoding/gob"
	"go-raft/net"
	"go-raft/types"

	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	//	"6.5840/labgob"
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      *SnapshotInfo
	SnapshotTerm  int
	SnapshotIndex int
}

type WalMsg struct {
	Data []byte
}

type SnapshotMsg struct {
	SnapData []byte
}

type Entry struct {
	Command interface{}
	Term    int
}

const (
	Follower = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu    sync.Mutex                // Lock to protect shared access to this peer's state
	peers map[string]*net.ClientEnd // RPC end points of all peers
	me    string                    // this peer's index into peers[]
	dead  int32                     // set by Kill()

	currentTerm int
	votedFor    string
	voteCount   int
	log         []Entry
	nextIndex   map[string]int // 对于每一个follwer，预计要追加的下一条日志条目所在的索引
	matchIndex  map[string]int // 对于每一个follwer，已经复制给该follwer的最后日志条目下标

	commitIndex int
	lastApplied int
	applych     chan ApplyMsg
	applyCond   *sync.Cond

	stateCh    chan WalMsg
	snapshotCh chan SnapshotMsg
	snapShot   *SnapshotInfo

	lastIncludedIndex int
	lastIncludedTerm  int

	voteTimer  *time.Timer
	heartTimer *time.Timer
	role       int
}

func (rf *Raft) getRealLogIndex(idx int) int {
	return idx - rf.lastIncludedIndex
}

func (rf *Raft) getVirtualLogIndex(idx int) int {
	return idx + rf.lastIncludedIndex
}

func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

func (rf *Raft) persist() {

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)

	e.Encode(rf.votedFor)
	e.Encode(rf.currentTerm)
	e.Encode(rf.log)

	//要考虑encode的顺序
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	e.Encode(rf.lastApplied)

	raftState := w.Bytes()

	newWalMsg := WalMsg{
		Data: raftState,
	}

	rf.stateCh <- newWalMsg
}

// Warn！！！！ 存储persist的时候和readpersist的时候的状态信息的顺序需要保持一致
// restore previously persisted state.

func (rf *Raft) readRaftState(data [][]byte) {
	if data == nil || len(data) < 1 {
		return
	}

	// 按顺序读取每个 WAL 数据
	for _, tmpData := range data {
		r := bytes.NewBuffer(tmpData)
		d := gob.NewDecoder(r)

		var votedFor string
		var currentTerm int
		var log []Entry
		var lastIncludedIndex int
		var lastIncludedTerm int
		var lastApplied int

		if d.Decode(&votedFor) != nil ||
			d.Decode(&currentTerm) != nil ||
			d.Decode(&log) != nil ||
			d.Decode(&lastIncludedIndex) != nil ||
			d.Decode(&lastIncludedTerm) != nil ||
			d.Decode(&lastApplied) != nil {
			continue // 如果解码失败，继续处理下一条记录
		}

		// 更新状态，后面的记录会覆盖前面的记录
		rf.votedFor = votedFor
		rf.currentTerm = currentTerm
		rf.log = log
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm

		rf.commitIndex = lastIncludedIndex
		rf.lastApplied = lastApplied
	}
}
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	if rf.voteTimer != nil {
		rf.voteTimer.Stop()
	}
	if rf.heartTimer != nil {
		rf.heartTimer.Stop()
	}
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) resetVoteTimer() {
	// 随机生成一个选举超时时间，范围在 300 到 500 毫秒
	electionTimeout := 300 + rand.Intn(200) // 随机生成 300~500 毫秒的值
	rf.voteTimer.Reset(time.Duration(electionTimeout) * time.Millisecond)
}

// 重设心跳时间
func (rf *Raft) resetHeartTimer(heartBeatInterval int) {
	// 设置固定的心跳间隔时间，例如 50 毫秒
	rf.heartTimer.Reset(time.Duration(heartBeatInterval) * time.Millisecond)
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// 如果不是leader返回false
	rf.mu.Lock()
	// 输出当前线程获取锁的调试信息
	DPrintf("server %v Start 获取锁mu", rf.me)
	defer func() {
		rf.resetHeartTimer(10)
		rf.mu.Unlock()
		// 输出当前线程释放锁的调试信息
		DPrintf("server %v Start 释放锁mu", rf.me)
	}()
	if rf.role != Leader {
		return -1, -1, false
	}
	if op, ok := command.(types.Op); ok {
		if op.OpType == "NodesChange" {
			rf.ChangeToMidPeers(op.Names, op.Addrs)
			newOp := &types.Op{
				OpType:    types.OPNodesToMidStateChange,
				Names:     op.Names,
				Addrs:     op.Addrs,
				CommanIdx: op.CommanIdx,
				ClientId:  op.ClientId,
			}
			var newOpInterface interface{} = newOp
			command = newOpInterface
		}
	}
	newEntry := &Entry{Term: rf.currentTerm, Command: command}
	rf.log = append(rf.log, *newEntry)
	rf.persist()

	return rf.getVirtualLogIndex(len(rf.log) - 1), rf.currentTerm, true
}

func (rf *Raft) isNodesChange(logs []Entry, isFollower bool) (bool, *types.Op) {
	for _, entry := range logs {
		if op, ok := entry.Command.(types.Op); ok {
			if op.OpType == types.OPNodesToMidStateChange {
				if isFollower {
					rf.ChangeToMidPeers(op.Names, op.Addrs)
				}
				return true, &op
			} else if op.OpType == types.OPNodesToFinalState {
				if _, exists := op.Addrs[rf.me]; exists {
					rf.ChangeToFinalPeers(op.Names, op.Addrs)
				} else {
					rf.Kill()
				}

				return true, &op
			}
		} else {
			if tmpop, success := entry.Command.(*types.Op); success {
				// 成功断言为 Op 结构体，可以访问 op 的字段
				if tmpop.OpType == types.OPNodesToMidStateChange {
					if isFollower {
						rf.ChangeToMidPeers(tmpop.Names, tmpop.Addrs)
					}
					return true, tmpop
				} else if tmpop.OpType == types.OPNodesToFinalState {
					if _, exists := tmpop.Addrs[rf.me]; exists {
						rf.ChangeToFinalPeers(tmpop.Names, tmpop.Addrs)
					} else {
						rf.Kill()
					}
					return true, tmpop
				}
			}
		}
	}
	return false, nil
}

func (rf *Raft) sendNewNodesGroupLog(op *types.Op) bool {
	newOp := &types.Op{
		OpType:    types.OPNodesToFinalState,
		Names:     op.Names,
		Addrs:     op.Addrs,
		CommanIdx: op.CommanIdx + 1,
		ClientId:  op.ClientId,
	}
	if rf.role != Leader {
		return false
	}
	command := interface{}(newOp)
	newEntry := &Entry{Term: rf.currentTerm, Command: command}
	rf.log = append(rf.log, *newEntry)
	rf.persist()
	return true
}
func (rf *Raft) CommitChecker() {
	for !rf.killed() {
		rf.mu.Lock()
		// 输出当前线程获取锁的调试信息
		DPrintf("server %v CommitChecker 获取锁mu，222", rf.me)
		for rf.commitIndex <= rf.lastApplied {
			rf.applyCond.Wait()
		}
		rf.mu.Unlock()
		DPrintf("server %v CommitChecker 释放锁mu,228", rf.me)

		rf.mu.Lock()
		msgBuf := make([]*ApplyMsg, 0, rf.commitIndex-rf.lastApplied)
		applyIndex := rf.lastApplied
		// 存在提交了还未应用的日志
		for rf.commitIndex > applyIndex {
			applyIndex++
			//最后应用的日志索引小于等于上次快照的最后索引
			//在 Raft 协议中，Follower 节点在跟随 Leader 时，如果发现自己落后过多或者 Leader 认为 Follower 无法通过正常的日志复制赶上，
			//Leader 会向 Follower 发送一个快照来更新其状态。Follower 在应用这个快照后，会更新 lastIncludedIndex。
			//情况：如果 Follower 在接收并应用了快照后，继续处理本地旧的日志条目，就可能遇到 tmpApplied <= rf.lastIncludedIndex 的情况。
			if applyIndex < rf.lastIncludedIndex {
				continue
			}
			msg := &ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.getRealLogIndex(applyIndex)].Command,
				CommandIndex: applyIndex,
				SnapshotTerm: rf.log[rf.getRealLogIndex(applyIndex)].Term,
			}
			msgBuf = append(msgBuf, msg)
		}
		rf.mu.Unlock()
		for _, msg := range msgBuf {
			rf.applych <- *msg
			rf.mu.Lock()
			rf.lastApplied = msg.CommandIndex
			rf.mu.Unlock()
		}
		// 输出当前线程释放锁的调试信息
		//DPrintf("server %v CommitChecker 释放锁mu,253", rf.me)
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// 这里通过 <-rf.voteTimer.C 实现了一个阻塞，直到选举计时器（voteTimer）超时。
		// 计时器的时间是随机的，通常设置为 50 到 350 毫秒之间的一个随机值。
		<-rf.voteTimer.C
		rf.mu.Lock()
		// 输出当前线程获取锁的调试信息
		DPrintf("server %v ticker 获取锁mu", rf.me)
		if len(rf.peers) == 0 {
			DPrintf("只有一个节点，直接成为leader", rf.me)
			rf.role = Leader
			go rf.sendHeartBeats()
		}
		if rf.role != Leader {
			// 超时
			go rf.Elect()
		}
		rf.resetVoteTimer()
		rf.mu.Unlock()
		// 输出当前线程释放锁的调试信息
		DPrintf("server %v ticker 释放锁mu", rf.me)
	}
}

func (rf *Raft) ChangeToMidPeers(names []string, addrs map[string]string) {
	ends, newNames := net.ChangeToMidEnds(names, addrs, rf.peers)
	if ends != nil {
		rf.peers = ends
		for _, name := range newNames {
			rf.nextIndex[name] = rf.getVirtualLogIndex(len(rf.log))
			rf.matchIndex[name] = rf.lastIncludedIndex
		}
	}
}

func (rf *Raft) ChangeToFinalPeers(names []string, addrs map[string]string) {
	newPeers := net.ChangeToFinalEnds(names, rf.peers)

	nodesToRemove := make(map[string]bool)
	for nodeID := range rf.peers {
		if _, exists := newPeers[nodeID]; !exists {
			nodesToRemove[nodeID] = true
		}
	}
	for nodeID := range nodesToRemove {
		delete(rf.nextIndex, nodeID)
		delete(rf.matchIndex, nodeID)
		DPrintf("Raft[%d] - Removed node %s from nextIndex and matchIndex\n", rf.me, nodeID)
	}
	rf.peers = newPeers
	DPrintf("Raft[%d] - Updated peers to new configuration: %v\n", rf.me, rf.peers)
}

func Make(peers map[string]*net.ClientEnd, me string,
	applyCh chan ApplyMsg, stateCh chan WalMsg, raftState [][]byte) *Raft {
	rf := &Raft{}
	rf.peers = peers

	rf.me = me

	rf.log = make([]Entry, 0)
	rf.log = append(rf.log, Entry{Term: 0})
	rf.nextIndex = make(map[string]int, len(peers))
	rf.matchIndex = make(map[string]int, len(peers))

	rf.applych = applyCh
	rf.stateCh = stateCh

	rf.role = Follower
	rf.voteTimer = time.NewTimer(0)
	rf.heartTimer = time.NewTimer(0)
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.resetVoteTimer()

	if len(raftState) != 0 {
		rf.readRaftState(raftState)
	}

	go rf.ticker()
	go rf.CommitChecker()

	return rf
}
