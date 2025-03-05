package server

import (
	"encoding/gob"
	"go-raft/net"
	"go-raft/raft"
	snapshoter "go-raft/snapshot"
	"go-raft/storage"
	"go-raft/storage/bucket"
	"go-raft/types"
	"go-raft/wal"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const (
	HandleOpTimeOut = time.Millisecond * 2000 // 超时为2s
)

// Store a request's seq and result
type result struct {
	LastCommanIndex int
	Err             types.Err
	Value           string
	CurrentTerm     int
}

type KVServer struct {
	mu         sync.Mutex
	me         string               //node's identity
	rf         *raft.Raft           //raft
	applyCh    chan raft.ApplyMsg   //channel to apply raft node
	dead       int32                // set by Kill()
	resultCh   map[int]*chan result //channel to receive from raft
	historyMap map[int64]*result    // 保证特殊情况请求幂等性以及提前返回

	WalMsg chan raft.WalMsg

	maxraftstate int             // snapshot if log grows this big
	backend      storage.Backend //backend is using for storage
}

// RPC handler
func (kv *KVServer) Get(args *types.GetArgs, reply *types.GetReply) {
	opArgs := &types.Op{OpType: types.OPGet, CommanIdx: args.CommandIdx, Key: args.Key, ClientId: args.ClientId}
	res := kv.HandlerOp(opArgs)
	reply.Err = res.Err
	reply.Value = res.Value
}

func (kv *KVServer) Put(args *types.PutArgs, reply *types.PutReply) {
	opArgs := &types.Op{OpType: types.OPPut, CommanIdx: args.CommandIdx, Key: args.Key, Val: args.Value, ClientId: args.ClientId}

	res := kv.HandlerOp(opArgs)
	reply.Err = res.Err
}

func (kv *KVServer) NodesChange(args *types.NodesManageArgs, reply *types.NodesManageReply) {
	opArgs := &types.Op{CommanIdx: args.CommandIdx, ClientId: args.ClientId, Names: args.Names,
		Addrs: args.Addrs, OpType: types.OPNodesChange}
	res := kv.HandlerOp(opArgs)
	reply.Err = res.Err
}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func StartKVServer(servers map[string]*net.ClientEnd, me string, maxraftstate int, bd storage.Backend) *KVServer {
	gob.Register(types.Op{})

	kv := new(KVServer)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.WalMsg = make(chan raft.WalMsg, 64)
	kv.me = me
	kv.maxraftstate = maxraftstate
	_, err := wal.InitWal(kv.WalMsg)
	if err != nil {
		log.Fatal("init wal failed", err)
	}
	raftState, err := snapshoter.SnapManager.LoadSnapshot()
	if err != nil {
		log.Fatal("load snap failed", err)
	}

	kv.rf = raft.Make(servers, me, kv.applyCh, kv.WalMsg, raftState)
	kv.backend = bd
	btx := kv.backend.BatchTx()
	btx.Lock()
	btx.UnsafeCreateBucket(bucket.Key)
	btx.Unlock()

	kv.historyMap = make(map[int64]*result)
	kv.resultCh = make(map[int]*chan result)

	go kv.ApplyHandler()
	return kv
}

// process commit raft log
func (kv *KVServer) HandlerOp(opArgs *types.Op) (res result) {

	// 在调用raft层的Start之前就从historyMap中判断是否有历史记录可以直接返回
	kv.mu.Lock()
	if hisMap, exist := kv.historyMap[opArgs.ClientId+int64(opArgs.CommanIdx)]; exist && hisMap.LastCommanIndex == opArgs.CommanIdx {
		kv.mu.Unlock()
		return *hisMap
	}
	kv.mu.Unlock()
	startIndex, startTerm, isLeader := kv.rf.Start(*opArgs)
	if !isLeader {
		return result{
			Err:   types.ErrNotLeader,
			Value: "",
		}
	}

	kv.mu.Lock()
	newCh := make(chan result)
	if opArgs.OpType == types.OPNodesChange {
		kv.resultCh[startIndex+1] = &newCh
	} else {
		kv.resultCh[startIndex] = &newCh
	}
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.resultCh, startIndex)
		close(newCh)
		kv.mu.Unlock()
	}()

	select {
	case <-time.After(HandleOpTimeOut):
		res.Err = types.ErrHandleOpTimeOut
		return
	case msg, success := <-newCh:
		if success && msg.CurrentTerm == startTerm {
			res = msg
			return
		} else if !success {
			res.Err = types.ErrChanClose
			return
		} else {
			res.Err = types.ErrLeaderOutDated
			res.Value = ""
			return
		}
	}
}

// to the boltDB
func (kv *KVServer) Execute(op *types.Op) (res result) {
	// 调用该函数需要持有锁
	res.LastCommanIndex = op.CommanIdx
	switch op.OpType {
	case types.OPGet:
		ctx := kv.backend.ConcurrentReadTx()
		ctx.RLock()
		_, val := ctx.UnsafeRange(bucket.Key, []byte(op.Key), nil, 1)
		ctx.RUnlock()
		if val != nil {
			res.Value = string(val[0])
		} else {
			res.Err = types.ErrKeyNotExist
			res.Value = ""
		}
		return

	case types.OPPut:
		btx := kv.backend.BatchTx()
		btx.Lock()
		btx.UnsafePut(bucket.Key, []byte(op.Key), []byte(op.Val))
		btx.Unlock()
		return

	}
	return
}

func (kv *KVServer) ApplyHandler() {

	for !kv.killed() {
		entryLog, ok := <-kv.applyCh
		if !ok {
			kv.Kill()
			return
		}

		if entryLog.CommandValid {
			if op, success := entryLog.Command.(*types.Op); !success {
				newop := entryLog.Command.(types.Op)
				var res result
				needApply := false

				kv.mu.Lock()
				if histroy, exist := kv.historyMap[newop.ClientId+int64(newop.CommanIdx)]; exist {
					if histroy.LastCommanIndex == newop.CommanIdx {
						res = *histroy
					} else if histroy.LastCommanIndex < newop.CommanIdx {
						needApply = true
					}
				} else {
					needApply = true
				}
				kv.mu.Unlock()

				if needApply {
					kv.mu.Lock()
					res = kv.Execute(&newop)
					res.CurrentTerm = entryLog.SnapshotTerm
					kv.historyMap[newop.ClientId+int64(newop.CommanIdx)] = &res
					kv.mu.Unlock()
				}

				kv.mu.Lock()
				resultch, exist := kv.resultCh[entryLog.CommandIndex]
				kv.mu.Unlock()
				if exist {
					res.CurrentTerm = entryLog.SnapshotTerm
					*resultch <- res
				}

				if kv.maxraftstate != -1 && int(wal.WalFile.GetGlobalLogSeq()) > kv.maxraftstate {
					kv.rf.Snapshot(entryLog.CommandIndex)
				}
			} else {
				var res result
				needApply := false

				kv.mu.Lock()
				if histroy, exist := kv.historyMap[op.ClientId+int64(op.CommanIdx)]; exist {
					if histroy.LastCommanIndex == op.CommanIdx {
						res = *histroy
					} else if histroy.LastCommanIndex < op.CommanIdx {
						needApply = true
					}
				} else {
					needApply = true
				}
				kv.mu.Unlock()

				if needApply {
					kv.mu.Lock()
					res = kv.Execute(op)
					res.CurrentTerm = entryLog.SnapshotTerm
					kv.historyMap[op.ClientId+int64(op.CommanIdx)] = &res
					kv.mu.Unlock()
				}

				kv.mu.Lock()
				resultch, exist := kv.resultCh[entryLog.CommandIndex]
				kv.mu.Unlock()
				if exist {
					res.CurrentTerm = entryLog.SnapshotTerm
					*resultch <- res
				}

				if kv.maxraftstate != -1 && int(wal.WalFile.GetGlobalLogSeq()) > kv.maxraftstate {
					kv.rf.Snapshot(entryLog.CommandIndex)
				}
			}
		} else if entryLog.SnapshotValid {
			err := snapshoter.SnapManager.ChangeSnapshot(entryLog.Snapshot)
			if err != nil {
				//TODO
				log.Fatal("change snapshot failed", err)
			}
		}
	}
}
