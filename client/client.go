package client

import (
	"crypto/rand"
	"go-raft/net"
	"go-raft/types"
	"log"
	"time"
)
import "math/big"

type Operate interface {
	Get(string) string
	Put(string, string)
	NodesManage([]string, map[string]string, []string, map[string]string)
}

type Clerk struct {
	servers    map[string]*net.ClientEnd
	clientId   int64
	leaderName []string
	commandIdx int
	uuid       string
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func randLeader(min, max int64) int64 {

	randomNumber, err := rand.Int(rand.Reader, big.NewInt(max-min+1))
	if err != nil {
		log.Fatal(err)
	}
	result := randomNumber.Int64() + min

	return result
}

func MakeClerk(serversEnds map[string]*net.ClientEnd, serverNames []string) Operate {
	ck := new(Clerk)
	ck.servers = serversEnds
	ck.leaderName = serverNames
	ck.clientId = nrand()
	ck.commandIdx = 0
	return ck
}

// Client Operation
func (ck *Clerk) Get(key string) string {

	args := &types.GetArgs{
		Key:        key,
		ClientId:   ck.clientId,
		CommandIdx: ck.getCommanIdx(),
	}

	result := randLeader(0, int64(len(ck.leaderName)-1))
	index := int(result)
	for {
		reply := &types.GetReply{}
		ok := ck.servers[ck.leaderName[index]].Call("KVServer.Get", args, reply)
		if !ok || reply.Err == types.ErrNotLeader || reply.Err == types.ErrLeaderOutDated {
			index++
			index %= len(ck.servers)
			continue
		}
		switch reply.Err {
		case types.ErrChanClose:
			time.Sleep(1 * time.Second)
			continue
		case types.ErrHandleOpTimeOut:
			time.Sleep(1 * time.Second)
			continue
		}
		return reply.Value
	}
}

func (ck *Clerk) Put(key string, value string) {
	args := &types.PutArgs{
		Key:        key,
		Value:      value,
		ClientId:   ck.clientId,
		CommandIdx: ck.getCommanIdx(),
	}

	result := randLeader(0, int64(len(ck.leaderName)-1))
	index := int(result)
	for {
		reply := &types.PutReply{}
		ok := ck.servers[ck.leaderName[index]].Call("KVServer.Put", args, reply)
		if !ok || reply.Err == types.ErrNotLeader || reply.Err == types.ErrLeaderOutDated {
			index++
			index %= len(ck.servers)
			continue
		}
		switch reply.Err {
		case types.ErrChanClose:
			time.Sleep(1 * time.Second)
			continue
		case types.ErrHandleOpTimeOut:
			time.Sleep(1 * time.Second)
			continue
		}
		return
	}
}

func (ck *Clerk) NodesManage(oldnames []string, oldaddrs map[string]string, names []string, addrs map[string]string) {
	args := &types.NodesManageArgs{
		OldNames:   oldnames,
		OldAddrs:   oldaddrs,
		Names:      names,
		Addrs:      addrs,
		ClientId:   ck.clientId,
		CommandIdx: ck.getCommanIdx(),
	}
	// 设置随机种子
	result := randLeader(0, int64(len(ck.leaderName)-1))
	index := int(result)
	for {
		reply := &types.NodesManageReply{}
		ok := ck.servers[ck.leaderName[index]].Call("KVServer.NodesChange", args, reply)
		if !ok || reply.Err == types.ErrNotLeader || reply.Err == types.ErrLeaderOutDated {
			index++
			index %= len(ck.servers)
			continue
		}
		switch reply.Err {
		case types.ErrChanClose:
			time.Sleep(1 * time.Second)
			continue
		case types.ErrHandleOpTimeOut:
			time.Sleep(1 * time.Second)
			continue
		}
		return
	}
}

func (ck *Clerk) getCommanIdx() int {
	ck.commandIdx++
	return ck.commandIdx
}
