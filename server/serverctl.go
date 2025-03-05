package server

import (
	"go-raft/net"
	snapshoter "go-raft/snapshot"
	"go-raft/storage"
	"go-raft/utils"
	"sync"
)

type ServerConfig struct {
	mu           sync.Mutex
	Net          *net.Network
	kvserver     *KVServer
	maxraftstate int
}

var SvConfig *ServerConfig

//func MakeServersDynamic(servername string, serverHost string, manageHost string, maxraftstate int, nt *net.Network) bool {
//	success := manageNodes.RegisterNode(servername, serverHost, manageHost)
//	if !success {
//		return false
//	}
//
//}

func Make_servers(names []string, maxraftstate int, nt *net.Network, addresses map[string]string, index int) {
	SvConfig = &ServerConfig{
		Net:          nt,
		kvserver:     nil,
		maxraftstate: maxraftstate,
	}
	SvConfig.mu.Lock()

	n := len(names)

	ends := map[string]*net.ClientEnd{}
	for j := 0; j < n; j++ {
		if index != j {
			endnames := utils.Randstring(20)
			ends[names[j]] = net.MakeEnd(endnames, addresses[names[j]], false)
		}
	}

	SvConfig.mu.Unlock()

	bd := storage.NewDefaultBackend()
	_ = snapshoter.NewSnapshotManager()
	SvConfig.kvserver = StartKVServer(ends, names[index], SvConfig.maxraftstate, bd)
	kvsvc := net.MakeService(SvConfig.kvserver)
	rfsvc := net.MakeService(SvConfig.kvserver.rf)
	srv := net.MakeServer()
	srv.AddService(kvsvc)
	srv.AddService(rfsvc)
	// 注册服务器到网络层
	SvConfig.Net.AddServer(names[index], srv)
}

func Change_servers(oldnames []string, oldaddrs map[string]string, names []string, maxraftstate int, nt *net.Network, addresses map[string]string, index int) {
	newNames, newindex := getNewGroup(oldnames, oldaddrs, names, addresses, index)

	SvConfig = &ServerConfig{
		Net:          nt,
		kvserver:     nil,
		maxraftstate: maxraftstate,
	}
	SvConfig.mu.Lock()

	n := len(newNames)

	ends := map[string]*net.ClientEnd{}
	for j := 0; j < n; j++ {
		if newindex != j {
			endnames := utils.Randstring(20)
			ends[newNames[j]] = net.MakeEnd(endnames, addresses[newNames[j]], false)
		}
	}

	SvConfig.mu.Unlock()

	bd := storage.NewDefaultBackend()
	_ = snapshoter.NewSnapshotManager()
	SvConfig.kvserver = StartKVServer(ends, names[index], SvConfig.maxraftstate, bd)
	kvsvc := net.MakeService(SvConfig.kvserver)
	rfsvc := net.MakeService(SvConfig.kvserver.rf)
	srv := net.MakeServer()
	srv.AddService(kvsvc)
	srv.AddService(rfsvc)
	// 注册服务器到网络层
	SvConfig.Net.AddServer(newNames[newindex], srv)
}

func getNewGroup(oldNames []string, oldAddrs map[string]string, newNames []string, newAddrs map[string]string, index int) ([]string, int) {
	oldNamesMap := make(map[string]struct{})
	for _, name := range oldNames {
		oldNamesMap[name] = struct{}{}
	}

	resultNames := []string{}
	resultAddrs := make(map[string]string)

	targetName := newNames[index]
	targetPosition := -1

	for _, name := range newNames {
		if _, exists := oldNamesMap[name]; !exists {
			resultNames = append(resultNames, name)
			resultAddrs[name] = newAddrs[name]

		}
	}

	resultNames = append(resultNames, oldNames...)
	for i := 0; i < len(resultNames); i++ {
		if resultNames[i] == targetName {
			targetPosition = i
		}
	}

	return resultNames, targetPosition
}
