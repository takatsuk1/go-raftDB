package net

import (
	"encoding/gob"
	"go-raft/pool"
	"go-raft/types"
	"go-raft/utils"
	"log"
	"net"
	"sync"
)

type Network struct {
	mu     sync.Mutex
	server *Server // endname -> server
}

func MakeNetwork() *Network {
	rn := &Network{}
	rn.server = nil

	// 节点通信
	go rn.startServerListener()
	// 客户端通信
	go rn.startClientListener()

	return rn
}

func (rn *Network) startServerListener() {
	serverPort, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("无法启动服务器TCP监听: %v", err)
	}
	defer serverPort.Close()

	for {
		conn1, err1 := serverPort.Accept()

		if err1 != nil {
			log.Printf("接受连接失败: %v, %v", err1)
			continue
		}

		if err1 == nil {
			go rn.handleServerConnection(conn1)
		}
	}
}

func (rn *Network) startClientListener() {
	clientPort, err := net.Listen("tcp", ":8081")
	if err != nil {
		log.Fatalf("无法启动TCP监听: %v", err)
	}
	defer clientPort.Close()

	for {
		conn2, err2 := clientPort.Accept()

		if err2 != nil {
			log.Printf("接受连接失败: %v, %v", err2)
			continue
		}

		if err2 == nil {
			go rn.handleClientConnection(conn2)
		}
	}
}

func (rn *Network) handleClientConnection(conn net.Conn) {
	defer conn.Close()

	for {
		decoder := gob.NewDecoder(conn)
		encoder := gob.NewEncoder(conn)

		var req reqMsg
		var reply replyMsg

		if err := decoder.Decode(&req); err != nil {
			reply.Ok = false
			reply.Err = types.ErrDecodeRequest + err.Error()
		} else if rn.server == nil {
			reply.Ok = false
			reply.Err = types.ErrServerNotExist
		} else {
			reply = rn.server.dispatch(req)
		}

		// 发送回复
		if err := encoder.Encode(reply); err != nil {
			log.Printf("Error sending reply: %v", err)
			return
		}
	}
}

func (rn *Network) handleServerConnection(conn net.Conn) {
	defer conn.Close()

	for {

		decoder := gob.NewDecoder(conn)
		encoder := gob.NewEncoder(conn)

		var req reqMsg
		var reply replyMsg

		if err := decoder.Decode(&req); err != nil {
			reply.Ok = false
			reply.Err = types.ErrDecodeRequest + err.Error()
		} else if rn.server == nil {
			reply.Ok = false
			reply.Err = types.ErrServerNotExist
		} else {
			reply = rn.server.dispatch(req)
		}

		// 发送回复
		if err := encoder.Encode(reply); err != nil {
			log.Printf("Error sending reply: %v", err)
			return
		}
	}
}

// 建立连接端点，连接池
// client -> server: 8081
// server -> server: 8080
func MakeEnd(endname string, address string, isClient bool) *ClientEnd {
	e := &ClientEnd{}
	e.Endname = endname
	port := ""
	if !isClient {
		port = "8080"
	} else {
		port = "8081"
	}

	netPoll := pool.NewPool(func() (net.Conn, error) {
		return pool.NewTCPConnection(address, port)
	})

	e.Pool = netPoll

	return e
}

// 集群变更过渡态处理
// 对于新加入集群的节点，旧节点需要对其建立通信端点
// 最后返回中间态的所有通信端点
func ChangeToMidEnds(names []string, addrs map[string]string, oldEnd map[string]*ClientEnd) (map[string]*ClientEnd, []string) {

	resultNames := []string{}
	for _, name := range names {
		if _, exists := oldEnd[name]; !exists {
			resultNames = append(resultNames, name)
		}
	}

	if len(resultNames) == 0 {
		return nil, nil
	}

	ends := make(map[string]*ClientEnd)
	for i := 0; i < len(resultNames); i++ {
		endname := utils.Randstring(20)
		ends[resultNames[i]] = MakeEnd(endname, addrs[resultNames[i]], false)
	}
	for key, value := range oldEnd {
		ends[key] = value
	}
	return ends, resultNames
}

// 集群变更最终态处理
// 只返回新集群的通信端点
func ChangeToFinalEnds(names []string, oldEnd map[string]*ClientEnd) map[string]*ClientEnd {
	result := make(map[string]*ClientEnd)

	for _, name := range names {
		if clientEnd, exists := oldEnd[name]; exists {
			result[name] = clientEnd
		}
	}

	return result
}

func (rn *Network) AddServer(servername string, rs *Server) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rn.server = rs
}
