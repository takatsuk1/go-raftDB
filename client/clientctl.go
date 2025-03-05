package client

import (
	"go-raft/net"
	"go-raft/utils"
	"sync"
)

type Client struct {
	mu          sync.Mutex
	serverNames []string
	address     map[string]string
}

func Init_ClientConfig(names []string, address map[string]string) *Client {
	cl := &Client{
		serverNames: names,
		address:     address,
	}
	return cl
}

func (cl *Client) MakeClient() Operate {
	cl.mu.Lock()
	defer cl.mu.Unlock()

	nums := len(cl.serverNames)
	// a fresh set of ClientEnds.
	ends := make(map[string]*net.ClientEnd, nums)
	//该client与所有server连接的端点名，
	endnames := make(map[string]string, nums)
	//生成连接到网络层的地址
	for i := 0; i < nums; i++ {
		endnames[cl.serverNames[i]] = utils.Randstring(20) // 确保endname唯一
		ends[cl.serverNames[i]] = net.MakeEnd(endnames[cl.serverNames[i]], cl.address[cl.serverNames[i]], true)
	}

	ck := MakeClerk(ends, cl.serverNames)
	return ck
}
