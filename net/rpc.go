package net

import (
	"bytes"
	"context"
	"encoding/gob"
	"go-raft/pool"
	"log"
)

type reqMsg struct {
	Endname string // Client
	SvcMeth string // e.g. "Raft.AppendEntries"
	Args    []byte
}

type replyMsg struct {
	Ok    bool
	Err   string
	Reply []byte
}

type ClientEnd struct {
	Endname string     // 该端点的名称
	Pool    *pool.Pool // 与网络层的TCP连接
}

func (e *ClientEnd) Call(svcMeth string, args interface{}, reply interface{}) bool {

	req := reqMsg{}
	req.Endname = e.Endname
	req.SvcMeth = svcMeth

	// 序列化参数
	qb := new(bytes.Buffer)
	qe := gob.NewEncoder(qb)
	if err := qe.Encode(args); err != nil {
		log.Fatalf("ClientEnd.Call(): 序列化参数失败: %v\n", err)
	}
	req.Args = qb.Bytes()

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(req); err != nil {
		log.Fatalf("ClientEnd.Call(): 序列化请求失败: %v\n", err)
	}

	//连接池取出连接
	conn, err := e.Pool.Get(context.Background())
	if err != nil {
		log.Fatalf("Failed to get connection: %v", err)
	}
	defer e.Pool.Put(conn)

	encoder := gob.NewEncoder(conn)
	if err := encoder.Encode(req); err != nil {
		log.Fatalf("ClientEnd.Call(): 发送请求失败: %v\n", err)
	}

	dec := gob.NewDecoder(conn)
	var rep replyMsg
	if err := dec.Decode(&rep); err != nil {
		log.Fatalf("ClientEnd.Call(): 接收回复失败: %v\n", err)
	}
	//回复失败
	if !rep.Ok {
		return false
	}

	if rep.Ok {
		rb := bytes.NewBuffer(rep.Reply)
		rd := gob.NewDecoder(rb)
		if err := rd.Decode(reply); err != nil {
			log.Fatalf("ClientEnd.Call(): 解码回复失败: %v\n", err)
		}
		return true
	} else {
		return false
	}
}
