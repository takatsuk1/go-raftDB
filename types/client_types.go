package types

// Put or Append
type PutArgs struct {
	Key        string
	Value      string
	ClientId   int64
	CommandIdx int
}

type PutReply struct {
	Err Err
}

type GetArgs struct {
	Key        string
	ClientId   int64
	CommandIdx int
}

type GetReply struct {
	Err   Err
	Value string
}

type NodesManageArgs struct {
	OldNames   []string
	OldAddrs   map[string]string
	Names      []string
	Addrs      map[string]string
	ClientId   int64
	CommandIdx int
}

type NodesManageReply struct {
	Err Err
}
