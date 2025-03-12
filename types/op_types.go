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

type OType string

const (
	OPGet                   OType = "Get"
	OPPut                   OType = "Put"
	OPAppend                OType = "Append"
	OPNodesChange           OType = "NodesChange"
	OPNodesToFinalState     OType = "NodesToFinalState"
	OPNodesToMidStateChange OType = "NodesToMidStateChange"
)

type Op struct {
	OpType    OType
	Names     []string
	Addrs     map[string]string
	Key       string
	Val       string
	CommanIdx int
	ClientId  int64
}
