package types

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
