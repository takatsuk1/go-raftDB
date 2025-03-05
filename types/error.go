package types

const (
	ErrNotLeader       = "NotLeader"
	ErrKeyNotExist     = "KeyNotExist"
	ErrHandleOpTimeOut = "HandleOpTimeOut"
	ErrChanClose       = "ChanClose"
	ErrLeaderOutDated  = "LeaderOutDated"
	ErrRPCFailed       = "RPCFailed"

	ErrDecodeRequest  = "DecodeRequestFailed"
	ErrServerNotExist = "ServerNotExist"
	ErrUnkonwnService = "UnkonwnService"
	ErrUnkonwnMethod  = "UnkonwnMethod"
)

type Err string
