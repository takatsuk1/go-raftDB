package types

import "errors"

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
	ErrCRCMismatch    = errors.New("crc mismatch")
	ErrCorrupt        = errors.New("wal: corrupt log entry")
)

type Err string
