package wal

import "errors"

var (
	ErrCRCMismatch = errors.New("crc mismatch")
	ErrCorrupt     = errors.New("wal: corrupt log entry")
)
