package storage_test

import (
	"crypto/rand"
	"github.com/stretchr/testify/assert"
	"go-raft/storage"
	"go-raft/storage/bucket"
	"testing"
	"time"
)

func BenchmarkBackendPut(b *testing.B) {
	backend, _ := storage.NewTmpBackend(b, 100*time.Millisecond, 10000)
	defer storage.Close(b, backend)

	// prepare keys
	keys := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = make([]byte, 64)
		_, err := rand.Read(keys[i])
		assert.NoError(b, err)
	}
	value := make([]byte, 128)
	_, err := rand.Read(value)
	assert.NoError(b, err)

	batchTx := backend.BatchTx()

	batchTx.Lock()
	batchTx.UnsafeCreateBucket(bucket.Test)
	batchTx.Unlock()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batchTx.Lock()
		batchTx.UnsafePut(bucket.Test, keys[i], value)
		batchTx.Unlock()
	}
}
