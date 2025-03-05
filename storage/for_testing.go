package storage

import (
	"github.com/stretchr/testify/assert"
	bolt "go.etcd.io/bbolt"
	"io/ioutil"
	"path/filepath"
	"testing"
	"time"
)

func NewTmpBackendFromCfg(t testing.TB, bcfg BackendConfig) (Backend, string) {
	dir, err := ioutil.TempDir(t.TempDir(), "etcd_backend_test")
	if err != nil {
		panic(err)
	}
	tmpPath := filepath.Join(dir, "database")

	return New(bcfg), tmpPath
}

func DbFromBackendForTest(b Backend) *bolt.DB {
	return b.(*backend).db
}

// NewTmpBackend creates a backend implementation for testing.
func NewTmpBackend(t testing.TB, batchInterval time.Duration, batchLimit int) (Backend, string) {
	bcfg := DefaultBackendConfig()
	bcfg.BatchInterval, bcfg.BatchLimit = batchInterval, batchLimit
	return NewTmpBackendFromCfg(t, bcfg)
}

func NewDefaultTmpBackend(t testing.TB) (Backend, string) {
	return NewTmpBackendFromCfg(t, DefaultBackendConfig())
}

func Close(t testing.TB, b Backend) {
	assert.NoError(t, b.Close())
}
