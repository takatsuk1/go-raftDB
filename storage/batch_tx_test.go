package storage_test

import (
	"go-raft/storage"
	"go-raft/storage/bucket"
	bolt "go.etcd.io/bbolt"
	"reflect"
	"testing"
	"time"
)

func TestBatchTxPut(t *testing.T) {
	b, _ := storage.NewTmpBackend(t, time.Hour, 10000)
	defer storage.Close(t, b)

	tx := b.BatchTx()

	tx.Lock()

	// create bucket
	tx.UnsafeCreateBucket(bucket.Test)

	// put
	v := []byte("bar")
	tx.UnsafePut(bucket.Test, []byte("foo"), v)

	tx.Unlock()

	// check put result before and after tx is committed
	for k := 0; k < 2; k++ {
		tx.Lock()
		_, gv := tx.UnsafeRange(bucket.Test, []byte("foo"), nil, 0)
		tx.Unlock()
		if !reflect.DeepEqual(gv[0], v) {
			t.Errorf("v = %s, want %s", string(gv[0]), string(v))
		}
		tx.Commit()
	}
}

func TestBatchTxRange(t *testing.T) {
	b, _ := storage.NewTmpBackend(t, time.Hour, 10000)
	defer storage.Close(t, b)

	tx := b.BatchTx()
	tx.Lock()
	defer tx.Unlock()

	tx.UnsafeCreateBucket(bucket.Test)
	// put keys
	allKeys := [][]byte{[]byte("foo"), []byte("foo1"), []byte("foo2")}
	allVals := [][]byte{[]byte("bar"), []byte("bar1"), []byte("bar2")}
	for i := range allKeys {
		tx.UnsafePut(bucket.Test, allKeys[i], allVals[i])
	}

	tests := []struct {
		key    []byte
		endKey []byte
		limit  int64

		wkeys [][]byte
		wvals [][]byte
	}{
		// single key
		{
			[]byte("foo"), nil, 0,
			allKeys[:1], allVals[:1],
		},
		// single key, bad
		{
			[]byte("doo"), nil, 0,
			nil, nil,
		},
		// key range
		{
			[]byte("foo"), []byte("foo1"), 0,
			allKeys[:1], allVals[:1],
		},
		// key range, get all keys
		{
			[]byte("foo"), []byte("foo3"), 0,
			allKeys, allVals,
		},
		// key range, bad
		{
			[]byte("goo"), []byte("goo3"), 0,
			nil, nil,
		},
		// key range with effective limit
		{
			[]byte("foo"), []byte("foo3"), 1,
			allKeys[:1], allVals[:1],
		},
		// key range with limit
		{
			[]byte("foo"), []byte("foo3"), 4,
			allKeys, allVals,
		},
	}
	for i, tt := range tests {
		keys, vals := tx.UnsafeRange(bucket.Test, tt.key, tt.endKey, tt.limit)
		if !reflect.DeepEqual(keys, tt.wkeys) {
			t.Errorf("#%d: keys = %+v, want %+v", i, keys, tt.wkeys)
		}
		if !reflect.DeepEqual(vals, tt.wvals) {
			t.Errorf("#%d: vals = %+v, want %+v", i, vals, tt.wvals)
		}
	}
}

func TestBatchTxDelete(t *testing.T) {
	b, _ := storage.NewTmpBackend(t, time.Hour, 10000)
	defer storage.Close(t, b)

	tx := b.BatchTx()
	tx.Lock()

	tx.UnsafeCreateBucket(bucket.Test)
	tx.UnsafePut(bucket.Test, []byte("foo"), []byte("bar"))

	tx.UnsafeDelete(bucket.Test, []byte("foo"))

	tx.Unlock()

	// check put result before and after tx is committed
	for k := 0; k < 2; k++ {
		tx.Lock()
		ks, _ := tx.UnsafeRange(bucket.Test, []byte("foo"), nil, 0)
		tx.Unlock()
		if len(ks) != 0 {
			t.Errorf("keys on foo = %v, want nil", ks)
		}
		tx.Commit()
	}
}

func TestBatchTxCommit(t *testing.T) {
	b, _ := storage.NewTmpBackend(t, time.Hour, 10000)
	defer storage.Close(t, b)

	tx := b.BatchTx()
	tx.Lock()
	tx.UnsafeCreateBucket(bucket.Test)
	tx.UnsafePut(bucket.Test, []byte("foo"), []byte("bar"))
	tx.Unlock()

	tx.Commit()

	// check whether put happens via db view
	storage.DbFromBackendForTest(b).View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucket.Test.Name())
		if bucket == nil {
			t.Errorf("bucket test does not exit")
			return nil
		}
		v := bucket.Get([]byte("foo"))
		if v == nil {
			t.Errorf("foo key failed to written in backend")
		}
		return nil
	})
}

func TestBatchTxBatchLimitCommit(t *testing.T) {
	// start backend with batch limit 1 so one write can
	// trigger a commit
	b, _ := storage.NewTmpBackend(t, time.Hour, 1)
	defer storage.Close(t, b)

	tx := b.BatchTx()
	tx.Lock()
	tx.UnsafeCreateBucket(bucket.Test)
	tx.UnsafePut(bucket.Test, []byte("foo"), []byte("bar"))
	tx.Unlock()

	// batch limit commit should have been triggered
	// check whether put happens via db view
	storage.DbFromBackendForTest(b).View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucket.Test.Name())
		if bucket == nil {
			t.Errorf("bucket test does not exit")
			return nil
		}
		v := bucket.Get([]byte("foo"))
		if v == nil {
			t.Errorf("foo key failed to written in backend")
		}
		return nil
	})
}
