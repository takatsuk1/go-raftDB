package storage

import (
	"bytes"
	"go-raft/wal"
	bolt "go.etcd.io/bbolt"
	"log"
	"math"
	"sync"
)

type BucketID int

type Bucket interface {
	ID() BucketID
	Name() []byte
	String() string
	IsSafeRangeBucket() bool
}

type BatchTx interface {
	Lock()
	Unlock()
	UnsafeRange(bucket Bucket, key, endKey []byte, limit int64) (keys [][]byte, vals [][]byte)
	UnsafeCreateBucket(bucket Bucket)
	UnsafeDeleteBucket(bucket Bucket)
	UnsafePut(bucket Bucket, key []byte, value []byte)
	UnsafeDelete(bucket Bucket, key []byte)
	// Commit commits a previous tx and begins a new writable one.
	Commit()
	// CommitAndStop commits the previous tx and does not create a new one.
	CommitAndStop()
}

type batchTx struct {
	sync.Mutex
	tx      *bolt.Tx
	backend *backend
	buf     txWriteBuffer
	// 未提交的删除操作数
	pendingDeleteOperations int

	pending int
}

func newBatchTx(backend *backend) *batchTx {
	tx := &batchTx{
		backend: backend,
		buf: txWriteBuffer{
			txBuffer: txBuffer{make(map[BucketID]*bucketBuffer)},
			//bucket2seq: make(map[BucketID]bool),
		},
	}
	tx.Commit()
	return tx
}

//func (t *batchTx) GetSnapShot(w io.Writer) error {
//	_, err := t.tx.WriteTo(w)
//	return err
//}

// Lock is supposed to be called only by the unit test.
func (t *batchTx) Lock() {
	t.lock()
}

func (t *batchTx) lock() {
	t.Mutex.Lock()
}

func (t *batchTx) Unlock() {
	if t.pending != 0 {
		t.backend.readTx.Lock()
		t.buf.writeback(&t.backend.readTx.buf)
		t.backend.readTx.Unlock()
		if t.pending >= t.backend.batchLimit || t.pendingDeleteOperations > 0 {
			// 删除操作需要立即提交
			t.CommitGetReadLock(false)
		}
	}
	t.Mutex.Unlock()

}

func (t *batchTx) UnsafeCreateBucket(bucket Bucket) {
	_, err := t.tx.CreateBucket(bucket.Name())
	if err != nil && err != bolt.ErrBucketExists {
		log.Print("failed to create a bucket")
	}
	t.pending++
}

func (t *batchTx) UnsafeDeleteBucket(bucket Bucket) {
	err := t.tx.DeleteBucket(bucket.Name())
	if err != nil && err != bolt.ErrBucketNotFound {
		log.Print("failed to delete bucket")
	}
	t.pendingDeleteOperations++
	t.pending++
}

// UnsafePut must be called holding the lock on the tx.
func (t *batchTx) UnsafePut(bucket Bucket, key []byte, value []byte) {
	t.unsafePut(bucket, key, value)
	t.buf.put(bucket, key, value)
}

func (t *batchTx) unsafePut(bucketType Bucket, key []byte, value []byte) {
	bucket := t.tx.Bucket(bucketType.Name())
	if bucket == nil {
		log.Print("bucket does not exist")
	}
	if err := bucket.Put(key, value); err != nil {
		log.Println("failed to put value:", err)
	}
	t.pending++
}

// UnsafeRange must be called holding the lock on the tx.
func (t *batchTx) UnsafeRange(bucketType Bucket, key, endKey []byte, limit int64) ([][]byte, [][]byte) {
	bucket := t.tx.Bucket(bucketType.Name())
	if bucket == nil {
		log.Fatal("bucket does not exist")
	}
	return unsafeRange(bucket.Cursor(), key, endKey, limit)
}

func unsafeRange(c *bolt.Cursor, key, endKey []byte, limit int64) (keys [][]byte, vs [][]byte) {
	if limit <= 0 {
		limit = math.MaxInt64
	}
	var isMatch func(b []byte) bool
	if len(endKey) > 0 {
		isMatch = func(b []byte) bool { return bytes.Compare(b, endKey) < 0 }
	} else {
		isMatch = func(b []byte) bool { return bytes.Equal(b, key) }
		limit = 1
	}

	for ck, cv := c.Seek(key); ck != nil && isMatch(ck); ck, cv = c.Next() {
		vs = append(vs, cv)
		keys = append(keys, ck)
		if limit == int64(len(keys)) {
			break
		}
	}
	return keys, vs
}

// UnsafeDelete must be called holding the lock on the tx.
func (t *batchTx) UnsafeDelete(bucketType Bucket, key []byte) {
	bucket := t.tx.Bucket(bucketType.Name())
	if bucket == nil {
		log.Print("bucket does not exist")
	}
	err := bucket.Delete(key)
	if err != nil {
		log.Println("failed to delete value:", err)
	}
	t.pendingDeleteOperations++
	t.pending++
}

// Commit commits a previous tx and begins a new writable one.
func (t *batchTx) Commit() {
	t.lock()
	t.CommitGetReadLock(false)
	t.Unlock()
}

// CommitAndStop commits the previous tx and does not create a new one.
func (t *batchTx) CommitAndStop() {
	t.lock()
	t.CommitGetReadLock(true)
	t.Unlock()
}

func (t *batchTx) safePending() int {
	t.Mutex.Lock()
	defer t.Mutex.Unlock()
	return t.pending
}

func (t *batchTx) CommitGetReadLock(stop bool) {
	// 提交写操作时，必须先获取读事务的锁，保证目前没有读事务，防止数据不一致
	t.backend.readTx.Lock()
	// 如果仍有读事务，则将读事务回滚
	if t.backend.readTx.tx != nil {
		go func(tx *bolt.Tx, wg *sync.WaitGroup) {
			wg.Wait()
			if err := tx.Rollback(); err != nil {
				log.Print("\"failed to rollback tx\"")
			}
		}(t.backend.readTx.tx, t.backend.readTx.txWg)
		t.backend.readTx.reset()
	}

	t.commit(stop)
	t.pendingDeleteOperations = 0

	if !stop {
		// TODO
		t.backend.readTx.tx = t.backend.begin(false)
	}
	t.backend.readTx.Unlock()
}

func (t *batchTx) commit(stop bool) {
	// commit the last tx
	if t.tx != nil {
		if t.pending == 0 && !stop {
			return
		}
		err := wal.WalFile.Commit()
		if err != nil {
			log.Println("failed to flush wal:", err)
		}
		err = t.tx.Commit()

		t.pending = 0
		if err != nil {
			log.Println("failed to commit:", err)
		}
	}
	if !stop {
		t.tx = t.backend.begin(true)
	}
}
