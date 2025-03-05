package storage

import (
	"go-raft/config"
	bolt "go.etcd.io/bbolt"
	"log"
	"os"
	"sync"
	"time"
)

var (
	defaultBatchLimit    = 10000
	defaultBatchInterval = 100 * time.Millisecond

	//自定义存储路径
	Path string
)

func initPath() {
	_, _, dbPath := config.InitPath()
	Path = dbPath
}

type Backend interface {
	BatchTx() BatchTx
	ConcurrentReadTx() ReadTx

	Close() error
	ChangeDB()
}

type txReadBufferCache struct {
	mu         sync.Mutex
	buf        *txReadBuffer
	bufVersion uint64
}

type backend struct {
	mu    sync.RWMutex
	bopts *bolt.Options
	db    *bolt.DB

	batchInterval time.Duration
	batchLimit    int
	batchTx       *batchTx

	readTx *baseReadTx

	txReadBufferCache txReadBufferCache

	stopc chan struct{}
	donec chan struct{}
}

var GlobalBackend Backend

type BackendConfig struct {
	// BatchInterval is the maximum time before flushing the BatchTx.
	BatchInterval time.Duration
	// BatchLimit is the maximum puts before flushing the BatchTx.
	BatchLimit int
}

type BackendConfigOption func(*BackendConfig)

func DefaultBackendConfig() BackendConfig {
	return BackendConfig{
		BatchInterval: defaultBatchInterval,
		BatchLimit:    defaultBatchLimit,
	}
}

func New(bcfg BackendConfig) Backend {
	GlobalBackend = newBackend(bcfg)
	return GlobalBackend
}

func NewDefaultBackend(opts ...BackendConfigOption) Backend {
	bcfg := DefaultBackendConfig()
	for _, opt := range opts {
		opt(&bcfg)
	}

	GlobalBackend = newBackend(bcfg)
	return GlobalBackend
}

func newBackend(bcfg BackendConfig) *backend {
	initPath()
	bopts := &bolt.Options{}

	db, err := bolt.Open(Path+"/"+"boltDB.db", 0600, bopts)
	if err != nil {
		log.Fatal("failed to open database", err)
	}

	b := &backend{
		bopts: bopts,
		db:    db,

		batchInterval: bcfg.BatchInterval,
		batchLimit:    bcfg.BatchLimit,

		readTx: &baseReadTx{
			buf: txReadBuffer{
				txBuffer:   txBuffer{make(map[BucketID]*bucketBuffer)},
				bufVersion: 0,
			},
			buckets: make(map[BucketID]*bolt.Bucket),
			txWg:    new(sync.WaitGroup),
			txMu:    new(sync.RWMutex),
		},

		txReadBufferCache: txReadBufferCache{
			mu:         sync.Mutex{},
			bufVersion: 0,
			buf:        nil,
		},

		stopc: make(chan struct{}),
		donec: make(chan struct{}),
	}

	b.batchTx = newBatchTx(b)

	go b.run()
	return b
}

// 通过重命名操作更换db文件
func (b *backend) ChangeDB() {
	bopts := &bolt.Options{}

	// 提交所有事务
	close(b.stopc)
	<-b.donec
	if b.batchTx != nil {
		b.batchTx.CommitAndStop()
	}
	if b.readTx != nil {
		if b.readTx.txWg != nil {
			b.readTx.txWg.Wait()
		}
	}
	//关闭数据库
	if b.db != nil {
		if err := b.db.Close(); err != nil {
			log.Printf("failed to close database: %v", err)
		}
	}
	//重命名更换数据库

	oldName := Path + "/" + "oldDB.db"
	tmpName := Path + "/" + "tmpDB.db"
	realName := Path + "/" + "boltDB.db"
	if err := os.Rename(realName, oldName); err != nil && !os.IsNotExist(err) {
		log.Printf("failed to rename database: %v", err)
	}
	if err := os.Rename(tmpName, realName); err != nil {
		log.Fatal("failed to rename temp database", err)
	}
	db, err := bolt.Open(realName, 0600, bopts)
	if err != nil {
		log.Fatal("failed to open database:", err, realName)
	}

	//初始化新数据库
	b.db = db
	b.readTx = &baseReadTx{
		buf: txReadBuffer{
			txBuffer:   txBuffer{make(map[BucketID]*bucketBuffer)},
			bufVersion: 0,
		},
		buckets: make(map[BucketID]*bolt.Bucket),
		txWg:    new(sync.WaitGroup),
		txMu:    new(sync.RWMutex),
	}

	b.batchTx = newBatchTx(b)

	b.stopc = make(chan struct{})
	b.donec = make(chan struct{})
	go b.run()
}

// 读写事务
func (b *backend) BatchTx() BatchTx {
	return b.batchTx
}

func (b *backend) ConcurrentReadTx() ReadTx {
	//保护readTx，防止后面使用buf时被修改
	b.readTx.RLock()
	defer b.readTx.RUnlock()
	b.readTx.txWg.Add(1)

	b.txReadBufferCache.mu.Lock()

	//获取当前的read缓冲
	curCache := b.txReadBufferCache.buf
	//获取当前的read缓冲版本号
	curCacheVer := b.txReadBufferCache.bufVersion
	//获取当前read缓冲的实际版本号
	curBufVer := b.readTx.buf.bufVersion

	//判断使用的read缓冲是否为空
	isEmptyCache := curCache == nil
	//判断使用的read缓冲版本号与实际缓冲版本号是否一致
	isStaleCache := curCacheVer != curBufVer

	var buf *txReadBuffer
	switch {
	case isEmptyCache:
		// 为空的情况只会发生一次，因此可以接受有锁进行copy
		//unsafeCopy() 的执行成本较低，在锁内进行一次复制不会对并发性能造成明显影响。
		//如果先 Unlock() 再 Lock()，会增加一次上下文切换，可能导致额外的竞争，反而降低性能。
		curBuf := b.readTx.buf.unsafeCopy()
		buf = &curBuf
	case isStaleCache:
		b.txReadBufferCache.mu.Unlock()
		curBuf := b.readTx.buf.unsafeCopy()
		b.txReadBufferCache.mu.Lock()
		buf = &curBuf
	default:
		buf = curCache
	}
	// 为空或者触发isStaleCache释放了锁。这时候其他线程调用copy可能更改b.txReadBufferCache.bufVersion
	// 其他线程调用copy后可能先于此线程更新buf，如果此时version不同时继续更新buf，可能会将其他线程新的buf覆盖了。
	// 因此只能让后面新的线程更新，而此线程用旧的buf一般情况下会满足查询需求。

	// 总结：防止isStaleCache解锁后被新的线程抢先一步覆盖了buf。防止旧线程覆盖新的buf
	if isEmptyCache || curCacheVer == b.txReadBufferCache.bufVersion {
		b.txReadBufferCache.buf = buf
		b.txReadBufferCache.bufVersion = curBufVer
	}

	b.txReadBufferCache.mu.Unlock()

	return &concurrentReadTx{
		baseReadTx: baseReadTx{
			buf:     *buf,
			txMu:    b.readTx.txMu,
			tx:      b.readTx.tx,
			buckets: b.readTx.buckets,
			txWg:    b.readTx.txWg,
		},
	}
}

func (b *backend) run() {
	defer close(b.donec)
	t := time.NewTimer(b.batchInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
		case <-b.stopc:
			b.batchTx.CommitAndStop()
			return
		}
		//有待提交的事务，提交
		if b.batchTx.safePending() != 0 {
			b.batchTx.Commit()
		}
		t.Reset(b.batchInterval)
	}
}

func (b *backend) Close() error {
	close(b.stopc)
	<-b.donec
	return b.db.Close()
}

func (b *backend) begin(write bool) *bolt.Tx {
	b.mu.RLock()
	tx := b.unsafeBegin(write)
	b.mu.RUnlock()
	return tx
}

func (b *backend) unsafeBegin(write bool) *bolt.Tx {
	tx, err := b.db.Begin(write)
	if err != nil {
		log.Print("\"failed to begin tx\"")
	}
	return tx
}
