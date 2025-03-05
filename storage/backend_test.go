package storage_test

import (
	"go-raft/storage"
	"go-raft/storage/bucket"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestBackendClose(t *testing.T) {
	b, _ := storage.NewTmpBackend(t, time.Hour, 1000)

	done := make(chan struct{})
	go func() {
		err := b.Close()
		if err != nil {
			log.Print("close error = ", err)
		}
		done <- struct{}{}
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		log.Print("timeout")
	}
}

func TestBackendWriteback(t *testing.T) {
	b, _ := storage.NewDefaultTmpBackend(t)
	defer storage.Close(t, b)

	tx := b.BatchTx()
	tx.Lock()
	tx.UnsafeCreateBucket(bucket.Key)
	tx.UnsafePut(bucket.Key, []byte("abc"), []byte("bar"))
	tx.UnsafePut(bucket.Key, []byte("def"), []byte("baz"))
	tx.UnsafePut(bucket.Key, []byte("overwrite"), []byte("1"))
	tx.Unlock()

	// overwrites should be propagated too
	tx.Lock()
	tx.UnsafePut(bucket.Key, []byte("overwrite"), []byte("2"))
	tx.Unlock()

	keys := []struct {
		key   []byte
		end   []byte
		limit int64

		wkey [][]byte
		wval [][]byte
	}{
		{
			key: []byte("abc"),
			end: nil,

			wkey: [][]byte{[]byte("abc")},
			wval: [][]byte{[]byte("bar")},
		},
		{
			key: []byte("abc"),
			end: []byte("def"),

			wkey: [][]byte{[]byte("abc")},
			wval: [][]byte{[]byte("bar")},
		},
		{
			key: []byte("abc"),
			end: []byte("deg"),

			wkey: [][]byte{[]byte("abc"), []byte("def")},
			wval: [][]byte{[]byte("bar"), []byte("baz")},
		},
		{
			key:   []byte("abc"),
			end:   []byte("\xff"),
			limit: 1,

			wkey: [][]byte{[]byte("abc")},
			wval: [][]byte{[]byte("bar")},
		},
		{
			key: []byte("abc"),
			end: []byte("\xff"),

			wkey: [][]byte{[]byte("abc"), []byte("def"), []byte("overwrite")},
			wval: [][]byte{[]byte("bar"), []byte("baz"), []byte("2")},
		},
	}
	rtx := b.ConcurrentReadTx()
	for i, tt := range keys {
		func() {
			rtx.RLock()
			defer rtx.RUnlock()
			k, v := rtx.UnsafeRange(bucket.Key, tt.key, tt.end, tt.limit)
			if !reflect.DeepEqual(tt.wkey, k) || !reflect.DeepEqual(tt.wval, v) {
				t.Errorf("#%d: want k=%+v, v=%+v; got k=%+v, v=%+v", i, tt.wkey, tt.wval, k, v)
			}
		}()
	}
}

func TestConcurrentReadTx(t *testing.T) {
	b, _ := storage.NewTmpBackend(t, time.Hour, 10000)
	defer storage.Close(t, b)

	wtx1 := b.BatchTx()
	wtx1.Lock()
	wtx1.UnsafeCreateBucket(bucket.Key)
	wtx1.UnsafePut(bucket.Key, []byte("abc"), []byte("ABC"))
	wtx1.UnsafePut(bucket.Key, []byte("overwrite"), []byte("1"))
	wtx1.Unlock()

	wtx2 := b.BatchTx()
	wtx2.Lock()
	wtx2.UnsafePut(bucket.Key, []byte("def"), []byte("DEF"))
	wtx2.UnsafePut(bucket.Key, []byte("overwrite"), []byte("2"))
	wtx2.Unlock()

	rtx := storage.GlobalBackend.ConcurrentReadTx()
	rtx.RLock()
	k, v := rtx.UnsafeRange(bucket.Key, []byte("abc"), []byte("\xff"), 0)
	rtx.RUnlock()
	wKey := [][]byte{[]byte("abc"), []byte("def"), []byte("overwrite")}
	wVal := [][]byte{[]byte("ABC"), []byte("DEF"), []byte("2")}
	if !reflect.DeepEqual(wKey, k) || !reflect.DeepEqual(wVal, v) {
		t.Errorf("want k=%+v, v=%+v; got k=%+v, v=%+v", wKey, wVal, k, v)
	}
}

func TestBackend_ChangeDB(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), "backend_test_"+time.Now().Format("20060102150405"))
	err := os.MkdirAll(testDir, 0755)
	if err != nil {
		t.Fatalf("创建测试目录失败: %v", err)
	}
	defer os.RemoveAll(testDir)

	originalPath := storage.Path
	storage.Path = testDir
	defer func() {
		storage.Path = originalPath
	}()

	backend := storage.NewDefaultBackend()
	defer backend.Close()

	tx := backend.BatchTx()
	tx.Lock()
	tx.UnsafeCreateBucket(bucket.Test)
	tx.UnsafePut(bucket.Test, []byte("key1"), []byte("value1"))
	tx.UnsafePut(bucket.Test, []byte("key2"), []byte("value2"))
	tx.Unlock()
	tx.Commit()

	tx = backend.BatchTx()
	tx.Lock()
	keys, values := tx.UnsafeRange(bucket.Test, []byte("key1"), nil, 0)
	tx.Unlock()

	// 创建一个空的临时数据库文件
	tmpDB := filepath.Join(testDir, "tmpDB.db")
	file, err := os.Create(tmpDB)
	if err != nil {
		t.Fatal(err)
	}
	file.Close()

	// 执行数据库切换
	backend.ChangeDB()

	tx = backend.BatchTx()
	tx.Lock()
	tx.UnsafeCreateBucket(bucket.Test)
	k, v := tx.UnsafeRange(bucket.Test, []byte("key1"), nil, 0)
	if len(k) != 0 {
		t.Errorf("期望数据库为空，但仍能读取到数据: key=%s, value=%s",
			string(k[0]), string(v[0]))
	}
	tx.Unlock()

	// 验证新数据库是否为空（通过尝试读取之前写入的键值）
	rtx := backend.ConcurrentReadTx()
	rtx.RLock()
	keys, values = rtx.UnsafeRange(bucket.Test, []byte("key1"), nil, 0)
	rtx.RUnlock()

	if len(keys) != 0 {
		t.Errorf("期望数据库为空，但仍能读取到数据: key=%s, value=%s",
			string(keys[0]), string(values[0]))
	}

	// 验证旧数据库文件是否存在
	oldDB := filepath.Join(testDir, "oldDB.db")
	if _, err := os.Stat(oldDB); os.IsNotExist(err) {
		t.Error("旧数据库文件不存在")
	}
}
