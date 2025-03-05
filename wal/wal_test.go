package wal

import (
	"bytes"
	"fmt"
	"go-raft/raft"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// 测试WAL的基本操作
func TestWALBasicOperations(t *testing.T) {
	// 创建临时测试目录
	testDir := filepath.Join(os.TempDir(), "wal_test")
	err := os.MkdirAll(testDir, 0755)
	if err != nil {
		t.Fatalf("创建测试目录失败: %v", err)
	}
	defer os.RemoveAll(testDir)

	// 初始化WAL
	walCh := make(chan raft.WalMsg)
	w, err := InitWal(walCh)
	if err != nil {
		t.Fatalf("初始化WAL失败: %v", err)
	}
	defer w.Close()

	// 测试写入和读取
	testData := []byte("test_data")
	fmt.Printf("测试数据长度: %d 字节\n", len(testData))

	go func() {
		walCh <- raft.WalMsg{Data: testData}
	}()

	// 保存数据
	if err := w.Save(); err != nil {
		t.Fatalf("保存数据失败: %v", err)
	}

	// 提交数据
	if err := w.Commit(); err != nil {
		t.Fatalf("提交数据失败: %v", err)
	}

	// 读取数据
	data, err := w.Read(0)
	if err != nil {
		t.Fatalf("读取数据失败: %v", err)
	}

	if !bytes.Equal(data[0], testData) {
		t.Errorf("数据不匹配: 期望 %v, 实际 %v", testData, data[0])
	}
}

// 测试WAL文件切换
func TestWALFileSwitch(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), "wal_test")
	err := os.MkdirAll(testDir, 0755)
	if err != nil {
		t.Fatalf("创建测试目录失败: %v", err)
	}
	defer os.RemoveAll(testDir)

	walCh := make(chan raft.WalMsg)
	w, err := InitWal(walCh)
	if err != nil {
		t.Fatalf("初始化WAL失败: %v", err)
	}
	defer w.Close()

	// 写入足够多的数据触发文件切换
	originalSeq := w.getCurrentWalSeq()
	data := make([]byte, MaxPayloadSize) // 使用最大负载大小

	// 计算需要写入的次数
	writeTimes := (SegmentSize / int64(RecordSize)) + 1 // 确保超过段大小
	fmt.Println(writeTimes)

	for i := 0; i < int(writeTimes); i++ {
		go func() {
			walCh <- raft.WalMsg{Data: data}
		}()
		if err := w.Save(); err != nil {
			t.Fatal(err)
		}
		if err := w.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// 验证文件是否切换
	if w.getCurrentWalSeq() <= originalSeq {
		t.Error("文件未切换")
	}
}

// 测试WAL文件清理
func TestWALCleanup(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), "wal_test")
	err := os.MkdirAll(testDir, 0755)
	if err != nil {
		t.Fatalf("创建测试目录失败: %v", err)
	}
	defer os.RemoveAll(testDir)

	walCh := make(chan raft.WalMsg)
	w, err := InitWal(walCh)
	if err != nil {
		t.Fatalf("初始化WAL失败: %v", err)
	}
	defer w.Close()

	// 计算需要写入的次数
	data := make([]byte, MaxPayloadSize)                // 使用最大负载大小
	writeTimes := (SegmentSize / int64(RecordSize)) + 1 // 确保超过段大小
	fmt.Println(writeTimes)

	for i := 0; i < int(writeTimes); i++ {
		go func() {
			walCh <- raft.WalMsg{Data: data}
		}()
		if err := w.Save(); err != nil {
			t.Fatal(err)
		}
		if err := w.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	//filePipe分配文件需要时间，不然clean的时候读取不到新分配的文件
	time.Sleep(3 * time.Second)

	// 清理WAL文件
	if err := w.CleanupWALsBefore(); err != nil {
		t.Fatalf("清理WAL文件失败: %v", err)
	}

	// 验证清理结果
	files, err := os.ReadDir(w.dir)
	if err != nil {
		t.Fatal(err)
	}

	walCount := 0
	for _, file := range files {
		if filepath.Ext(file.Name()) == ".wal" {
			walCount++
		}
	}

	if walCount > 2 {
		t.Errorf("WAL文件清理失败: 仍存在 %d 个文件", walCount)
	}
}
