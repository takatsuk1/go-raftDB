package snapshoter_test

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"go-raft/raft"
	snapshoter "go-raft/snapshot"
	"go-raft/storage"
	"go-raft/storage/bucket"
	"go-raft/wal"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSaveSnapshot(t *testing.T) {
	// 创建临时测试目录
	testDir := filepath.Join(os.TempDir(), "snapshot_test_")
	err := os.MkdirAll(testDir, 0755)
	if err != nil {
		t.Fatalf("创建测试目录失败: %v", err)
	}
	defer os.RemoveAll(testDir)

	// 初始化存储后端
	backend := storage.NewDefaultBackend()
	defer backend.Close()

	// 初始化WAL
	stateCh := make(chan raft.WalMsg, 64)
	w, err := wal.InitWal(stateCh)
	if err != nil {
		t.Fatalf("初始化WAL失败: %v", err)
	}
	wal.WalFile = w

	fmt.Println(1231231)
	// 写入一些测试数据
	tx := backend.BatchTx()
	tx.Lock()
	tx.UnsafeCreateBucket(bucket.Test)
	tx.UnsafePut(bucket.Test, []byte("key1"), []byte("value1"))
	tx.UnsafePut(bucket.Test, []byte("key2"), []byte("value2"))
	tx.Unlock()
	tx.Commit()
	fmt.Println(43543634)

	// 获取原始数据库内容
	var originalData bytes.Buffer
	rtx := backend.ConcurrentReadTx()
	rtx.RLock()
	err = rtx.GetSnapShot(&originalData)
	rtx.RUnlock()
	if err != nil {
		t.Fatalf("获取原始数据失败: %v", err)
	}

	fmt.Println("原始数据", originalData.Bytes())

	// 创建快照管理器并设置测试目录
	sm := snapshoter.NewSnapshotManager()
	sm.Dir = testDir

	// 创建测试快照信息
	testSnapshot := &raft.SnapshotInfo{
		LastIndex:   100,
		LastTerm:    2,
		VotedFor:    "node1",
		CurrentTerm: 3,
		Log:         []byte("test log data"),
	}

	// 保存快照
	err = sm.SaveSnapshot(testSnapshot)
	if err != nil {
		t.Fatalf("保存快照失败: %v", err)
	}

	// 验证快照文件是否存在
	if _, err := os.Stat(sm.SnapName); os.IsNotExist(err) {
		t.Errorf("快照文件未创建: %v", err)
	}

	// 读取快照文件并验证内容
	snapFile, err := os.Open(sm.SnapName)
	if err != nil {
		t.Fatalf("打开快照文件失败: %v", err)
	}
	defer snapFile.Close()

	// 解码快照信息
	dec := gob.NewDecoder(snapFile)
	var decodedInfo raft.SnapshotInfo
	if err := dec.Decode(&decodedInfo); err != nil {
		t.Fatalf("解码快照信息失败: %v", err)
	}

	// 验证快照信息
	if decodedInfo.LastIndex != testSnapshot.LastIndex ||
		decodedInfo.LastTerm != testSnapshot.LastTerm ||
		decodedInfo.VotedFor != testSnapshot.VotedFor ||
		decodedInfo.CurrentTerm != testSnapshot.CurrentTerm {
		t.Error("快照信息不匹配")
	}
}

func TestSaveAndLoadSnapshot(t *testing.T) {
	// 初始化存储后端
	backend := storage.NewDefaultBackend()
	defer backend.Close()

	// 初始化WAL
	stateCh := make(chan raft.WalMsg)
	w, err := wal.InitWal(stateCh)
	if err != nil {
		t.Fatalf("初始化WAL失败: %v", err)
	}
	wal.WalFile = w

	sm := snapshoter.NewSnapshotManager()

	// 创建测试用的日志数据
	wb := new(bytes.Buffer)
	e := gob.NewEncoder(wb)

	log := []raft.Entry{
		{Term: 1, Command: []byte("test command 1")},
		{Term: 2, Command: []byte("test command 2")},
	}
	if err := e.Encode(log); err != nil {
		t.Fatalf("编码日志失败: %v", err)
	}
	logs := wb.Bytes()

	testSnapshot := &raft.SnapshotInfo{
		LastIndex:   100,
		LastTerm:    2,
		VotedFor:    "node1",
		CurrentTerm: 3,
		Log:         logs,
	}

	err = sm.SaveSnapshot(testSnapshot)
	if err != nil {
		t.Fatalf("保存快照失败: %v", err)
	}

	walData, err := sm.LoadSnapshot()
	if err != nil {
		t.Fatalf("加载快照失败: %v", err)
	}

	// 验证WAL数据
	if len(walData) > 0 {
		for _, data := range walData {
			if len(data) == 0 {
				t.Error("WAL数据为空")
			}
		}
	}

	if sm.SnapInfo.LastIndex != testSnapshot.LastIndex {
		t.Errorf("LastIndex不匹配: 期望 %d, 实际 %d", testSnapshot.LastIndex, sm.SnapInfo.LastIndex)
	}
	if sm.SnapInfo.LastTerm != testSnapshot.LastTerm {
		t.Errorf("LastTerm不匹配: 期望 %d, 实际 %d", testSnapshot.LastTerm, sm.SnapInfo.LastTerm)
	}
	if sm.SnapInfo.VotedFor != testSnapshot.VotedFor {
		t.Errorf("VotedFor不匹配: 期望 %s, 实际 %s", testSnapshot.VotedFor, sm.SnapInfo.VotedFor)
	}
	if sm.SnapInfo.CurrentTerm != testSnapshot.CurrentTerm {
		t.Errorf("CurrentTerm不匹配: 期望 %d, 实际 %d", testSnapshot.CurrentTerm, sm.SnapInfo.CurrentTerm)
	}

	var decodedLog []raft.Entry
	r := bytes.NewReader(sm.SnapInfo.Log)
	d := gob.NewDecoder(r)
	if err := d.Decode(&decodedLog); err != nil {
		t.Fatalf("解码日志失败: %v", err)
	}

	if len(decodedLog) != len(log) {
		t.Errorf("日志长度不匹配: 期望 %d, 实际 %d", len(log), len(decodedLog))
	}

	for i := 0; i < len(log); i++ {
		if decodedLog[i].Term != log[i].Term {
			t.Errorf("日志项 %d 的Term不匹配: 期望 %d, 实际 %d", i, log[i].Term, decodedLog[i].Term)
		}
		if !bytes.Equal(decodedLog[i].Command.([]byte), log[i].Command.([]byte)) {
			t.Errorf("日志项 %d 的Command不匹配: 期望 %s, 实际 %s",
				i, string(log[i].Command.([]byte)), string(decodedLog[i].Command.([]byte)))
		}
	}
}

func TestGetLatestSnapshot(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), "snapshot_test_")
	err := os.MkdirAll(testDir, 0755)
	if err != nil {
		t.Fatalf("无法创建测试目录: %v", err)
	}
	defer os.RemoveAll(testDir)

	files := []string{
		filepath.Join(testDir, "1.snap"),
		filepath.Join(testDir, "2.snap"),
		filepath.Join(testDir, "3.snap"),
	}

	for i, file := range files {
		f, err := os.Create(file)
		if err != nil {
			t.Fatalf("创建测试文件失败: %v", err)
		}
		f.Close()

		modTime := time.Now().Add(time.Duration(i) * time.Second)
		err = os.Chtimes(file, modTime, modTime)
		if err != nil {
			t.Fatalf("设置文件时间失败: %v", err)
		}
	}

	latestFile, err := snapshoter.GetLatestSnapshot(testDir)
	if err != nil {
		t.Fatalf("获取最新快照失败: %v", err)
	}

	expectedFile := files[len(files)-1]
	if filepath.Base(latestFile) != filepath.Base(expectedFile) {
		t.Errorf("获取到的最新快照文件不正确: 期望 %s, 实际 %s", expectedFile, latestFile)
	}
}

func TestChangeSnapshot(t *testing.T) {
	// 初始化WAL
	stateCh := make(chan raft.WalMsg)
	w, err := wal.InitWal(stateCh)
	if err != nil {
		t.Fatalf("初始化WAL失败: %v", err)
	}
	wal.WalFile = w

	backend := storage.NewDefaultBackend()
	defer backend.Close()

	leaderSm := snapshoter.NewSnapshotManager()
	followerSm := snapshoter.NewSnapshotManager()

	wb := new(bytes.Buffer)
	e := gob.NewEncoder(wb)
	leaderLog := []raft.Entry{
		{Term: 3, Command: []byte("leader command 1")},
		{Term: 4, Command: []byte("leader command 2")},
	}
	if err := e.Encode(leaderLog); err != nil {
		t.Fatalf("编码leader日志失败: %v", err)
	}
	logs := wb.Bytes()

	leaderSnapshot := &raft.SnapshotInfo{
		LastIndex:   200,
		LastTerm:    4,
		VotedFor:    "leader",
		CurrentTerm: 5,
		Log:         logs,
	}

	err = leaderSm.SaveSnapshot(leaderSnapshot)
	if err != nil {
		t.Fatalf("保存leader快照失败: %v", err)
	}

	err = followerSm.ChangeSnapshot(leaderSnapshot)
	if err != nil {
		t.Fatalf("更改follower快照失败: %v", err)
	}

	if followerSm.SnapInfo.LastIndex != leaderSnapshot.LastIndex {
		t.Errorf("LastIndex不匹配: 期望 %d, 实际 %d", leaderSnapshot.LastIndex, followerSm.SnapInfo.LastIndex)
	}
	if followerSm.SnapInfo.LastTerm != leaderSnapshot.LastTerm {
		t.Errorf("LastTerm不匹配: 期望 %d, 实际 %d", leaderSnapshot.LastTerm, followerSm.SnapInfo.LastTerm)
	}
	if followerSm.SnapInfo.VotedFor != leaderSnapshot.VotedFor {
		t.Errorf("VotedFor不匹配: 期望 %s, 实际 %s", leaderSnapshot.VotedFor, followerSm.SnapInfo.VotedFor)
	}
	if followerSm.SnapInfo.CurrentTerm != leaderSnapshot.CurrentTerm {
		t.Errorf("CurrentTerm不匹配: 期望 %d, 实际 %d", leaderSnapshot.CurrentTerm, followerSm.SnapInfo.CurrentTerm)
	}

	var decodedLog []raft.Entry
	r := bytes.NewReader(followerSm.SnapInfo.Log)
	d := gob.NewDecoder(r)
	if err := d.Decode(&decodedLog); err != nil {
		t.Fatalf("解码follower日志失败: %v", err)
	}

	if len(decodedLog) != len(leaderLog) {
		t.Errorf("日志长度不匹配: 期望 %d, 实际 %d", len(leaderLog), len(decodedLog))
	}

	for i := 0; i < len(leaderLog); i++ {
		if decodedLog[i].Term != leaderLog[i].Term {
			t.Errorf("日志项 %d 的Term不匹配: 期望 %d, 实际 %d",
				i, leaderLog[i].Term, decodedLog[i].Term)
		}
		if !bytes.Equal(decodedLog[i].Command.([]byte), leaderLog[i].Command.([]byte)) {
			t.Errorf("日志项 %d 的Command不匹配: 期望 %s, 实际 %s",
				i, string(leaderLog[i].Command.([]byte)), string(decodedLog[i].Command.([]byte)))
		}
	}
}

//func TestSnapshotWalStorageIntegration(t *testing.T) {
//	// 初始化存储后端
//	backend := storage.NewDefaultBackend()
//	defer backend.Close()
//
//	// 初始化WAL
//	stateCh := make(chan raft.WalMsg)
//	w, err := wal.InitWal(stateCh)
//	if err != nil {
//		t.Fatalf("初始化WAL失败: %v", err)
//	}
//	wal.WalFile = w
//
//	// 初始化快照管理器
//	sm := snapshoter.NewSnapshotManager()
//
//	// 1.首先写入一些数据到存储中
//	tx := backend.BatchTx()
//	tx.Lock()
//	tx.UnsafeCreateBucket(bucket.Test)
//	tx.UnsafePut(bucket.Test, []byte("key1"), []byte("value1"))
//	tx.UnsafePut(bucket.Test, []byte("key2"), []byte("value2"))
//	tx.Unlock()
//	tx.Commit()
//
//	// 2.创建一些WAL记录
//	testRecords := []*wal.Record{
//		{
//			Data: []byte("operation1"),
//			CRC:  crc32.ChecksumIEEE([]byte("operation1")),
//		},
//		{
//			Data: []byte("operation2"),
//			CRC:  crc32.ChecksumIEEE([]byte("operation2")),
//		},
//	}
//
//	// 写入WAL记录
//	for _, record := range testRecords {
//		if err := w.Save(record); err != nil {
//			t.Fatalf("保存WAL记录失败: %v", err)
//		}
//		if err := w.Commit(); err != nil {
//			t.Fatalf("提交WAL记录失败: %v", err)
//		}
//	}
//
//	// 3.创建快照
//	var dbData bytes.Buffer
//	rtx := backend.ConcurrentReadTx()
//	rtx.RLock()
//	err = rtx.GetSnapShot(&dbData)
//	rtx.RUnlock()
//	if err != nil {
//		t.Fatalf("获取数据库快照失败: %v", err)
//	}
//
//	snapshotInfo := &raft.SnapshotInfo{
//		LastIndex:   100,
//		LastTerm:    2,
//		VotedFor:    "node1",
//		CurrentTerm: 3,
//		Log:         logBuffer.Bytes(),
//		DbData:      dbData.Bytes(),
//	}
//
//	// 4. 模拟崩溃恢复
//	backend.Close()
//	w.Close()
//
//	// 重新初始化
//	backend = storage.NewDefaultBackend()
//	defer backend.Close()
//
//	stateCh = make(chan raft.WalMsg)
//	snapshotCh = make(chan []byte)
//	w, err = wal.InitWal(stateCh, snapshotCh)
//	if err != nil {
//		t.Fatalf("重新初始化WAL失败: %v", err)
//	}
//	wal.WalFile = w
//
//	// 加载快照
//	walData, err := sm.LoadSnapshot()
//	if err != nil {
//		t.Fatalf("加载快照失败: %v", err)
//	}
//
//	// 验证WAL数据
//	if len(walData) != len(testRecords) {
//		t.Errorf("WAL记录数量不匹配: 期望 %d, 实际 %d", len(testRecords), len(walData))
//	}
//
//	// 验证数据库内容
//	tx = backend.BatchTx()
//	tx.Lock()
//	keys, values := tx.UnsafeRange(bucket.Test, []byte("key1"), nil, 0)
//	tx.Unlock()
//
//	if len(keys) == 0 || string(values[0]) != "value1" {
//		t.Errorf("存储内容验证失败: 期望 value1, 实际 %s", string(values[0]))
//	}
//
//	// 验证快照元数据
//	if sm.SnapInfo.LastIndex != snapshotInfo.LastIndex {
//		t.Errorf("LastIndex不匹配: 期望 %d, 实际 %d",
//			snapshotInfo.LastIndex, sm.SnapInfo.LastIndex)
//	}
//	if sm.SnapInfo.LastTerm != snapshotInfo.LastTerm {
//		t.Errorf("LastTerm不匹配: 期望 %d, 实际 %d",
//			snapshotInfo.LastTerm, sm.SnapInfo.LastTerm)
//	}
//	if sm.SnapInfo.VotedFor != snapshotInfo.VotedFor {
//		t.Errorf("VotedFor不匹配: 期望 %s, 实际 %s",
//			snapshotInfo.VotedFor, sm.SnapInfo.VotedFor)
//	}
//	if sm.SnapInfo.CurrentTerm != snapshotInfo.CurrentTerm {
//		t.Errorf("CurrentTerm不匹配: 期望 %d, 实际 %d",
//			snapshotInfo.CurrentTerm, sm.SnapInfo.CurrentTerm)
//	}
//
//	// 验证日志内容
//	var decodedLog []raft.Entry
//	logReader := bytes.NewReader(sm.SnapInfo.Log)
//	logDecoder := gob.NewDecoder(logReader)
//	if err := logDecoder.Decode(&decodedLog); err != nil {
//		t.Fatalf("解码日志失败: %v", err)
//	}
//
//	if len(decodedLog) != len(logs) {
//		t.Errorf("日志长度不匹配: 期望 %d, 实际 %d", len(logs), len(decodedLog))
//	}
//
//	for i := 0; i < len(logs); i++ {
//		if decodedLog[i].Term != logs[i].Term {
//			t.Errorf("日志项 %d 的Term不匹配: 期望 %d, 实际 %d",
//				i, logs[i].Term, decodedLog[i].Term)
//		}
//		if !bytes.Equal(decodedLog[i].Command.([]byte), logs[i].Command.([]byte)) {
//			t.Errorf("日志项 %d 的Command不匹配: 期望 %s, 实际 %s",
//				i, string(logs[i].Command.([]byte)), string(decodedLog[i].Command.([]byte)))
//		}
//	}
//}
