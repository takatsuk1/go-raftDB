package snapshoter

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"go-raft/config"
	"go-raft/raft"
	"go-raft/storage"
	"go-raft/wal"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

var SnapManager *SnapshotManager

var (
	SnapDir  string
	tmpDBdir string
)

func initPath() {
	sDir, _, tDBdir := config.InitPath()
	SnapDir = sDir
	tmpDBdir = tDBdir
}

type SnapshotManager struct {
	Dir      string
	mu       sync.Mutex
	walSeq   int
	SnapFile *os.File
	SnapInfo raft.SnapshotInfo
	SnapName string
}

func NewSnapshotManager() *SnapshotManager {
	initPath()
	SnapManager = &SnapshotManager{
		Dir:      SnapDir,
		walSeq:   0,
		SnapFile: nil,
		SnapInfo: raft.SnapshotInfo{},
		SnapName: "",
	}
	return SnapManager
}

func (sm *SnapshotManager) SaveSnapshot(sf *raft.SnapshotInfo) error {

	sm.mu.Lock()
	defer sm.mu.Unlock()

	snapTime := time.Now().UnixNano()
	snapFile := filepath.Join(sm.Dir, strconv.FormatInt(snapTime, 10)+".snap")

	f, err := os.OpenFile(snapFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	var dbData bytes.Buffer

	rtx := storage.GlobalBackend.ConcurrentReadTx()
	rtx.RLock()
	err = rtx.GetSnapShot(&dbData)
	if err != nil {
		return err
	}
	rtx.RUnlock()

	sf.DbData = dbData.Bytes()

	enc := gob.NewEncoder(f)
	if err := enc.Encode(sf); err != nil {
		return err
	}

	sm.SnapName = snapFile
	sm.SnapFile = f
	sm.walSeq = int(wal.WalFile.GetGlobalLogSeq())

	// 清理旧的WAL文件
	return wal.WalFile.CleanupWALsBefore()
}

// 崩溃恢复回放快照
func (sm *SnapshotManager) LoadSnapshot() ([][]byte, error) {
	if sm.SnapFile == nil {
		return wal.WalFile.Read(sm.walSeq)
	}
	snapName, err := GetLatestSnapshot(sm.Dir)
	if err != nil {
		return nil, err
	}

	f, err := os.Open(snapName)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	newDbFile := filepath.Join(tmpDBdir, fmt.Sprintf("tmpDB.db"))
	newDbFileHandle, err := os.OpenFile(newDbFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return nil, err
	}
	defer newDbFileHandle.Close()

	dec := gob.NewDecoder(f)
	var snapshotInfo raft.SnapshotInfo
	if err = dec.Decode(&snapshotInfo); err != nil {
		return nil, err
	}

	tmpInfo := raft.SnapshotInfo{
		LastIndex:   snapshotInfo.LastIndex,
		LastTerm:    snapshotInfo.LastTerm,
		LastApplied: snapshotInfo.LastApplied,
		VotedFor:    snapshotInfo.VotedFor,
		CurrentTerm: snapshotInfo.CurrentTerm,
		Log:         snapshotInfo.Log,
	}
	sm.SnapInfo = tmpInfo

	data, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}

	if _, err = newDbFileHandle.Write(data); err != nil {
		return nil, err
	}

	sm.SnapFile = newDbFileHandle
	sm.SnapName = snapName
	sm.walSeq = int(wal.WalFile.GetGlobalLogSeq())
	newDbFileHandle.Close()
	storage.GlobalBackend.ChangeDB()
	return wal.WalFile.Read(sm.walSeq)
}

func GetLatestSnapshot(dir string) (string, error) {
	// 获取目录下的所有文件
	files, err := os.ReadDir(dir)
	if err != nil {
		return "", err
	}

	var latestFile string
	var latestModTime time.Time

	// 遍历文件列表，查找修改时间最新的 .snap 文件
	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), ".snap") {
			// 获取文件信息
			fileInfo, err := file.Info()
			if err != nil {
				return "", err
			}

			// 比较文件的修改时间
			if fileInfo.ModTime().After(latestModTime) {
				latestModTime = fileInfo.ModTime()
				latestFile = filepath.Join(dir, file.Name())
			}
		}
	}

	if latestFile == "" {
		return "", errors.New("no latest snapshot found")
	}

	return latestFile, nil
}

func (sm *SnapshotManager) ChangeSnapshot(snap *raft.SnapshotInfo) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	snapTime := time.Now().UnixNano()
	newSnapFile := filepath.Join(sm.Dir, strconv.FormatInt(snapTime, 10)+".snap")
	newSnapFileHandle, err := os.OpenFile(newSnapFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer newSnapFileHandle.Close()

	newDbFile := tmpDBdir + "/" + "tmpDB.db"
	newDbFileHandle, err := os.OpenFile(newDbFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}

	snapName := filepath.Join(sm.Dir, strconv.FormatInt(snapTime, 10)+".snap")

	snapshotInfo := snap

	tmpInfo := raft.SnapshotInfo{
		LastIndex:   snapshotInfo.LastIndex,
		LastTerm:    snapshotInfo.LastTerm,
		LastApplied: snapshotInfo.LastApplied,
		VotedFor:    snapshotInfo.VotedFor,
		CurrentTerm: snapshotInfo.CurrentTerm,
		Log:         snapshotInfo.Log,
	}

	sm.SnapInfo = tmpInfo
	sm.walSeq = 0
	sm.SnapName = snapName
	sm.SnapFile = newSnapFileHandle

	if _, err = newDbFileHandle.Write(snap.DbData); err != nil {
		return err
	}

	if _, err = newSnapFileHandle.Write(snap.DbData); err != nil {
		return err
	}
	newDbFileHandle.Close()

	storage.GlobalBackend.ChangeDB()
	return nil
}
