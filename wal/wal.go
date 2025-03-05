package wal

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"go-raft/config"
	"go-raft/raft"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
)

//暂定机制：两阶段写入，先写入缓冲区中，事务提交时再刷盘。wal只写入raft节点状态变更的日志，不写入操作日志。 snapshot写入数据库数据以及raft状态。
//
//还没触发snapshot的崩溃恢复：
//1.leader 在收到操作日志，在写入raft日志之前崩溃。无影响，客户端超时重新请求
//2.leader 收到操作日志，写入raft日志后，写入wal之前崩溃，无影响，客户端超时重新请求
//3. leader收到操作日志，写入raft日志后，写入wal之后，写入到大多数节点之前崩溃。没有日志的raft节点成为leader，客户端超时重新请求，有日志的raft节点会被新leader覆盖。
//而旧leader通过wal恢复，如果落后日志过多，会被发snapshot，覆盖数据库以及raft状态，不受影响。如果落后日志不多，恢复后成为follower，会被新leader进行截断日志，之后会自己应用前面的日志
//3. leader收到操作日志，写入raft日志，写入wal后，写入到大多数节点之后崩溃。有新日志的raft节点成为leader。客户端超时重新请求。！！！请求幂等性。
//新leader如何应用这条日志的？新leader会发送心跳，心跳被同意后，因为新leader会初始化所有matchindex为上次快照的位置，而所有nextindex默认为新leader的最后一条日志。
//然后新leader会进行回退，找到之前作为follower最后提交的日志。（此时因为之前作为follower，是要收到leader的第二次日志复制请求才会更新commitindex，所以这时候作为新leader的commitIndex一定是在没应用的日志之前的）
//然后进行应用，就会把之前的日志应用了。而旧leader崩溃恢复后时，如果落后日志过多，会被发snapshot，不受影响，如果落后日志不多，恢复后成为follower，leader没有这条日志，会被发送这条日志
//
//4.follower收到日志，写入wal之前崩溃，利用wal恢复，无影响，跟随leader
//5.follower收到日志，写入wal之后崩溃，利用wal恢复，无影响，收到leader心跳后，会应用之前的日志
//
//以上只利用wal进行恢复，不会对底层数据库进行回放，因为只恢复raft的日志状态，会跟着新leader的指引应用前面的所有日志。
//
//如果只是重启的操作，利用wal恢复raft状态，底层数据库文件还存在，并且lastapplied这些状态都存在，不会重复应用
//触发了snapshot的崩溃恢复。
//
//场景与上面相同。只是恢复的时候使用了snapshot，将底层数据库的数据进行了恢复，并且利用snapshot+wal对raft状态进行了恢复。随后依然跟随新leader将之前没应用的日志进行了应用，更新了数据库。
//

var CurrentFileName string
var WalFile *WAL

type WAL struct {
	dir      string
	filename string
	file     *os.File
	writer   *bufio.Writer
	walCh    chan raft.WalMsg
	buffer   *bytes.Buffer // 全局缓冲区
	commitCh chan struct{} // 提交信号通道

	// pipeline相关
	segmentSize int64
	currentSize int64
	seq         int
	pipeline    *filePipeline
}

func (w *WAL) switchSegment() error {

	// 直接从pipeline获取新文件
	newFile, err := w.pipeline.File()
	if err != nil {
		return err
	}

	// 刷新当前文件
	if err := w.writer.Flush(); err != nil {
		return err
	}

	// 切换文件
	oldFile := w.file
	w.file = newFile
	w.filename = newFile.Name()
	w.writer = bufio.NewWriter(newFile)
	w.seq++
	w.currentSize = 0
	CurrentFileName = w.filename

	// 异步关闭旧文件
	go func() {
		if err := oldFile.Close(); err != nil {
			//TODo
		}
	}()

	return nil
}

const (
	SegmentSize = 64 * 1024 * 1024 // 64MB，每个文件固定大小
)

var (
	globalSeq  uint64 = 0
	zeroHeader        = make([]byte, headerSize)
	WalPath    string
)

func initPath() {
	_, WPath, _ := config.InitPath()
	WalPath = WPath
}

type Record struct {
	Seq  uint64 // 全局递增序列号
	Data []byte // 记录数据
	CRC  uint32 // CRC校验和
}

// 记录头部大小:CRC(4) +  Len(4)
const (
	headerSize = 16 // 8(序列号) + 4(数据长度) + 4(CRC)
)

// Open 打开已存在的WAL文件
func InitWal(walCh chan raft.WalMsg) (*WAL, error) {
	initPath()
	filePipeLine := NewFilePipeline(WalPath, SegmentSize)
	firstFile, err := filePipeLine.File()
	if err != nil {
		return nil, err
	}
	WalFile = &WAL{
		dir:         WalPath,
		filename:    firstFile.Name(),
		file:        firstFile,
		walCh:       walCh,
		writer:      bufio.NewWriter(firstFile),
		pipeline:    filePipeLine,
		seq:         0,
		segmentSize: SegmentSize,
		currentSize: 0,
		buffer:      new(bytes.Buffer),
		commitCh:    make(chan struct{}),
	}
	go WalFile.Save()
	return WalFile, nil
}

func (w *WAL) Save() {
	for {
		walMsg := <-w.walCh

		atomic.AddUint64(&globalSeq, 1)
		dataLen := uint32(len(walMsg.Data))
		crc := crc32.ChecksumIEEE(walMsg.Data)

		// 写入头部
		header := make([]byte, headerSize)
		binary.BigEndian.PutUint64(header[0:8], globalSeq)
		binary.BigEndian.PutUint32(header[8:12], dataLen)
		binary.BigEndian.PutUint32(header[12:16], crc)

		// 写入缓冲区
		if _, err := w.buffer.Write(header); err != nil {
			log.Println("write header fail")
		}
		if _, err := w.buffer.Write(walMsg.Data); err != nil {
			log.Println("write data fail")
		}
	}
}

// 添加新的提交函数
func (w *WAL) Commit() error {
	// 检查是否需要切换文件
	if w.currentSize+int64(w.buffer.Len()) > w.segmentSize {
		if err := w.switchSegment(); err != nil {
			return err
		}
	}

	// 将缓冲区数据写入文件
	if _, err := w.writer.Write(w.buffer.Bytes()); err != nil {
		return err
	}

	w.currentSize += int64(w.buffer.Len())

	// 确保数据写入磁盘
	if err := w.writer.Flush(); err != nil {
		return err
	}

	// 清空缓冲区
	w.buffer.Reset()

	return nil
}

func (w *WAL) Read(walSeq int) ([][]byte, error) {
	var data [][]byte

	files, err := os.ReadDir(w.dir)
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, nil
	}

	// 获取所有 WAL 文件并排序
	var walFiles []string
	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), ".wal") {
			walFiles = append(walFiles, filepath.Join(w.dir, file.Name()))
		}
	}
	sort.Strings(walFiles)

	// 确保写入缓冲区刷新到磁盘
	if err := w.writer.Flush(); err != nil {
		return nil, err
	}

	// 遍历所有 WAL 文件
	for _, walFile := range walFiles {
		readFile, err := os.Open(walFile)
		if err != nil {
			return nil, err
		}
		defer readFile.Close()

		reader := bufio.NewReader(readFile)
		for {
			// 尝试读取头部
			header := make([]byte, headerSize)
			n, err := reader.Read(header)
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
			if n < headerSize {
				break
			}
			if bytes.Equal(header, zeroHeader) {
				break
			}

			// 解析头部
			seq := binary.BigEndian.Uint64(header[0:8])
			dataLen := binary.BigEndian.Uint32(header[8:12])
			expectedCRC := binary.BigEndian.Uint32(header[12:16])

			// 读取数据
			payload := make([]byte, dataLen)
			n, err = reader.Read(payload)
			if err != nil {
				if err == io.EOF {
					break
				}
				return nil, err
			}
			if uint32(n) < dataLen {
				break
			}

			// 验证 CRC
			if crc32.ChecksumIEEE(payload[:n]) != expectedCRC {
				return nil, ErrCRCMismatch
			}

			if int(seq) > walSeq {
				data = append(data, payload[:n])
			}
		}
	}
	return data, nil
}
func (w *WAL) Close() error {
	if err := w.writer.Flush(); err != nil {
		return err
	}
	return w.file.Close()
}

// Remove 删除WAL文件
func (w *WAL) Remove() error {
	w.Close()
	return os.Remove(filepath.Join(w.dir, w.filename))
}

// CleanupWALsBefore 实现WALCleaner接口
func (w *WAL) CleanupWALsBefore() error {

	files, err := os.ReadDir(w.dir)
	if err != nil {
		return err
	}

	// 获取所有 WAL 文件并排序
	var walFiles []string
	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), ".wal") {
			walFiles = append(walFiles, filepath.Join(w.dir, file.Name()))
		}
	}
	sort.Strings(walFiles)
	fileNum := len(walFiles)
	for i := 0; i < fileNum-2; i++ {
		if err := os.Remove(walFiles[i]); err != nil {
			return err
		}
	}

	w.seq = 1

	return nil
}

func (w *WAL) getCurrentWalSeq() int {
	return w.seq
}

func (w *WAL) GetGlobalLogSeq() uint64 {
	return globalSeq
}
