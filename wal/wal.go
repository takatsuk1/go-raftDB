package wal

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"go-raft/config"
	"go-raft/raft"
	"go-raft/types"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
)

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
				return nil, types.ErrCRCMismatch
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
