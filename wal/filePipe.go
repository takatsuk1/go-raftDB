package wal

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

var FilePipe *filePipeline

type filePipeline struct {
	mu     sync.Mutex
	fileC  chan *os.File // 无缓冲channel，用于传递预分配的文件
	size   int64         // 预分配大小
	done   chan struct{} // 用于关闭pipeline
	closed bool
}

func NewFilePipeline(path string, size int64) *filePipeline {
	FilePipe = &filePipeline{
		fileC:  make(chan *os.File), // 无缓冲channel
		size:   size,
		done:   make(chan struct{}),
		closed: false,
	}

	// 启动预分配循环
	go FilePipe.allocLoop(path)
	return FilePipe
}

func (fp *filePipeline) allocLoop(basePath string) {
	seq := uint64(0)
	for {
		select {
		case <-fp.done:
			return
		default:
			// 生成新文件名
			timestamp := time.Now().UnixNano()
			path := fmt.Sprintf("%d.wal", timestamp)
			seq++

			// 创建并预分配文件
			f, err := fp.allocateFile(basePath + "/" + path)
			if err != nil {
				continue
			}

			// 发送文件到channel (阻塞直到被消费)
			select {
			case fp.fileC <- f:
				// 文件被消费，继续分配下一个
			case <-fp.done:
				f.Close()
				return
			}
		}
	}
}

func (fp *filePipeline) allocateFile(path string) (*os.File, error) {
	// 创建文件
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return nil, err
	}

	// 预分配固定大小空间
	zeros := make([]byte, fp.size)
	if _, err = f.Write(zeros); err != nil {
		f.Close()
		return nil, err
	}

	// 将文件指针移回开始位置
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		f.Close()
		return nil, err
	}

	return f, nil
}

func (fp *filePipeline) File() (*os.File, error) {
	select {
	case f := <-fp.fileC:
		return f, nil
	case <-fp.done:
		return nil, errors.New("pipeline closed")
	}
}

func (fp *filePipeline) Close() {
	fp.mu.Lock()
	if !fp.closed {
		close(fp.done)
		fp.closed = true
	}
	fp.mu.Unlock()
}
