package pool

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"
)

const (
	maxSize = 10
	maxOpen = 5
)

// net.Conn Pool
type Pool struct {
	m            sync.Mutex                  // 保证多个 goroutine 访问时，closed 的线程安全
	res          chan net.Conn               // 存储连接的 channel
	factory      func() (net.Conn, error)    // 用于创建新连接的工厂方法
	closed       bool                        // 标记连接池是否已关闭
	maxOpen      int                         // 最大打开连接数
	numOpen      int                         // 当前打开连接数
	connRequests map[uint64]chan connRequest // 用来管理排队请求
}

type connRequest struct {
	conn net.Conn
	err  error
}

// 创建一个新的连接池
func NewPool(factory func() (net.Conn, error)) *Pool {
	return &Pool{
		res:          make(chan net.Conn, maxSize), // 最大连接数
		factory:      factory,
		closed:       false,
		maxOpen:      maxOpen,
		connRequests: make(map[uint64]chan connRequest),
	}
}

// 从连接池获取一个连接
func (p *Pool) Get(ctx context.Context) (net.Conn, error) {
	p.m.Lock()

	if p.closed {
		return nil, fmt.Errorf("connection pool is closed")
	}

	// 如果连接池中有空闲连接，直接返回
	select {
	case conn := <-p.res:
		p.m.Unlock()
		return conn, nil
	default:
		// 若没有空闲连接且已占用连接数大于等于最大占用数
		// 则加入等待队列等待
		if p.maxOpen > 0 && p.numOpen >= p.maxOpen {
			// 如果连接数达到了最大连接数，加入排队请求
			req := make(chan connRequest, 1)
			reqKey := uint64(time.Now().UnixNano())
			p.connRequests[reqKey] = req

			p.m.Unlock()
			// 等待连接
			select {
			case <-ctx.Done():
				// 取消时，从排队队列中删除请求
				p.m.Lock()
				delete(p.connRequests, reqKey)
				p.m.Unlock()
				return nil, ctx.Err()
			case ret := <-req:
				if ret.err != nil {
					return nil, ret.err
				}
				return ret.conn, nil
			}
		}
		// 如果没有达到最大连接数，可以创建新的连接
		p.numOpen++

		conn, err := p.factory()
		if err != nil {
			p.m.Lock()
			p.numOpen--
			p.m.Unlock()
			return nil, err
		}
		p.m.Unlock()
		return conn, nil
	}
}

// 将连接放回连接池
func (p *Pool) Put(conn net.Conn) error {
	p.m.Lock()
	defer p.m.Unlock()

	if p.closed {
		return fmt.Errorf("connection pool is closed")
	}

	// 如果有等待的请求，直接返回给排队中的请求
	if len(p.connRequests) > 0 {
		for reqKey, req := range p.connRequests {
			delete(p.connRequests, reqKey)
			req <- connRequest{conn: conn}
			return nil
		}
	}

	p.res <- conn
	return nil
}

// 关闭连接池，关闭所有连接
func (p *Pool) Close() error {
	p.m.Lock()
	defer p.m.Unlock()

	if p.closed {
		return nil
	}

	p.closed = true
	for len(p.res) > 0 {
		select {
		case conn := <-p.res:
			conn.Close()
		default:
			break
		}
	}

	// 清理排队的请求
	for _, req := range p.connRequests {
		req <- connRequest{err: fmt.Errorf("connection pool is closed")}
	}

	return nil
}

// 连接方法
func NewTCPConnection(host, port string) (net.Conn, error) {
	var conn net.Conn
	var err error
	maxRetries := 5

	// 尝试连接 maxRetries 次
	for i := 0; i < maxRetries; i++ {
		conn, err = net.DialTimeout("tcp", host+":"+port, 5*time.Second)
		if err == nil {
			// 连接成功，返回连接
			return conn, nil
		}
		// 暂停3s再尝试连接
		time.Sleep(3 * time.Second)
	}

	// 如果所有重试都失败，返回最后的错误
	return nil, fmt.Errorf("failed to connect to %s:%s after %d attempts: %v", host, port, maxRetries, err)
}
