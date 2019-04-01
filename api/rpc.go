package api

import (
	"container/list"
	"log"
	"net"
	"net/rpc"
	"sync"
	"time"

	"github.com/open-falcon/graph/g"
)

// 加锁的列表
type conn_list struct {
	sync.RWMutex
	list *list.List
}

func (l *conn_list) insert(c net.Conn) *list.Element {
	l.Lock()
	defer l.Unlock()
	return l.list.PushBack(c)
}
func (l *conn_list) remove(e *list.Element) net.Conn {
	l.Lock()
	defer l.Unlock()
	return l.list.Remove(e).(net.Conn)
}

var Close_chan, Close_done_chan chan int // rpc退出通道，优雅退出响应通道
var connects conn_list                   //保存rpc accept的连接，用于退出时依次关闭rpc连接

func init() {
	Close_chan = make(chan int, 1)
	Close_done_chan = make(chan int, 1)
	connects = conn_list{list: list.New()}
}

// 启动rpc服务
func Start() {
	if !g.Config().Rpc.Enabled {
		log.Println("rpc.Start warning, not enabled")
		return
	}
	addr := g.Config().Rpc.Listen
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		log.Fatalf("rpc.Start error, net.ResolveTCPAddr failed, %s", err)
	}

	listener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		log.Fatalf("rpc.Start error, listen %s failed, %s", addr, err)
	} else {
		log.Println("rpc.Start ok, listening on", addr)
	}

	rpc.Register(new(Graph))

	go func() {
		var tempDelay time.Duration // how long to sleep on accept failure
		for {
			conn, err := listener.Accept()
			// 失败时翻倍递增sleep时长，最大1s
			if err != nil {
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}
				if max := 1 * time.Second; tempDelay > max {
					tempDelay = max
				}
				time.Sleep(tempDelay)
				continue
			}
			tempDelay = 0
			go func() {
				e := connects.insert(conn)
				defer connects.remove(e)
				rpc.ServeConn(conn)
			}()
		}
	}()

	select {
	case <-Close_chan:
		// 等待退出信号，优雅退出
		log.Println("rpc, recv sigout and exiting...")
		listener.Close()
		Close_done_chan <- 1

		connects.Lock()
		for e := connects.list.Front(); e != nil; e = e.Next() {
			e.Value.(net.Conn).Close()
		}
		connects.Unlock()

		return
	}

}
