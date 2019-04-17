package rrdtool

import (
	"io"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/open-falcon/graph/g"
	"github.com/open-falcon/graph/store"
	"github.com/toolkits/file"
)

const (
	_               = iota
	IO_TASK_M_READ  //读取整个rrdfile
	IO_TASK_M_WRITE //写入新rrdfile
	IO_TASK_M_FLUSH //追加到存在的rrdfile
	IO_TASK_M_FETCH //读取rrdfile一段时间的统计数据
)

type io_task_t struct {
	method int
	args   interface{}
	done   chan error
}

var (
	Out_done_chan chan int // 接收退出信号
	// io_task任务队列，任务是对本地rrdfile进行相应的读写操作
	// migrate.go的fetch_rrd请求IO_TASK_M_WRITE，migrate_start调用net_task_worker，net_task_worker调用fetch_rrd
	// rrdtool.go的ReadFile请求IO_TASK_M_READ、FlushFile请求IO_TASK_M_FLUSH、Fetch请求IO_TASK_M_FETCH
	io_task_chan chan *io_task_t
)

func init() {
	Out_done_chan = make(chan int, 1)
	io_task_chan = make(chan *io_task_t, 16)
}

// 定期FlushRRD，将store.GraphItems进行落盘
func syncDisk() {
	// 启动后不立即进行落盘
	time.Sleep(time.Second * 300)
	ticker := time.NewTicker(time.Millisecond * g.FLUSH_DISK_STEP).C
	var idx int = 0

	for {
		select {
		case <-ticker:
			// 每次仅对一个index的数据进行落盘
			idx = idx % store.GraphItems.Size
			FlushRRD(idx, false)
			idx += 1
		case <-Out_done_chan:
			log.Println("cron recv sigout and exit...")
			return
		}
	}
}

// 写文件
// WriteFile writes data to a file named by filename.
// file must not exist
func writeFile(filename string, data []byte, perm os.FileMode) error {
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_EXCL, perm)
	if err != nil {
		return err
	}
	n, err := f.Write(data)
	if err == nil && n < len(data) {
		err = io.ErrShortWrite
	}
	if err1 := f.Close(); err == nil {
		err = err1
	}
	return err
}

//读取io_task_chan队列任务，执行io_task
func ioWorker() {
	var err error
	for {
		select {
		case task := <-io_task_chan:
			if task.method == IO_TASK_M_READ {
				// 执行读整个rrdfile
				if args, ok := task.args.(*readfile_t); ok {
					args.data, err = ioutil.ReadFile(args.filename)
					task.done <- err
				}
			} else if task.method == IO_TASK_M_WRITE {
				// 执行写全新rrdfile任务
				//filename must not exist
				if args, ok := task.args.(*g.File); ok {
					baseDir := file.Dir(args.Filename)
					if err = file.InsureDir(baseDir); err != nil {
						task.done <- err
					}
					task.done <- writeFile(args.Filename, args.Body, 0644)
				}
			} else if task.method == IO_TASK_M_FLUSH {
				if args, ok := task.args.(*flushfile_t); ok {
					// 将GraphItem数据追加到存在的rrdfile
					task.done <- flushrrd(args.filename, args.items)
				}
			} else if task.method == IO_TASK_M_FETCH {
				if args, ok := task.args.(*fetch_t); ok {
					// 直接从rrdfile读取某时间段的统计数据,旧数据在头部
					args.data, err = fetch(args.filename, args.cf, args.start, args.end, args.step)
					task.done <- err
				}
			}
		}
	}
}
