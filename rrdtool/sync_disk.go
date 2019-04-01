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
	_ = iota
	IO_TASK_M_READ
	IO_TASK_M_WRITE
	IO_TASK_M_FLUSH
	IO_TASK_M_FETCH
)

type io_task_t struct {
	method int
	args   interface{}
	done   chan error
}

var (
	Out_done_chan chan int        // 接收退出信号
	io_task_chan  chan *io_task_t // io_task任务队列
)

func init() {
	Out_done_chan = make(chan int, 1)
	io_task_chan = make(chan *io_task_t, 16)
}

// 定期FlushRRD，进行落盘
func syncDisk() {
	// 启动后不立即进行落盘
	time.Sleep(time.Second * 300)
	ticker := time.NewTicker(time.Millisecond * g.FLUSH_DISK_STEP).C
	var idx int = 0

	for {
		select {
		case <-ticker:
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
				// 执行读文件任务
				if args, ok := task.args.(*readfile_t); ok {
					args.data, err = ioutil.ReadFile(args.filename)
					task.done <- err
				}
			} else if task.method == IO_TASK_M_WRITE {
				// 执行写文件任务
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
					// 将GraphItem数据追加到rrdfile
					task.done <- flushrrd(args.filename, args.items)
				}
			} else if task.method == IO_TASK_M_FETCH {
				if args, ok := task.args.(*fetch_t); ok {
					// 直接从rrdfile读取某时间段的统计数据
					args.data, err = fetch(args.filename, args.cf, args.start, args.end, args.step)
					task.done <- err
				}
			}
		}
	}
}
