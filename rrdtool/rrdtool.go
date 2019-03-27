package rrdtool

import (
	"errors"
	"log"
	"math"
	"sync/atomic"
	"time"

	cmodel "github.com/open-falcon/common/model"
	"github.com/open-falcon/rrdlite"
	"github.com/toolkits/file"

	"github.com/open-falcon/graph/g"
	"github.com/open-falcon/graph/store"
)

var (
	disk_counter uint64
	net_counter  uint64
)

type fetch_t struct {
	filename string
	cf       string //合并（统计）功能有 AVERAGE、MAX、MIN、LAST 四种
	start    int64
	end      int64
	step     int
	data     []*cmodel.RRDData
}

type flushfile_t struct {
	filename string
	items    []*cmodel.GraphItem
}

// 读取文件的数据机构
type readfile_t struct {
	filename string // 文件路径
	data     []byte // 文件内容
}

func Start() {
	cfg := g.Config()
	var err error
	// 创建数据存储目录，并尝试写入文件，尝试写权限
	// check data dir
	if err = file.EnsureDirRW(cfg.RRD.Storage); err != nil {
		log.Fatalln("rrdtool.Start error, bad data dir "+cfg.RRD.Storage+",", err)
	}

	migrate_start(cfg)

	// sync disk
	go syncDisk()
	go ioWorker()
	log.Println("rrdtool.Start ok")
}

// RRA.Point.Size
const (
	RRA1PointCnt   = 720 // 1m一个点存12h
	RRA5PointCnt   = 576 // 5m一个点存2d
	RRA20PointCnt  = 504 // 20m一个点存7d
	RRA180PointCnt = 766 // 3h一个点存3month
	RRA720PointCnt = 730 // 12h一个点存1year
)

func create(filename string, item *cmodel.GraphItem) error {
	now := time.Now()
	start := now.Add(time.Duration(-24) * time.Hour)
	step := uint(item.Step)

	c := rrdlite.NewCreator(filename, start, step)
	c.DS("metric", item.DsType, item.Heartbeat, item.Min, item.Max)

	// 设置各种归档策略
	// 1分钟一个点存 12小时
	c.RRA("AVERAGE", 0.5, 1, RRA1PointCnt)

	// 5m一个点存2d
	c.RRA("AVERAGE", 0.5, 5, RRA5PointCnt)
	c.RRA("MAX", 0.5, 5, RRA5PointCnt)
	c.RRA("MIN", 0.5, 5, RRA5PointCnt)

	// 20m一个点存7d
	c.RRA("AVERAGE", 0.5, 20, RRA20PointCnt)
	c.RRA("MAX", 0.5, 20, RRA20PointCnt)
	c.RRA("MIN", 0.5, 20, RRA20PointCnt)

	// 3小时一个点存3个月
	c.RRA("AVERAGE", 0.5, 180, RRA180PointCnt)
	c.RRA("MAX", 0.5, 180, RRA180PointCnt)
	c.RRA("MIN", 0.5, 180, RRA180PointCnt)

	// 12小时一个点存1year
	c.RRA("AVERAGE", 0.5, 720, RRA720PointCnt)
	c.RRA("MAX", 0.5, 720, RRA720PointCnt)
	c.RRA("MIN", 0.5, 720, RRA720PointCnt)

	return c.Create(true)
}

// 将GraphItem追加到rrdb
func update(filename string, items []*cmodel.GraphItem) error {
	u := rrdlite.NewUpdater(filename)

	for _, item := range items {
		v := math.Abs(item.Value)
		if v > 1e+300 || (v < 1e-300 && v > 0) {
			continue
		}
		if item.DsType == "DERIVE" || item.DsType == "COUNTER" {
			u.Cache(item.Timestamp, int(item.Value))
		} else {
			u.Cache(item.Timestamp, item.Value)
		}
	}

	return u.Update()
}

// 将GraphItem数据追加到rrdfile
// flush to disk from memory
// 最新的数据在列表的最后面
// TODO fix me, filename fmt from item[0], it's hard to keep consistent
func flushrrd(filename string, items []*cmodel.GraphItem) error {
	if items == nil || len(items) == 0 {
		return errors.New("empty items")
	}

	if !g.IsRrdFileExist(filename) {
		baseDir := file.Dir(filename)

		err := file.InsureDir(baseDir)
		if err != nil {
			return err
		}

		err = create(filename, items[0])
		if err != nil {
			return err
		}
	}

	return update(filename, items)
}

// 发送io_task任务，读取rrdfile
func ReadFile(filename string) ([]byte, error) {
	done := make(chan error, 1)
	task := &io_task_t{
		method: IO_TASK_M_READ,
		args:   &readfile_t{filename: filename},
		done:   done,
	}

	io_task_chan <- task
	err := <-done
	return task.args.(*readfile_t).data, err
}

// 发送io_task任务，写入rrdfile，等待任务完成
func FlushFile(filename string, items []*cmodel.GraphItem) error {
	done := make(chan error, 1)
	io_task_chan <- &io_task_t{
		method: IO_TASK_M_FLUSH,
		args: &flushfile_t{
			filename: filename,
			items:    items,
		},
		done: done,
	}
	atomic.AddUint64(&disk_counter, 1)
	return <-done
}

// 发送io_task任务，读取某时间段的统计数据
func Fetch(filename string, cf string, start, end int64, step int) ([]*cmodel.RRDData, error) {
	done := make(chan error, 1)
	task := &io_task_t{
		method: IO_TASK_M_FETCH,
		args: &fetch_t{
			filename: filename,
			cf:       cf,
			start:    start,
			end:      end,
			step:     step,
		},
		done: done,
	}
	io_task_chan <- task
	err := <-done
	return task.args.(*fetch_t).data, err
}

// 直接从rrdfile读取某时间段的统计数据
func fetch(filename string, cf string, start, end int64, step int) ([]*cmodel.RRDData, error) {
	start_t := time.Unix(start, 0)
	end_t := time.Unix(end, 0)
	step_t := time.Duration(step) * time.Second

	fetchRes, err := rrdlite.Fetch(filename, cf, start_t, end_t, step_t)
	if err != nil {
		return []*cmodel.RRDData{}, err
	}

	defer fetchRes.FreeValues()

	values := fetchRes.Values()
	size := len(values)
	ret := make([]*cmodel.RRDData, size)

	start_ts := fetchRes.Start.Unix()
	step_s := fetchRes.Step.Seconds()

	for i, val := range values {
		ts := start_ts + int64(i+1)*int64(step_s)
		d := &cmodel.RRDData{
			Timestamp: ts,
			Value:     cmodel.JsonFloat(val),
		}
		ret[i] = d
	}

	return ret, nil
}

// 将GraphItems中所有数据落盘
func FlushAll(force bool) {
	n := store.GraphItems.Size / 10
	for i := 0; i < store.GraphItems.Size; i++ {
		FlushRRD(i, force)
		if i%n == 0 {
			// 每处理十分之一GraphItems数据，进行一次log
			log.Printf("flush hash idx:%03d size:%03d disk:%08d net:%08d\n",
				i, store.GraphItems.Size, disk_counter, net_counter)
		}
	}
	log.Printf("flush hash done (disk:%08d net:%08d)\n", disk_counter, net_counter)
}

// 将key对应的所有GraphItem元素写入rrd文件
func CommitByKey(key string) {

	md5, dsType, step, err := g.SplitRrdCacheKey(key)
	if err != nil {
		return
	}
	// 构成rrdfile文件名
	filename := g.RrdFileName(g.Config().RRD.Storage, md5, dsType, step)

	// 取出并清空对应key的所有GraphItem元素
	items := store.GraphItems.PopAll(key)
	if len(items) == 0 {
		return
	}
	// 发送io_task任务，写入rrdfile，等待任务完成
	FlushFile(filename, items)
}

// 对该key的pull任务放入任务队列，实现非阻塞执行pull
func PullByKey(key string) {
	done := make(chan error)

	item := store.GraphItems.First(key)
	if item == nil {
		return
	}
	// 获取保存item的物理node
	node, err := Consistent.Get(item.PrimaryKey())
	if err != nil {
		return
	}
	// 将pull任务放入对应node的net_task队列
	Net_task_ch[node] <- &Net_task_t{
		Method: NET_TASK_M_PULL,
		Key:    key,
		Done:   done,
	}
	// net_task slow, shouldn't block syncDisk() or FlushAll()
	// warning: recev sigout when migrating, maybe lost memory data
	go func() {
		// 开一个协程，等待pull任务的完成
		err := <-done
		if err != nil {
			log.Printf("get %s from remote err[%s]\n", key, err)
			return
		}
		atomic.AddUint64(&net_counter, 1)
		//todo: flushfile after getfile? not yet
	}()
}

// 将GraphItems中idx索引位置的所有数据落盘
func FlushRRD(idx int, force bool) {
	begin := time.Now()
	atomic.StoreInt32(&flushrrd_timeout, 0)

	keys := store.GraphItems.KeysByIndex(idx)
	if len(keys) == 0 {
		return
	}

	for _, key := range keys {
		flag, _ := store.GraphItems.GetFlag(key)

		//write err data to local filename
		if force == false && g.Config().Migrate.Enabled && flag&g.GRAPH_F_MISS != 0 {
			// 如果存在rrd文件
			if time.Since(begin) > time.Millisecond*g.FLUSH_DISK_STEP {
				atomic.StoreInt32(&flushrrd_timeout, 1)
			}
			PullByKey(key)
		} else {
			// 将key对应的所有GraphItem元素写入rrd文件，阻塞式写入
			CommitByKey(key)
		}
	}
}
