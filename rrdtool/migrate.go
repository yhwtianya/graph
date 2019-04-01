package rrdtool

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync/atomic"
	"time"

	pfc "github.com/niean/goperfcounter"
	"github.com/toolkits/consistent"

	cmodel "github.com/open-falcon/common/model"
	cutils "github.com/open-falcon/common/utils"
	"github.com/open-falcon/graph/g"
	"github.com/open-falcon/graph/store"
)

// Net_task_t的Method取值类型
const (
	_ = iota
	NET_TASK_M_SEND
	NET_TASK_M_QUERY
	NET_TASK_M_PULL
)

// Net_task数据机构
type Net_task_t struct {
	Method int
	Key    string
	Done   chan error
	Args   interface{}
	Reply  interface{}
}

// 统计信息计数器类型
const (
	FETCH_S_SUCCESS = iota
	FETCH_S_ERR
	FETCH_S_ISNOTEXIST
	SEND_S_SUCCESS
	SEND_S_ERR
	QUERY_S_SUCCESS
	QUERY_S_ERR
	CONN_S_ERR
	CONN_S_DIAL
	STAT_SIZE
)

var (
	Consistent       *consistent.Consistent      // 一致性哈希
	Net_task_ch      map[string]chan *Net_task_t //保存每个graph的net_task队列，key为graph节点名称
	clients          map[string][]*rpc.Client    // 保存migrate的每个graph的rcpClient，key为graph节点名称
	flushrrd_timeout int32                       //标记flushrrd操作是否超时，超时时间为time.Millisecond*g.FLUSH_DISK_STEP
	stat_cnt         [STAT_SIZE]uint64           // 统计计数器数组
)

func init() {
	Consistent = consistent.New()
	Net_task_ch = make(map[string]chan *Net_task_t)
	clients = make(map[string][]*rpc.Client)
}

// 获取migrate统计计数器信息
func GetCounter() (ret string) {
	return fmt.Sprintf("FETCH_S_SUCCESS[%d] FETCH_S_ERR[%d] FETCH_S_ISNOTEXIST[%d] SEND_S_SUCCESS[%d] SEND_S_ERR[%d] QUERY_S_SUCCESS[%d] QUERY_S_ERR[%d] CONN_S_ERR[%d] CONN_S_DIAL[%d]",
		atomic.LoadUint64(&stat_cnt[FETCH_S_SUCCESS]),
		atomic.LoadUint64(&stat_cnt[FETCH_S_ERR]),
		atomic.LoadUint64(&stat_cnt[FETCH_S_ISNOTEXIST]),
		atomic.LoadUint64(&stat_cnt[SEND_S_SUCCESS]),
		atomic.LoadUint64(&stat_cnt[SEND_S_ERR]),
		atomic.LoadUint64(&stat_cnt[QUERY_S_SUCCESS]),
		atomic.LoadUint64(&stat_cnt[QUERY_S_ERR]),
		atomic.LoadUint64(&stat_cnt[CONN_S_ERR]),
		atomic.LoadUint64(&stat_cnt[CONN_S_DIAL]))
}

// 创建并返回rpcConn
func dial(address string, timeout time.Duration) (*rpc.Client, error) {
	d := net.Dialer{Timeout: timeout}
	conn, err := d.Dial("tcp", address)
	if err != nil {
		return nil, err
	}
	if tc, ok := conn.(*net.TCPConn); ok {
		if err := tc.SetKeepAlive(true); err != nil {
			conn.Close()
			return nil, err
		}
	}
	return rpc.NewClient(conn), err
}

// 启动数据迁移协程，监听Net_task_ch，执行Net_task
func migrate_start(cfg *g.GlobalConfig) {
	var err error
	var i int
	if cfg.Migrate.Enabled {
		Consistent.NumberOfReplicas = cfg.Migrate.Replicas

		// 返回排序后的key
		nodes := cutils.KeysOfMap(cfg.Migrate.Cluster)
		for _, node := range nodes {
			addr := cfg.Migrate.Cluster[node]
			// 加入一致性哈希，用于数据打散后迁移过程中的稳定性
			Consistent.Add(node)
			Net_task_ch[node] = make(chan *Net_task_t, 16)
			// 根据并发配置，每个node创建n个rpcClient
			clients[node] = make([]*rpc.Client, cfg.Migrate.Concurrency)

			for i = 0; i < cfg.Migrate.Concurrency; i++ {
				// 初始化每个node上的n个rpcClient
				if clients[node][i], err = dial(addr, time.Second); err != nil {
					log.Fatalf("node:%s addr:%s err:%s\n", node, addr, err)
				}
				// rpcClient为nil也没事，net_task_worker中有重连机制
				// 对每个node的每个conn，启动task_worker
				go net_task_worker(i, Net_task_ch[node], &clients[node][i], addr)
			}
		}
	}
}

// 监听Net_task_ch，执行Net_task
func net_task_worker(idx int, ch chan *Net_task_t, client **rpc.Client, addr string) {
	var err error
	for {
		select {
		case task := <-ch:
			if task.Method == NET_TASK_M_SEND {
				// 将key对应数据发送到其他graph
				if err = send_data(client, task.Key, addr); err != nil {
					// goperfcounter累积数监控
					pfc.Meter("migrate.send.err", 1)
					// 自身统计
					atomic.AddUint64(&stat_cnt[SEND_S_ERR], 1)
				} else {
					pfc.Meter("migrate.send.ok", 1)
					atomic.AddUint64(&stat_cnt[SEND_S_SUCCESS], 1)
				}
			} else if task.Method == NET_TASK_M_QUERY {
				// 向其他graph执行查询
				if err = query_data(client, addr, task.Args, task.Reply); err != nil {
					pfc.Meter("migrate.query.err", 1)
					atomic.AddUint64(&stat_cnt[QUERY_S_ERR], 1)
				} else {
					pfc.Meter("migrate.query.ok", 1)
					atomic.AddUint64(&stat_cnt[QUERY_S_SUCCESS], 1)
				}
			} else if task.Method == NET_TASK_M_PULL {
				if atomic.LoadInt32(&flushrrd_timeout) != 0 {
					// 落盘超时
					// 发送给其他graph，完成内存数据转移
					// hope this more faster than fetch_rrd
					if err = send_data(client, task.Key, addr); err != nil {
						pfc.Meter("migrate.sendbusy.err", 1)
						atomic.AddUint64(&stat_cnt[SEND_S_ERR], 1)
					} else {
						pfc.Meter("migrate.sendbusy.ok", 1)
						atomic.AddUint64(&stat_cnt[SEND_S_SUCCESS], 1)
					}
				} else {
					// 从目标graph获取数据并写入本地文件，目标graph也可以是本地
					if err = fetch_rrd(client, task.Key, addr); err != nil {
						if os.IsNotExist(err) {
							pfc.Meter("migrate.scprrd.null", 1)
							//文件不存在时，直接将缓存数据刷入本地
							atomic.AddUint64(&stat_cnt[FETCH_S_ISNOTEXIST], 1)
							store.GraphItems.SetFlag(task.Key, 0)
							CommitByKey(task.Key)
						} else {
							pfc.Meter("migrate.scprrd.err", 1)
							//warning:其他异常情况，缓存数据会堆积
							atomic.AddUint64(&stat_cnt[FETCH_S_ERR], 1)
						}
					} else {
						// fetch_rrd失败，无法落盘？
						pfc.Meter("migrate.scprrd.ok", 1)
						atomic.AddUint64(&stat_cnt[FETCH_S_SUCCESS], 1)
					}
				}
			} else {
				err = errors.New("error net task method")
			}
			if task.Done != nil {
				task.Done <- err
			}
		}
	}
}

// rpcClient重连
// TODO addr to node
func reconnection(client **rpc.Client, addr string) {
	pfc.Meter("migrate.reconnection."+addr, 1)

	var err error

	atomic.AddUint64(&stat_cnt[CONN_S_ERR], 1)
	if *client != nil {
		(*client).Close()
	}

	*client, err = dial(addr, time.Second)
	atomic.AddUint64(&stat_cnt[CONN_S_DIAL], 1)

	for err != nil {
		// 无限循环，阻塞式连接
		//danger!! block routine
		time.Sleep(time.Millisecond * 500)
		*client, err = dial(addr, time.Second)
		atomic.AddUint64(&stat_cnt[CONN_S_DIAL], 1)
	}
}

// 向其他graph执行查询
func query_data(client **rpc.Client, addr string,
	args interface{}, resp interface{}) error {
	var (
		err error
		i   int
	)

	for i = 0; i < 3; i++ {
		err = rpc_call(*client, "Graph.Query", args, resp,
			time.Duration(g.Config().CallTimeout)*time.Millisecond)

		if err == nil {
			break
		}
		if err == rpc.ErrShutdown {
			reconnection(client, addr)
		}
	}
	return err
}

// 将key对应数据发送给对应graph
func send_data(client **rpc.Client, key string, addr string) error {
	var (
		err  error
		flag uint32
		resp *cmodel.SimpleRpcResponse
		i    int
	)

	//remote
	if flag, err = store.GraphItems.GetFlag(key); err != nil {
		return err
	}
	cfg := g.Config()

	// 追加GRAPH_F_SENDING标记
	store.GraphItems.SetFlag(key, flag|g.GRAPH_F_SENDING)

	// 取出数据列表，并在GraphItems中删除该列表
	items := store.GraphItems.PopAll(key)
	items_size := len(items)
	if items_size == 0 {
		goto out
	}
	resp = &cmodel.SimpleRpcResponse{}

	for i = 0; i < 3; i++ {
		// 将数据发送给对应graph
		err = rpc_call(*client, "Graph.Send", items, resp,
			time.Duration(cfg.CallTimeout)*time.Millisecond)

		if err == nil {
			goto out
		}
		if err == rpc.ErrShutdown {
			reconnection(client, addr)
		}
	}

	// 发送失败，在内存中恢复该key的数据列表，此时没有flag
	// err
	store.GraphItems.PushAll(key, items)
	//flag |= g.GRAPH_F_ERR
out:

	// 清除GRAPH_F_SENDING标记，将对应key恢复原标记
	flag &= ^g.GRAPH_F_SENDING
	store.GraphItems.SetFlag(key, flag)
	return err

}

// 从目标graph获取数据并写入本地文件
func fetch_rrd(client **rpc.Client, key string, addr string) error {
	var (
		err      error
		flag     uint32
		md5      string
		dsType   string
		filename string
		step, i  int
		rrdfile  g.File
	)

	cfg := g.Config()

	if flag, err = store.GraphItems.GetFlag(key); err != nil {
		return err
	}

	// 追加GRAPH_F_FETCHING标记
	store.GraphItems.SetFlag(key, flag|g.GRAPH_F_FETCHING)

	// 由rrd的key解析出md5、dsType、step
	md5, dsType, step, _ = g.SplitRrdCacheKey(key)
	filename = g.RrdFileName(cfg.RRD.Storage, md5, dsType, step)

	for i = 0; i < 3; i++ {
		err = rpc_call(*client, "Graph.GetRrd", key, &rrdfile,
			time.Duration(cfg.CallTimeout)*time.Millisecond)

		if err == nil {
			// 生成io_task加入io_task_chan队列
			done := make(chan error, 1)
			io_task_chan <- &io_task_t{
				method: IO_TASK_M_WRITE,
				args: &g.File{
					Filename: filename,
					Body:     rrdfile.Body[:],
				},
				done: done,
			}

			// 阻塞式等待io_task执行完成
			if err = <-done; err != nil {
				goto out
			} else {
				// 任务执行成功，清除GRAPH_F_MISS标记
				flag &= ^g.GRAPH_F_MISS
				goto out
			}
		} else {
			log.Println(err)
		}
		if err == rpc.ErrShutdown {
			reconnection(client, addr)
		}
	}
out:
	// 清除GRAPH_F_FETCHING标记，将对应key恢复原标记
	flag &= ^g.GRAPH_F_FETCHING
	store.GraphItems.SetFlag(key, flag)
	return err
}

// 调用rpc method
func rpc_call(client *rpc.Client, method string, args interface{},
	reply interface{}, timeout time.Duration) error {
	done := make(chan *rpc.Call, 1)
	client.Go(method, args, reply, done)
	select {
	case <-time.After(timeout):
		return errors.New("i/o timeout[rpc]")
	case call := <-done:
		if call.Error == nil {
			return nil
		} else {
			return call.Error
		}
	}
}
