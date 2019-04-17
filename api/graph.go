package api

import (
	"fmt"
	"math"
	"time"

	cmodel "github.com/open-falcon/common/model"
	cutils "github.com/open-falcon/common/utils"
	"github.com/open-falcon/graph/g"
	"github.com/open-falcon/graph/index"
	"github.com/open-falcon/graph/proc"
	"github.com/open-falcon/graph/rrdtool"
	"github.com/open-falcon/graph/store"
)

type Graph int

// 将内存数据写入rrdfile,返回最新rrdfile的内容
func (this *Graph) GetRrd(key string, rrdfile *g.File) (err error) {
	if md5, dsType, step, err := g.SplitRrdCacheKey(key); err != nil {
		return err
	} else {
		rrdfile.Filename = g.RrdFileName(g.Config().RRD.Storage, md5, dsType, step)
	}

	// 取出并清空对应key的所有GraphItem元素
	items := store.GraphItems.PopAll(key)
	if len(items) > 0 {
		// 将GraphItem数据追加到本地rrdfile
		rrdtool.FlushFile(rrdfile.Filename, items)
	}

	rrdfile.Body, err = rrdtool.ReadFile(rrdfile.Filename)
	return
}

// 连通性
func (this *Graph) Ping(req cmodel.NullRpcRequest, resp *cmodel.SimpleRpcResponse) error {
	return nil
}

// 接收Transfer发送来的数据，新数据插入到缓存列表头部
func (this *Graph) Send(items []*cmodel.GraphItem, resp *cmodel.SimpleRpcResponse) error {
	go handleItems(items)
	return nil
}

// 供外部调用、处理接收到的数据 的接口
func HandleItems(items []*cmodel.GraphItem) error {
	handleItems(items)
	return nil
}

func handleItems(items []*cmodel.GraphItem) {
	if items == nil {
		return
	}

	count := len(items)
	if count == 0 {
		return
	}

	cfg := g.Config()

	for i := 0; i < count; i++ {
		if items[i] == nil {
			continue
		}
		dsType := items[i].DsType
		step := items[i].Step
		checksum := items[i].Checksum()
		key := g.FormRrdCacheKey(checksum, dsType, step)

		//statistics
		proc.GraphRpcRecvCnt.Incr()

		// To Graph
		first := store.GraphItems.First(key)
		if first != nil && items[i].Timestamp <= first.Timestamp {
			// 检查数据时间
			continue
		}

		// 将数据插入key对应list,如果不存在key对应的rrdfile则置GRAPH_F_MISS flag
		// 周期
		store.GraphItems.PushFront(key, items[i], checksum, cfg)

		// 更新缓存数据，方便快速获取item的最新属性
		// index定期将最新endpoint、count信息同步到数据库
		// To Index
		index.ReceiveItem(items[i], checksum)

		// 每个监控项仅缓存最新三条数据
		// To History
		store.AddItem(checksum, items[i])
	}
}

// 获取一段时间的统计数据,如果本地有rrdfile则从本地读取，否则采用一致性哈希从其他graph获取
func (this *Graph) Query(param cmodel.GraphQueryParam, resp *cmodel.GraphQueryResponse) error {
	var (
		datas      []*cmodel.RRDData
		datas_size int
	)

	// statistics
	proc.GraphQueryCnt.Incr()

	cfg := g.Config()

	// form empty response
	resp.Values = []*cmodel.RRDData{}
	resp.Endpoint = param.Endpoint
	resp.Counter = param.Counter
	dsType, step, exists := index.GetTypeAndStep(param.Endpoint, param.Counter) // complete dsType and step
	if !exists {
		// 数据库不存在此count
		return nil
	}
	resp.DsType = dsType
	resp.Step = step

	// 截取到step的整数倍
	start_ts := param.Start - param.Start%int64(step)
	end_ts := param.End - param.End%int64(step) + int64(step)
	if end_ts-start_ts-int64(step) < 1 {
		return nil
	}

	md5 := cutils.Md5(param.Endpoint + "/" + param.Counter)
	key := g.FormRrdCacheKey(md5, dsType, step)
	filename := g.RrdFileName(cfg.RRD.Storage, md5, dsType, step)

	// 先读取内存数据,获取flag,最新数据在尾部
	// read cached items
	items, flag := store.GraphItems.FetchAll(key)
	items_size := len(items)

	// 然后读取本地rrdfile或graph正在扩容且无本地文件则读取远程rrdfile数据
	if cfg.Migrate.Enabled && flag&g.GRAPH_F_MISS != 0 {
		// 本地没有rrdfile，发送一个网络请求任务，
		node, _ := rrdtool.Consistent.Get(param.Endpoint + "/" + param.Counter)
		done := make(chan error, 1)
		res := &cmodel.GraphAccurateQueryResponse{}
		rrdtool.Net_task_ch[node] <- &rrdtool.Net_task_t{
			Method: rrdtool.NET_TASK_M_QUERY,
			Done:   done,
			Args:   param,
			Reply:  res,
		}
		<-done
		// fetch data from remote
		datas = res.Values
		datas_size = len(datas)
	} else {
		// 直接从本地rrd文件获取
		// read data from rrd file
		datas, _ = rrdtool.Fetch(filename, param.ConsolFun, start_ts, end_ts, step)
		datas_size = len(datas)
	}

	nowTs := time.Now().Unix()
	// 截取到step的整数倍
	lastUpTs := nowTs - nowTs%int64(step)
	// rrdfile第一个点的时间
	rra1StartTs := lastUpTs - int64(rrdtool.RRA1PointCnt*step)

	// 请求开始时间比rrd最早记录还早，直接返回rrd数据
	// consolidated, do not merge
	if start_ts < rra1StartTs {
		resp.Values = datas
		goto _RETURN_OK
	}

	// 内存中没有缓存，不合并直接返回
	// no cached items, do not merge
	if items_size < 1 {
		resp.Values = datas
		goto _RETURN_OK
	}

	// merge
	{
		// fmt cached items
		var val cmodel.JsonFloat
		cache := make([]*cmodel.RRDData, 0)

		// 内存中最旧数据的时间
		ts := items[0].Timestamp
		// 内存中最新数据的时间
		itemEndTs := items[items_size-1].Timestamp
		// 先取最旧数据
		itemIdx := 0
		// 按请求时间和数据类型拼装内存中的数据
		if dsType == g.DERIVE || dsType == g.COUNTER {
			for ts < itemEndTs {
				if itemIdx < items_size-1 && ts == items[itemIdx].Timestamp &&
					ts == items[itemIdx+1].Timestamp-int64(step) {
					val = cmodel.JsonFloat(items[itemIdx+1].Value-items[itemIdx].Value) / cmodel.JsonFloat(step)
					if val < 0 {
						// 无效值NaN
						val = cmodel.JsonFloat(math.NaN())
					}
					itemIdx++
				} else {
					// missing
					val = cmodel.JsonFloat(math.NaN())
				}

				// 提取store.GraphItems中该时间范围的数据到cache,旧数据在头部
				if ts >= start_ts && ts <= end_ts {
					cache = append(cache, &cmodel.RRDData{Timestamp: ts, Value: val})
				}
				ts = ts + int64(step)
			}
		} else if dsType == g.GAUGE {
			for ts <= itemEndTs {
				if itemIdx < items_size && ts == items[itemIdx].Timestamp {
					val = cmodel.JsonFloat(items[itemIdx].Value)
					itemIdx++
				} else {
					// missing
					val = cmodel.JsonFloat(math.NaN())
				}

				if ts >= start_ts && ts <= end_ts {
					cache = append(cache, &cmodel.RRDData{Timestamp: ts, Value: val})
				}
				ts = ts + int64(step)
			}
		}
		cache_size := len(cache)

		// 合并rrdfile和内存中的拼装数据

		// 提取rrdfile中该时间范围的数据到merged,旧数据在头部
		// do merging
		merged := make([]*cmodel.RRDData, 0)
		if datas_size > 0 {
			for _, val := range datas {
				if val.Timestamp >= start_ts && val.Timestamp <= end_ts {
					merged = append(merged, val) //rrdtool返回的数据,时间戳是连续的、不会有跳点的情况
				}
			}
		}

		if cache_size > 0 {
			rrdDataSize := len(merged)
			// lastTs记录merged中拼接点的时间戳,即内存中最旧数据的上一条数据在rrdfile中的时间戳
			// cache的拼接点是索引0的位置
			// merged的拼接点不一定能够和cache[0]正好拼接,有重合的部分,为
			lastTs := cache[0].Timestamp

			// 反向遍历merged,通过cache最旧数据时间，找拼接点
			// find junction
			rrdDataIdx := 0 //记录merged遍历的位置
			for rrdDataIdx = rrdDataSize - 1; rrdDataIdx >= 0; rrdDataIdx-- {
				if merged[rrdDataIdx].Timestamp < cache[0].Timestamp {
					// rrdfile和cache可能有时间上为交集的数据,否则理论上第一条就满足
					lastTs = merged[rrdDataIdx].Timestamp
					break
				}
			}

			// rrdfile和cache时间上为交集的数据,置为空值,rrdfile和cache如果时间上有间隔,间隔数据置为空值
			// fix missing
			for ts := lastTs + int64(step); ts < cache[0].Timestamp; ts += int64(step) {
				merged = append(merged, &cmodel.RRDData{Timestamp: ts, Value: cmodel.JsonFloat(math.NaN())})
			}

			// 从merged拼接点开始,都使用cache里的值
			// merge cached items to result
			rrdDataIdx += 1
			for cacheIdx := 0; cacheIdx < cache_size; cacheIdx++ {
				if rrdDataIdx < rrdDataSize {
					//rrdfile和cache时间上为交集的数据,使用cache里的数据
					if !math.IsNaN(float64(cache[cacheIdx].Value)) {
						merged[rrdDataIdx] = cache[cacheIdx]
					}
				} else {
					merged = append(merged, cache[cacheIdx])
				}
				rrdDataIdx++
			}
		}
		mergedSize := len(merged)

		// fmt result
		ret_size := int((end_ts - start_ts) / int64(step))
		ret := make([]*cmodel.RRDData, ret_size, ret_size)
		mergedIdx := 0
		ts = start_ts
		for i := 0; i < ret_size; i++ {
			if mergedIdx < mergedSize && ts == merged[mergedIdx].Timestamp {
				ret[i] = merged[mergedIdx]
				mergedIdx++
			} else {
				// 如果拼接的数据对应时间点没有数据,则使用空值
				ret[i] = &cmodel.RRDData{Timestamp: ts, Value: cmodel.JsonFloat(math.NaN())}
			}
			ts += int64(step)
		}
		resp.Values = ret
	}

_RETURN_OK:
	// statistics
	proc.GraphQueryItemCnt.IncrBy(int64(len(resp.Values)))
	return nil
}

// 根据endpoint、counter查询cf,step和rrdfile路径
func (this *Graph) Info(param cmodel.GraphInfoParam, resp *cmodel.GraphInfoResp) error {
	// statistics
	proc.GraphInfoCnt.Incr()

	dsType, step, exists := index.GetTypeAndStep(param.Endpoint, param.Counter)
	if !exists {
		return nil
	}

	md5 := cutils.Md5(param.Endpoint + "/" + param.Counter)
	filename := fmt.Sprintf("%s/%s/%s_%s_%d.rrd", g.Config().RRD.Storage, md5[0:2], md5, dsType, step)

	resp.ConsolFun = dsType
	resp.Step = step
	resp.Filename = filename

	return nil
}

// 根据endpoint, counter,获取最新一个数据
func (this *Graph) Last(param cmodel.GraphLastParam, resp *cmodel.GraphLastResp) error {
	// statistics
	proc.GraphLastCnt.Incr()

	resp.Endpoint = param.Endpoint
	resp.Counter = param.Counter
	resp.Value = GetLast(param.Endpoint, param.Counter)

	return nil
}

// 根据endpoint, counter,获取最新一个数据
func (this *Graph) LastRaw(param cmodel.GraphLastParam, resp *cmodel.GraphLastResp) error {
	// statistics
	proc.GraphLastRawCnt.Incr()

	resp.Endpoint = param.Endpoint
	resp.Counter = param.Counter
	resp.Value = GetLastRaw(param.Endpoint, param.Counter)

	return nil
}

// 根据endpoint, counter,获取最新一个数据
// 非法值: ts=0,value无意义
func GetLast(endpoint, counter string) *cmodel.RRDData {
	dsType, step, exists := index.GetTypeAndStep(endpoint, counter)
	if !exists {
		return cmodel.NewRRDData(0, 0.0)
	}

	if dsType == g.GAUGE {
		return GetLastRaw(endpoint, counter)
	}

	if dsType == g.COUNTER || dsType == g.DERIVE {
		md5 := cutils.Md5(endpoint + "/" + counter)
		items := store.GetAllItems(md5)
		if len(items) < 2 {
			return cmodel.NewRRDData(0, 0.0)
		}

		f0 := items[0]
		f1 := items[1]
		delta_ts := f0.Timestamp - f1.Timestamp
		delta_v := f0.Value - f1.Value
		if delta_ts != int64(step) || delta_ts <= 0 {
			return cmodel.NewRRDData(0, 0.0)
		}
		if delta_v < 0 {
			// when cnt restarted, new cnt value would be zero, so fix it here
			delta_v = 0
		}

		return cmodel.NewRRDData(f0.Timestamp, delta_v/float64(delta_ts))
	}

	return cmodel.NewRRDData(0, 0.0)
}

// 根据endpoint, counter,获取最新一个数据
// 非法值: ts=0,value无意义
func GetLastRaw(endpoint, counter string) *cmodel.RRDData {
	md5 := cutils.Md5(endpoint + "/" + counter)
	item := store.GetLastItem(md5)
	return cmodel.NewRRDData(item.Timestamp, item.Value)
}
