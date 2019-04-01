package index

import (
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	tcache "github.com/toolkits/cache/localcache/timedcache"

	cmodel "github.com/open-falcon/common/model"
	cutils "github.com/open-falcon/common/utils"
	"github.com/open-falcon/graph/g"
	"github.com/open-falcon/graph/proc"
)

const (
	DefaultMaxCacheSize                     = 5000000 // 默认 最多500w个,太大了内存会耗尽
	DefaultCacheProcUpdateTaskSleepInterval = time.Duration(1) * time.Second
)

// item缓存
var (
	// key:md5(endpoint/counter), value:IndexCacheItem每个key只保存一个数据
	indexedItemCache = NewIndexCacheBase(DefaultMaxCacheSize)

	// 增量更新队列
	// 如果item没有在indexedItemCache中且没有rrd文件，则将该item保存到unIndexedItemCache
	// 如果item在indexedItemCache中，但item更新了dsType+step，则将该item保存到unIndexedItemCache
	unIndexedItemCache = NewIndexCacheBase(DefaultMaxCacheSize)
)

// db本地缓存
var (
	// 过期时间为600s，每60s进行一次自动清理
	// endpoint表的内存缓存, key:endpoint(string) / value:id(int64)
	dbEndpointCache = tcache.New(600*time.Second, 60*time.Second)
	// endpoint_counter表的内存缓存, key:endpoint_id-counter(string) / val:dstype-step(string)
	dbEndpointCounterCache = tcache.New(600*time.Second, 60*time.Second)
)

// 初始化cache
func InitCache() {
	// 周期性更新 cache的统计计数器信息
	go startCacheProcUpdateTask()
}

// 根据endpoint、counter查询dsType和step
// USED WHEN QUERY
func GetTypeAndStep(endpoint string, counter string) (dsType string, step int, found bool) {
	// get it from index cache
	pk := cutils.Md5(fmt.Sprintf("%s/%s", endpoint, counter))
	// 优先从数据项缓存获取
	if icitem := indexedItemCache.Get(pk); icitem != nil {
		if item := icitem.(*IndexCacheItem).Item; item != nil {
			dsType = item.DsType
			step = item.Step
			found = true
			return
		}
	}

	// statistics
	proc.GraphLoadDbCnt.Incr()

	// 然后从数据库表缓存获取
	// get it from db, this should rarely happen
	var endpointId int64 = -1
	if endpointId, found = GetEndpointFromCache(endpoint); found {
		if dsType, step, found = GetCounterFromCache(endpointId, counter); found {
			//found = true
			return
		}
	}

	// do not find it, this must be a bad request
	found = false
	return
}

// 根据endpoint获取endpint的数据库id
// Return EndpointId if Found
func GetEndpointFromCache(endpoint string) (int64, bool) {
	// 先从缓存查找
	// get from cache
	endpointId, found := dbEndpointCache.Get(endpoint)
	if found {
		return endpointId.(int64), true
	}

	// 从数据查询
	// get from db
	var id int64 = -1
	err := g.DB.QueryRow("SELECT id FROM endpoint WHERE endpoint = ?", endpoint).Scan(&id)
	if err != nil && err != sql.ErrNoRows {
		log.Println("query endpoint id fail,", err)
		return -1, false
	}

	if err == sql.ErrNoRows || id < 0 {
		return -1, false
	}

	// 更新缓存
	// update cache
	dbEndpointCache.Set(endpoint, id, 0)

	return id, true
}

// 查询监测指标的rrd数据类型，上报间隔
// Return DsType Step if Found
func GetCounterFromCache(endpointId int64, counter string) (dsType string, step int, found bool) {
	var err error
	// get from cache
	key := fmt.Sprintf("%d-%s", endpointId, counter)
	// 先从缓存中查找
	dsTypeStep, found := dbEndpointCounterCache.Get(key)
	if found {
		arr := strings.Split(dsTypeStep.(string), "_")
		step, err = strconv.Atoi(arr[1])
		if err != nil {
			found = false
			return
		}
		dsType = arr[0]
		return
	}

	// 从数据库查找
	// get from db
	err = g.DB.QueryRow("SELECT type, step FROM endpoint_counter WHERE endpoint_id = ? and counter = ?",
		endpointId, counter).Scan(&dsType, &step)
	if err != nil && err != sql.ErrNoRows {
		log.Println("query type and step fail", err)
		return
	}

	if err == sql.ErrNoRows {
		return
	}

	// 更新到缓存
	// update cache
	dbEndpointCounterCache.Set(key, fmt.Sprintf("%s_%d", dsType, step), 0)

	found = true
	return
}

// 周期性更新 cache的统计计数器信息
func startCacheProcUpdateTask() {
	for {
		time.Sleep(DefaultCacheProcUpdateTaskSleepInterval)
		proc.IndexedItemCacheCnt.SetCnt(int64(indexedItemCache.Size()))
		proc.UnIndexedItemCacheCnt.SetCnt(int64(unIndexedItemCache.Size()))
		proc.EndpointCacheCnt.SetCnt(int64(dbEndpointCache.Size()))
		proc.CounterCacheCnt.SetCnt(int64(dbEndpointCounterCache.Size()))
	}
}

// INDEX CACHE
// 索引缓存的元素数据结构
type IndexCacheItem struct {
	// GraphItem的Endpoint、Metric、Tags、DsType、Step构成UUID
	UUID string
	Item *cmodel.GraphItem
}

func NewIndexCacheItem(uuid string, item *cmodel.GraphItem) *IndexCacheItem {
	return &IndexCacheItem{UUID: uuid, Item: item}
}

// 索引缓存-基本缓存容器
type IndexCacheBase struct {
	sync.RWMutex
	maxSize int
	data    map[string]interface{}
}

// 创建缓存容器
func NewIndexCacheBase(max int) *IndexCacheBase {
	return &IndexCacheBase{maxSize: max, data: make(map[string]interface{})}
}

// 最大容量
func (this *IndexCacheBase) GetMaxSize() int {
	return this.maxSize
}

// 更新数据
func (this *IndexCacheBase) Put(key string, item interface{}) {
	this.Lock()
	defer this.Unlock()
	this.data[key] = item
}

// 删除数据
func (this *IndexCacheBase) Remove(key string) {
	this.Lock()
	defer this.Unlock()
	delete(this.data, key)
}

// 获取数据
func (this *IndexCacheBase) Get(key string) interface{} {
	this.RLock()
	defer this.RUnlock()
	return this.data[key]
}

// 是否包含
func (this *IndexCacheBase) ContainsKey(key string) bool {
	this.RLock()
	defer this.RUnlock()
	return this.data[key] != nil
}

// 数量
func (this *IndexCacheBase) Size() int {
	this.RLock()
	defer this.RUnlock()
	return len(this.data)
}

// 返回key列表
func (this *IndexCacheBase) Keys() []string {
	this.RLock()
	defer this.RUnlock()

	count := len(this.data)
	if count == 0 {
		return []string{}
	}

	keys := make([]string, 0, count)
	for key := range this.data {
		keys = append(keys, key)
	}

	return keys
}
