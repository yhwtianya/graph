package store

import (
	tlist "github.com/toolkits/container/list"
	tmap "github.com/toolkits/container/nmap"

	cmodel "github.com/open-falcon/common/model"
)

const (
	defaultHistorySize = 3
)

var (
	// 保存上报的每个监控项的数据，默认仅保存3条
	// 最前面是最新数据，key为md5sum，value为SafeListLimited
	// SafeListLimited元素类型为GraphItem，元素个数为defaultHistorySize
	// HistoryCache虽为可导出变量，但仅在此文件的可导出函数中使用
	// mem:  front = = = back
	// time: latest ...  old
	HistoryCache = tmap.NewSafeMap()
)

// 获取最新一个数据
func GetLastItem(key string) *cmodel.GraphItem {
	itemlist, found := HistoryCache.Get(key)
	if !found || itemlist == nil {
		return &cmodel.GraphItem{}
	}

	first := itemlist.(*tlist.SafeListLimited).Front()
	if first == nil {
		return &cmodel.GraphItem{}
	}

	return first.(*cmodel.GraphItem)
}

// 获取所有数据，默认最新3条
func GetAllItems(key string) []*cmodel.GraphItem {
	ret := make([]*cmodel.GraphItem, 0)
	itemlist, found := HistoryCache.Get(key)
	if !found || itemlist == nil {
		return ret
	}

	all := itemlist.(*tlist.SafeListLimited).FrontAll()
	for _, item := range all {
		if item == nil {
			continue
		}
		ret = append(ret, item.(*cmodel.GraphItem))
	}
	return ret
}

// 添加数据，默认保存3条,在rpc Send中被调用
func AddItem(key string, val *cmodel.GraphItem) {
	itemlist, found := HistoryCache.Get(key)
	var slist *tlist.SafeListLimited
	if !found {
		slist = tlist.NewSafeListLimited(defaultHistorySize)
		HistoryCache.Put(key, slist)
	} else {
		slist = itemlist.(*tlist.SafeListLimited)
	}

	// old item should be drop
	first := slist.Front()
	// 确保数据时间是最新的
	if first == nil || first.(*cmodel.GraphItem).Timestamp < val.Timestamp { // first item or latest one
		slist.PushFrontViolently(val)
	}
}
