package store

import (
	"container/list"
	"sync"

	cmodel "github.com/open-falcon/common/model"
)

type SafeLinkedList struct {
	sync.RWMutex
	Flag uint32
	L    *list.List // 元素类型为GraphItem
}

// 新创建SafeLinkedList容器
func NewSafeLinkedList() *SafeLinkedList {
	return &SafeLinkedList{L: list.New()}
}

// 首部插入单元素
func (this *SafeLinkedList) PushFront(v interface{}) *list.Element {
	this.Lock()
	defer this.Unlock()
	return this.L.PushFront(v)
}

// 获得首元素
func (this *SafeLinkedList) Front() *list.Element {
	this.RLock()
	defer this.RUnlock()
	return this.L.Front()
}

// 获得并删除尾元素
func (this *SafeLinkedList) PopBack() *list.Element {
	this.Lock()
	defer this.Unlock()

	back := this.L.Back()
	if back != nil {
		this.L.Remove(back)
	}

	return back
}

// 获得尾元素
func (this *SafeLinkedList) Back() *list.Element {
	this.Lock()
	defer this.Unlock()

	return this.L.Back()
}

// 返回列表长度
func (this *SafeLinkedList) Len() int {
	this.RLock()
	defer this.RUnlock()
	return this.L.Len()
}

// 取出并清空所有元素(元素转为GraphItem结构)
// remain参数表示要给linkedlist中留几个元素
// 在cron中刷磁盘的时候要留一个，用于创建数据库索引
// 插入时是新数据在头部,PopAll时是新数据在尾部
// 在程序退出的时候要一个不留的全部刷到磁盘
func (this *SafeLinkedList) PopAll() []*cmodel.GraphItem {
	this.Lock()
	defer this.Unlock()

	size := this.L.Len()
	if size <= 0 {
		return []*cmodel.GraphItem{}
	}

	ret := make([]*cmodel.GraphItem, 0, size)

	for i := 0; i < size; i++ {
		item := this.L.Back()
		ret = append(ret, item.Value.(*cmodel.GraphItem))
		this.L.Remove(item)
	}

	return ret
}

// 尾部插入多个元素(元素转为GraphItem结构)
//restore PushAll
func (this *SafeLinkedList) PushAll(items []*cmodel.GraphItem) {
	this.Lock()
	defer this.Unlock()

	size := len(items)
	if size > 0 {
		for i := size - 1; i >= 0; i-- {
			this.L.PushBack(items[i])
		}
	}
}

// 获取所有元素和flag, 插入时是新数据在头部,FetchAll时是新数据在尾部
//return为倒叙的?
func (this *SafeLinkedList) FetchAll() ([]*cmodel.GraphItem, uint32) {
	this.Lock()
	defer this.Unlock()
	count := this.L.Len()
	ret := make([]*cmodel.GraphItem, 0, count)

	p := this.L.Back()
	for p != nil {
		ret = append(ret, p.Value.(*cmodel.GraphItem))
		p = p.Prev()
	}

	return ret, this.Flag
}
