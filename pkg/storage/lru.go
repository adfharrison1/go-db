package storage

import (
	"container/list"
	"sync"
	"time"

	"github.com/adfharrison1/go-db/pkg/data"
)

type LRUCache struct {
	mu       sync.RWMutex
	capacity int
	list     *list.List
	cache    map[string]*list.Element
}

type cacheEntry struct {
	key   string
	value *data.Collection
	info  *CollectionInfo
}

func NewLRUCache(capacity int) *LRUCache {
	return &LRUCache{
		capacity: capacity,
		list:     list.New(),
		cache:    make(map[string]*list.Element),
	}
}

func (lru *LRUCache) Get(key string) (*data.Collection, *CollectionInfo, bool) {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	if element, exists := lru.cache[key]; exists {
		entry := element.Value.(*cacheEntry)
		lru.list.MoveToFront(element)
		entry.info.AccessCount++
		entry.info.LastAccessed = time.Now()
		return entry.value, entry.info, true
	}
	return nil, nil, false
}

func (lru *LRUCache) Put(key string, collection *data.Collection, info *CollectionInfo) {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	if element, exists := lru.cache[key]; exists {
		entry := element.Value.(*cacheEntry)
		entry.value = collection
		entry.info = info
		lru.list.MoveToFront(element)
		return
	}

	entry := &cacheEntry{key: key, value: collection, info: info}
	element := lru.list.PushFront(entry)
	lru.cache[key] = element

	if lru.list.Len() > lru.capacity {
		lru.evictOldest()
	}
}

func (lru *LRUCache) evictOldest() {
	element := lru.list.Back()
	if element != nil {
		entry := element.Value.(*cacheEntry)
		delete(lru.cache, entry.key)
		lru.list.Remove(element)
	}
}

func (lru *LRUCache) Remove(key string) {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	if element, exists := lru.cache[key]; exists {
		delete(lru.cache, key)
		lru.list.Remove(element)
	}
}

func (lru *LRUCache) Capacity() int {
	return lru.capacity
}

func (lru *LRUCache) Len() int {
	return lru.list.Len()
}

func (lru *LRUCache) CacheLen() int {
	return len(lru.cache)
}
