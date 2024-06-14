package utils

import (
	farmhash "github.com/leemcloughlin/gofarmhash"
	"math/rand/v2"
	"sync"
)

type ConcurrentHashMap struct {
	table []map[string]any
	locks []sync.RWMutex
	seg   int
	seed  uint32
}

type MapEntry struct {
	Key   string
	Value any
}

type MapIterator interface {
	Next() *MapEntry
}

func NewConcurrentHashMap(seg, cap int) *ConcurrentHashMap {
	table := make([]map[string]any, seg)
	for i := 0; i < seg; i++ {
		table[i] = make(map[string]any, cap/seg)
	}
	locks := make([]sync.RWMutex, seg)
	return &ConcurrentHashMap{
		table: table,
		locks: locks,
		seg:   seg,
		seed:  rand.Uint32(),
	}
}

// 判断key对应到哪个小map
func (m *ConcurrentHashMap) getSegIndex(key string) int {
	hash := int(farmhash.Hash32WithSeed([]byte(key), m.seed)) //FarmHash是google开源的Hash算法
	return hash % m.seg
}

func (m *ConcurrentHashMap) Get(key string) (value any, exits bool) {
	index := m.getSegIndex(key)
	m.locks[index].RLock()
	defer m.locks[index].RUnlock()
	value, exits = m.table[index][key]
	return value, exits
}

func (m *ConcurrentHashMap) Set(key string, value any) {
	index := m.getSegIndex(key)
	m.locks[index].Lock()
	defer m.locks[index].Unlock()
	m.table[index][key] = value
}

func (m *ConcurrentHashMap) Delete(key string) {
	index := m.getSegIndex(key)
	m.locks[index].Lock()
	defer m.locks[index].Unlock()
	delete(m.table[index], key)
}

func (m *ConcurrentHashMap) Range(fn func(key string, value any) error) {
	for i := 0; i < m.seg; i++ {
		m.locks[i].RLock()
		for k, v := range m.table[i] {
			err := fn(k, v)
			if err != nil {
				m.locks[i].RUnlock()
				panic(err)
			}
		}
		m.locks[i].RUnlock()
	}
}

type ConcurrentHashMapIterator struct {
	cm       *ConcurrentHashMap
	keys     [][]string
	rowIndex int
	colIndex int
}

func (m *ConcurrentHashMap) CreateIterator() *ConcurrentHashMapIterator {
	keys := make([][]string, len(m.table))
	for i := 0; i < len(m.table); i++ {
		colKeys := make([]string, 0)
		for key := range m.table[i] {
			colKeys = append(colKeys, key)
		}
		keys[i] = colKeys
	}
	return &ConcurrentHashMapIterator{
		cm:       m,
		keys:     keys,
		rowIndex: 0,
		colIndex: 0,
	}
}

func (iter *ConcurrentHashMapIterator) Next() *MapEntry {
	if iter.rowIndex >= len(iter.keys) {
		return nil
	}
	row := iter.keys[iter.rowIndex]
	if len(row) == 0 {
		iter.rowIndex += 1
		return iter.Next()
	}

	key := row[iter.colIndex]
	value, _ := iter.cm.Get(key)
	if iter.colIndex >= len(row)-1 {
		iter.rowIndex += 1
		iter.colIndex = 0
	} else {
		iter.colIndex += 1
	}
	return &MapEntry{
		key,
		value,
	}
}
