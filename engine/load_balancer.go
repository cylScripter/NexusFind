package engine

import (
	"math/rand"
	"sync/atomic"
)

type LoadBalancer interface {
	Take([]string) string
}

type RoundRobin struct {
	acc int64
}

func (b *RoundRobin) Take(endpoints []string) string {
	if len(endpoints) == 0 {
		return ""
	}
	n := atomic.AddInt64(&b.acc, 1) //Take()需要支持并发调用
	index := int(n % int64(len(endpoints)))
	return endpoints[index]
}

type RandomSelect struct {
}

func (b *RandomSelect) Take(endpoints []string) string {
	if len(endpoints) == 0 {
		return ""
	}
	index := rand.Intn(len(endpoints)) // 随机选择
	return endpoints[index]
}
