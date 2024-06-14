package engine

import (
	"context"
	"github.com/cylScripter/NexusFind/utils"
	etcdv3 "go.etcd.io/etcd/client/v3"
	"golang.org/x/time/rate"
	"strings"
	"sync"
	"time"
)

type HubProxy struct {
	*ServiceHub
	limiter       *rate.Limiter
	endpointCache sync.Map //维护每一个service下的所有servers
	logger        *utils.Log
}

var (
	proxy     *HubProxy
	proxyOnce sync.Once
)

// GetServiceHubProxy 返回 HubProxy 的单例实例
func GetServiceHubProxy(etcdServers []string, heartbeatFrequency int64, qps int, logger *utils.Log) *HubProxy {
	proxyOnce.Do(func() {
		proxy = createHubProxy(etcdServers, heartbeatFrequency, qps, logger)
	})
	return proxy
}

// createHubProxy 创建并返回 HubProxy 实例
func createHubProxy(etcdServers []string, heartbeatFrequency int64, qps int, logger *utils.Log) *HubProxy {
	sHub := GetServiceHub(etcdServers, heartbeatFrequency)
	if sHub == nil {
		return nil
	}
	return &HubProxy{
		ServiceHub: sHub,
		limiter:    rate.NewLimiter(rate.Every(time.Duration(1e9/qps)*time.Nanosecond), qps),
		logger:     logger,
	}
}

func (proxy *HubProxy) GetServiceEndpoints(service string) []string {
	//ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	//defer cancel()
	//proxy.limiter.Wait(ctx) //阻塞，直到桶中有1个令牌或超时。

	if !proxy.limiter.Allow() { //不阻塞，如果桶中没有1个令牌，则函数直接返回空，即没有可用的endpoints
		proxy.logger.NFLog.Warning("Please do not frequently request interfaces")
		return nil
	}
	if endpoints, exists := proxy.endpointCache.Load(service); exists {
		return endpoints.([]string)
	} else {
		endpoint := proxy.ServiceHub.GetServiceEndpoints(service) //显式调用父类的GetServiceEndpoints()
		if len(endpoint) > 0 {
			proxy.endpointCache.Store(service, endpoint) //查询etcd的结果放入本地缓存
		}
		return endpoint
	}
}

func (proxy *HubProxy) watchService(service string) {
	if _, exists := proxy.watched.LoadOrStore(service, true); exists { //watched是从父类继承过来的
		return //监听过了，不用重复监听
	}
	ctx := context.Background()
	prefix := strings.TrimRight(SERVICE_ROOT_PATH, "/") + "/" + service + "/"
	ch := proxy.ServiceHub.client.Watch(ctx, prefix, etcdv3.WithPrefix()) //根据前缀监听，每一个修改都会放入管道ch。client是从父类继承过来的
	go func() {
		for response := range ch { //遍历管道。这是个死循环，除非关闭管道
			for _, event := range response.Events { //每次从ch里取出来的是事件的集合
				path := strings.Split(string(event.Kv.Key), "/")
				if len(path) > 2 {
					service := path[len(path)-2]
					endpoints := proxy.ServiceHub.GetServiceEndpoints(service) //显式调用父类的GetServiceEndpoints()
					proxy.logger.NFLog.Infof("The current workers with heartbeat are %v", endpoints)
					if len(endpoints) > 0 {
						proxy.endpointCache.Store(service, endpoints) //查询etcd的结果放入本地缓存
					} else {
						proxy.endpointCache.Delete(service) //该service下已经没有endpoint
					}
				}
			}
		}
	}()
}
