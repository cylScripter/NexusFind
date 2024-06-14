package engine

import (
	"context"
	"fmt"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	etcdv3 "go.etcd.io/etcd/client/v3"
	"strings"
	"sync"
	"time"
)

type ServiceHub struct {
	client             *etcdv3.Client
	heartbeatFrequency int64 //server每隔几秒钟不动向中心上报一次心跳（其实就是续一次租约）
	watched            sync.Map
	loadBalancer       LoadBalancer //策略模式。完成同一个任务可以有多种不同的实现方案
}

const SERVICE_ROOT_PATH = "NFHub"

var (
	serviceHub *ServiceHub //该全局变量包外不可见，包外想使用时通过GetServiceHub()获得
	hubOnce    sync.Once   //单例模式需要用到一个once
)

func NewEtcdClient(etcdServers []string) (*etcdv3.Client, error) {
	client, err := etcdv3.New(etcdv3.Config{
		Endpoints:   etcdServers,
		DialTimeout: 3 * time.Second,
	})
	if err != nil {
		fmt.Println(err)
		return nil, fmt.Errorf("failed to connect to ETCD servers: %w", err)
	}
	return client, nil
}

func GetServiceHub(etcdServers []string, heartbeatFrequency int64) *ServiceHub {
	hubOnce.Do(func() {
		client, err := NewEtcdClient(etcdServers)
		if err != nil {
			panic(err)
		}
		serviceHub = &ServiceHub{
			client:             client,
			heartbeatFrequency: heartbeatFrequency,
			loadBalancer:       &RoundRobin{},
		}
	})
	return serviceHub
}

func (hub *ServiceHub) Register(service string, endpoint string, leaseID etcdv3.LeaseID) (etcdv3.LeaseID, error) {
	ctx := context.Background()
	if leaseID <= 0 {
		// 创建一个租约，有效期为heartbeatFrequency秒
		if lease, err := hub.client.Grant(ctx, hub.heartbeatFrequency); err != nil {
			return 0, err
		} else {
			key := strings.TrimRight(SERVICE_ROOT_PATH, "/") + "/" + service + "/" + endpoint
			// 服务注册
			if _, err = hub.client.Put(ctx, key, "", etcdv3.WithLease(lease.ID)); err != nil { //只需要key，不需要value
				return lease.ID, err
			} else {
				return lease.ID, nil
			}
		}
	} else {
		//续租
		if _, err := hub.client.KeepAliveOnce(ctx, leaseID); err == rpctypes.ErrLeaseNotFound { //续约一次，到期后还得再续约
			return hub.Register(service, endpoint, 0) //找不到租约，走注册流程(把leaseID置为0)
		} else if err != nil {
			return 0, err
		} else {
			return leaseID, nil
		}
	}
}

func (hub *ServiceHub) UnRegister(service string, endpoint string) error {
	ctx := context.Background()
	key := strings.TrimRight(SERVICE_ROOT_PATH, "/") + "/" + service + "/" + endpoint
	if _, err := hub.client.Delete(ctx, key); err != nil {
		return err
	} else {
		return nil
	}
}

func (hub *ServiceHub) GetServiceEndpoints(service string) []string {
	ctx := context.Background()
	prefix := strings.TrimRight(SERVICE_ROOT_PATH, "/") + "/" + service + "/"
	if resp, err := hub.client.Get(ctx, prefix, etcdv3.WithPrefix()); err != nil { //按前缀获取key-value
		return nil
	} else {
		endpoints := make([]string, 0, len(resp.Kvs))
		for _, kv := range resp.Kvs {
			path := strings.Split(string(kv.Key), "/") //只需要key，不需要value
			// fmt.Println(string(kv.Key), path[len(path)-1])
			endpoints = append(endpoints, path[len(path)-1])
		}
		return endpoints
	}
}

func (hub *ServiceHub) GetServiceEndpoint(service string) string {
	return hub.loadBalancer.Take(hub.GetServiceEndpoints(service))
}

func (hub *ServiceHub) Close() {
	hub.client.Close()
}
