package engine

import (
	"context"
	"fmt"
	"github.com/cylScripter/NexusFind/types/doc"
	"github.com/cylScripter/NexusFind/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"sync"
	"sync/atomic"
	"time"
)

type Sentinel struct {
	hub      *HubProxy
	connPool sync.Map // 与各个IndexServiceWorker建立的连接。把连接缓存起来，避免每次都重建连接
}

func NewSentinel(etcdServers []string, logger *utils.Log) *Sentinel {
	return &Sentinel{
		// hub: GetServiceHub(etcdServers, 10), //直接访问ServiceHub
		hub:      GetServiceHubProxy(etcdServers, 10, 100, logger), //走代理HubProxy
		connPool: sync.Map{},
	}
}

func (sentinel *Sentinel) GetGrpcConn(endpoint string) *grpc.ClientConn {
	if v, exists := sentinel.connPool.Load(endpoint); exists {
		conn := v.(*grpc.ClientConn)
		//如果连接状态不可用，则从连接缓存中删除
		if conn.GetState() == connectivity.TransientFailure || conn.GetState() == connectivity.Shutdown {
			conn.Close()
			sentinel.connPool.Delete(endpoint)
		} else {
			return conn //缓存中有该连接，则直接返回
		}
	}
	//连接到服务端
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond) //控制连接超时
	defer cancel()
	conn, err := grpc.DialContext(
		ctx,
		endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()), //Credential即使为空，也必须设置
		//grpc.Dial是异步连接的，连接状态为正在连接。但如果你设置了 grpc.WithBlock 选项，就会阻塞等待（等待握手成功）。另外你需要注意，当未设置 grpc.WithBlock 时，ctx 超时控制对其无任何效果。
		grpc.WithBlock(),
	)
	if err != nil {
		return nil
	}
	sentinel.connPool.Store(endpoint, conn)
	return conn
}

func (sentinel *Sentinel) Add(request *AddRequest) (int, error) {
	endpoint := sentinel.hub.GetServiceEndpoint(INDEX_SERVICE) // 根据负载均衡策略，选择一台index worker，把doc添加到它上面去
	if len(endpoint) == 0 {
		return 0, fmt.Errorf("there is no alive index worker")
	}
	conn := sentinel.GetGrpcConn(endpoint)
	if conn == nil {
		return 0, fmt.Errorf("connect to worker %s failed", endpoint)
	}
	client := NewIndexServiceClient(conn)
	affected, err := client.Add(context.Background(), request)
	if err != nil {
		return 0, err
	}

	return int(affected.StatusCode), nil
}

func (sentinel *Sentinel) Delete(request *DocIdRequest) (int, error) {
	endpoints := sentinel.hub.GetServiceEndpoints(INDEX_SERVICE)
	if len(endpoints) == 0 {
		return 0, fmt.Errorf("")
	}
	var n int32
	wg := sync.WaitGroup{}
	wg.Add(len(endpoints))
	for _, endpoint := range endpoints {
		go func(endpoint string) { //并行到各个IndexServiceWorker上把docId删除。正常情况下只有一个worker上有该doc
			defer wg.Done()
			conn := sentinel.GetGrpcConn(endpoint)
			if conn != nil {
				client := NewIndexServiceClient(conn)
				affected, err := client.Delete(context.Background(), request)
				if err != nil {
					fmt.Println()
				} else {
					if affected.StatusCode == 1 {
						atomic.AddInt32(&n, 1)
					}
				}
			}
		}(endpoint)
	}
	wg.Wait()
	return int(atomic.LoadInt32(&n)), nil
}

func (sentinel *Sentinel) Close() (err error) {
	sentinel.connPool.Range(func(key, value any) bool {
		conn := value.(*grpc.ClientConn)
		err = conn.Close()
		return true
	})
	sentinel.hub.Close()
	return
}

func (sentinel *Sentinel) CreateIndex(request *CreateIndexRequest) (int, error) {
	endpoints := sentinel.hub.GetServiceEndpoints(INDEX_SERVICE) // 根据负载均衡策略，选择一台index worker，把doc添加到它上面去
	if len(endpoints) == 0 {
		return 0, fmt.Errorf("there is no alive index worker")
	}
	var n int32
	wg := sync.WaitGroup{}
	wg.Add(len(endpoints))
	for _, endpoint := range endpoints {
		go func(endpoint string) { //并行到各个IndexServiceWorker上把docId删除。正常情况下只有一个worker上有该doc
			defer wg.Done()
			conn := sentinel.GetGrpcConn(endpoint)
			if conn != nil {
				client := NewIndexServiceClient(conn)
				affected, err := client.CreateIndex(context.Background(), request)
				if err != nil {
					fmt.Println()
				} else {
					if affected.StatusCode == 1 {
						atomic.AddInt32(&n, 1)
					}
				}
			}
		}(endpoint)
	}
	wg.Wait()
	return int(atomic.LoadInt32(&n)), nil
}

func (sentinel *Sentinel) Search(request *SearchRequest) ([]*doc.Document, error) {
	endpoints := sentinel.hub.GetServiceEndpoints(INDEX_SERVICE)
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("")
	}
	docs := make([]*doc.Document, 0, 1000)
	resultCh := make(chan *doc.Document, 1000)
	wg := sync.WaitGroup{}
	wg.Add(len(endpoints))
	for _, endpoint := range endpoints {
		go func(endpoint string) {
			defer wg.Done()
			conn := sentinel.GetGrpcConn(endpoint)
			if conn != nil {
				client := NewIndexServiceClient(conn)
				result, err := client.Search(context.Background(), request)
				fmt.Println(result)
				if err != nil {
					fmt.Println(err)
				} else {
					if len(result.DocResult) > 0 {
						for _, d := range result.DocResult {
							resultCh <- d
						}
					}
				}
			}
		}(endpoint)
	}
	receiveFinish := make(chan struct{})
	go func() { //为什么要放到一个子协程里？因为里面有个无限for循环，只有“//1”执行了该for循环才能退出
		for {
			d, ok := <-resultCh
			if !ok {
				break //2
			}
			docs = append(docs, d)
		}
		receiveFinish <- struct{}{} //3
	}()
	wg.Wait()
	close(resultCh) //1
	<-receiveFinish //4
	return docs, nil
}

func (sentinel *Sentinel) Get(request *DocIdRequest) (*doc.Document, error) {
	endpoints := sentinel.hub.GetServiceEndpoints(INDEX_SERVICE)
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("")
	}
	var docs *doc.Document
	for _, endpoint := range endpoints {
		conn := sentinel.GetGrpcConn(endpoint)
		if conn != nil {
			client := NewIndexServiceClient(conn)
			result, _ := client.Get(context.Background(), request)
			if result.GetExist() {
				docs = result.Doc
				break
			}
		}

	}
	return docs, nil
}
