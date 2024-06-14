package engine

import (
	"context"
	"github.com/cylScripter/NexusFind/utils"
)

type Service struct {
	ServiceIp   string
	ServicePort int
	sentinel    *Sentinel
}

func (svc *Service) mustEmbedUnimplementedIndexServiceServer() {
	//TODO implement me
	panic("implement me")
}

func NewService(ip string, port int, etcdServers []string, logger *utils.Log) *Service {
	return &Service{
		ServiceIp:   ip,
		ServicePort: port,
		sentinel:    NewSentinel(etcdServers, logger),
	}
}

func (svc *Service) Delete(ctx context.Context, request *DocIdRequest) (*Code, error) {
	code, err := svc.sentinel.Delete(request)
	return &Code{StatusCode: uint64(code)}, err
}

func (svc *Service) CreateIndex(ctx context.Context, request *CreateIndexRequest) (*Code, error) {
	code, err := svc.sentinel.CreateIndex(request)
	return &Code{StatusCode: uint64(code)}, err
}

func (svc *Service) Add(ctx context.Context, request *AddRequest) (*Code, error) {
	code, err := svc.sentinel.Add(request)
	return &Code{StatusCode: uint64(code)}, err
}

func (svc *Service) Search(ctx context.Context, request *SearchRequest) (*Result, error) {
	docList, err := svc.sentinel.Search(request)

	return &Result{DocResult: docList}, err
}

func (svc *Service) Get(ctx context.Context, request *DocIdRequest) (*GetResult, error) {
	docs, err := svc.sentinel.Get(request)
	var exist bool
	if docs != nil {
		exist = true
	} else {
		exist = false
	}
	return &GetResult{Doc: docs, Exist: exist}, err
}

func (svc *Service) Close() {
	err := svc.sentinel.Close()
	if err != nil {
		return
	}
}

func (svc *Service) Watch() {
	svc.sentinel.hub.watchService(INDEX_SERVICE)
}
