package engine

import (
	"context"
	"fmt"
	"github.com/cylScripter/NexusFind/config"
	"github.com/cylScripter/NexusFind/internal/index/segment"
	"github.com/cylScripter/NexusFind/utils"
	"time"
)

var INDEX_SERVICE string

func init() {
	if config.Config == nil {
		INDEX_SERVICE = "index_service"
	}
}

type IndexServiceWorker struct {
	idxManager *IndexManager
	hub        *ServiceHub
	LocalIP    string // 本地IP
	LocalPort  int    // 本地端口号
	Logger     *utils.Log
}

func (isw *IndexServiceWorker) mustEmbedUnimplementedIndexServiceServer() {
	//TODO implement me
	panic("implement me")
}

func (isw *IndexServiceWorker) Init(etcdServers []string, heartbeatFrequency int64, ip string, port int, logger *utils.Log) {
	isw.LocalIP = ip
	isw.LocalPort = port
	isw.Logger = logger
	isw.idxManager = NewIndexManager(logger)
	isw.hub = GetServiceHub(etcdServers, heartbeatFrequency)
	//logger.NFLog.Error(INDEX_SERVICE)
}

func (isw *IndexServiceWorker) Register() error {
	var heartBeat int64 = isw.hub.heartbeatFrequency
	leaseId, err := isw.hub.Register(INDEX_SERVICE, fmt.Sprintf("%v:%v", isw.LocalIP, isw.LocalPort), 0)
	if err != nil {
		panic(err)
	}
	go func() {
		for {
			_, err = isw.hub.Register(INDEX_SERVICE, fmt.Sprintf("%v:%v", isw.LocalIP, isw.LocalPort), leaseId)
			if err != nil {
				isw.Logger.NFLog.Error(err.Error())
			}
			time.Sleep(time.Duration(heartBeat)*time.Second - 1000*time.Millisecond)
		}
	}()
	return nil
}

func (isw *IndexServiceWorker) Close() error {
	endpoint := fmt.Sprintf("%v:%v", isw.LocalIP, isw.LocalPort)
	err := isw.idxManager.Close()
	if err != nil {
		return err
	}
	if isw.hub != nil {
		err = isw.hub.UnRegister(INDEX_SERVICE, endpoint)
		if err == nil {
			isw.Logger.NFLog.Infof("worker [%v] successfully logged out", endpoint)
		} else {
			isw.Logger.NFLog.Error(err.Error())
		}
		isw.hub.Close()
	}
	return nil
}

func (isw *IndexServiceWorker) Delete(ctx context.Context, request *DocIdRequest) (*Code, error) {
	err := isw.idxManager.Delete(request.IndexName, request.DocId)
	if err != nil {
		return &Code{StatusCode: 0}, err
	}
	return &Code{StatusCode: 1}, nil
}
func (isw *IndexServiceWorker) CreateIndex(ctx context.Context, request *CreateIndexRequest) (*Code, error) {
	fields := make([]segment.SimpleFieldInfo, 0)
	for _, iter := range request.FieldInfo {
		field := segment.SimpleFieldInfo{FieldName: iter.FieldName, FieldType: iter.FieldType}
		fields = append(fields, field)
	}
	err := isw.idxManager.CreateIndex(request.IndexName, fields)
	return &Code{StatusCode: 1}, err
}
func (isw *IndexServiceWorker) Add(ctx context.Context, request *AddRequest) (*Code, error) {
	docid, err := isw.idxManager.Add(request.IndexName, request.Doc)
	return &Code{StatusCode: docid}, err
}
func (isw *IndexServiceWorker) Search(ctx context.Context, request *SearchRequest) (*Result, error) {
	result := isw.idxManager.Search(request.IndexName, request.Query, request.Filter)
	fmt.Println(len(result))
	return &Result{DocResult: result}, nil
}
func (isw *IndexServiceWorker) Get(ctx context.Context, request *DocIdRequest) (*GetResult, error) {
	doc, exist := isw.idxManager.Get(request.IndexName, request.DocId)
	if exist {
		return &GetResult{Doc: doc, Exist: exist}, nil
	}
	isw.Logger.NFLog.Errorf("document [%v] no has exists", request.DocId)
	return nil, fmt.Errorf("document [%v] no has exists", request.DocId)
}
