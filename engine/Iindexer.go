package engine

import etcdv3 "go.etcd.io/etcd/client/v3"

type IServiceHub interface {
	Register(service string, endpoint string, leaseID etcdv3.LeaseID) (etcdv3.LeaseID, error) // 注册服务
	UnRegister(service string, endpoint string) error                                         // 注销服务
	GetServiceEndpoints(service string) []string                                              //服务发现
	GetServiceEndpoint(service string) string                                                 //选择服务的一台endpoint
	Close()                                                                                   //关闭etcd client connection
}
