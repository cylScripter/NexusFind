package service

import (
	"fmt"
	"github.com/cylScripter/NexusFind/config"
	"github.com/cylScripter/NexusFind/engine"
	"google.golang.org/grpc"
	"net"
)

var serviceApi *engine.Service

func StartService() {
	server := grpc.NewServer()
	serviceApi = engine.NewService("localhost", 50001, config.Config.Service.Etcd, logger)
	engine.RegisterIndexServiceServer(server, serviceApi)
	ls, err := net.Listen("tcp", fmt.Sprintf("%v:%v", service.LocalIP, 50001))
	if err != nil {
		panic(err)
	}
	logger.NFLog.Infof("Search engine (FexusFind)  service started successfully")
	serviceApi.Watch()
	err = server.Serve(ls) //Serve会一直阻塞，所以放到一个协程里异步执行
	if err != nil {
		fmt.Printf(err.Error())
	}
}
