package service

import (
	"fmt"
	"github.com/cylScripter/NexusFind/config"
	"github.com/cylScripter/NexusFind/engine"
	"github.com/cylScripter/NexusFind/utils"
	"google.golang.org/grpc"
	"net"
	"os"
	"os/signal"
	"syscall"
)

var service *engine.IndexServiceWorker //IndexWorker，是一个grpc server
var logger *utils.Log

func Init() {

	logo := "\n" + `
	 _   _                 _____GO            _ 
	| \ | |               |  ____|           | |
	|  \| |_   _ _ __ ___ | |___  _ _ __   __| |
	| . ' | | | | '_ ' _ \|  ___|| | '_ \ / _' |
	| |\  | |_| | | | | | | |    | | | | | (_| |
	|_| \_|\__,_|_| |_| |_|_|    |_|_| |_|\__,_|
    `
	logger = utils.NewLogger("index_service")
	logger.NFLog.Notice(logo)
	service = new(engine.IndexServiceWorker)
	service.Init(config.Config.Service.Etcd,
		2,
		config.Config.Service.WorkerHost,
		config.Config.Service.WorkerPort,
		logger,
	)
}

func StartWorker() {
	endpoint := fmt.Sprintf("%v:%v", service.LocalIP, service.LocalPort)
	server := grpc.NewServer()
	engine.RegisterIndexServiceServer(server, service)
	err := service.Register()
	if err != nil {
		panic(err)
	}
	lis, err := net.Listen("tcp", endpoint)
	if err != nil {
		panic(err)
	}
	logger.NFLog.Infof(" worker [%v] has been started", endpoint)
	err = server.Serve(lis) //Serve会一直阻塞，所以放到一个协程里异步执行
	if err != nil {
		panic(err)
	}
}

func StopService() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	err := service.Close()
	if err != nil {
		panic(err)
	} //接收到kill信号时关闭索引
	os.Exit(0) //然后自杀
}
