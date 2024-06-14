package main

import (
	"github.com/cylScripter/NexusFind/config"
	"github.com/cylScripter/NexusFind/service"
)

func main() {
	nodeName := config.Config.Service.NodeName
	service.Init()
	switch nodeName {
	case "master":
		go service.StopService()
		go service.StartWorker()
		service.StartService()
	default:
		go service.StopService()
		service.StartWorker()
	}
}
