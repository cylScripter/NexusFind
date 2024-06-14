package config

import (
	"fmt"
	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
	"os"
)

type Application struct {
	ConfigViper *viper.Viper
	Service     *ServiceConfig `json:"service"`
}

var Config = new(Application)

func init() {
	Config.ConfigViper = InitializeConfig()
}

func InitializeConfig() *viper.Viper {
	// 设置配置文件路径
	config := "./config/config.yaml"
	// 生产环境可以通过设置环境变量来改变配置文件路径
	if configEnv := os.Getenv("VIPER_CONFIG"); configEnv != "" {
		config = configEnv
	}

	// 初始化 viper
	v := viper.New()
	v.SetConfigFile(config)
	v.SetConfigType("yaml")
	if err := v.ReadInConfig(); err != nil {
		return nil
	}

	// 监听配置文件
	v.WatchConfig()
	v.OnConfigChange(func(in fsnotify.Event) {
		fmt.Println("config file changed:", in.Name)
		if err := v.Unmarshal(Config.Service); err != nil {
			fmt.Println(err)
		}
	})
	// 将配置赋值给全局变量
	if err := v.Unmarshal(&Config); err != nil {
		fmt.Println(err)
	}
	return v
}
