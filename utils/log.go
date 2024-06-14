package utils

import (
	"fmt"
	logger "github.com/phachon/go-logger"
)

func PrintLogo() {
	logo := `
	 _   _                 _____GO            _ 
	| \ | |               |  ____|           | |
	|  \| |_   _ _ __ ___ | |___  _ _ __   __| |
	| . ' | | | | '_ ' _ \|  ___|| | '_ \ / _' |
	| |\  | |_| | | | | | | |    | | | | | (_| |
	|_| \_|\__,_|_| |_| |_|_|    |_|_| |_|\__,_|
    `
	fmt.Println(logo)
}

type Log struct {
	IndexName string
	NFLog     *logger.Logger
}

func NewLogger(indexName string) *Log {
	self := &Log{IndexName: indexName}
	log := logger.NewLogger()
	//default attach console, detach console
	log.Detach("console")
	consoleConfig := &logger.ConsoleConfig{
		Color:      true,
		JsonFormat: false,
		Format:     "%millisecond_format% [" + indexName + "] [%level_string%] %file%:%line% %body%",
	}
	log.Attach("console", logger.LOGGER_LEVEL_DEBUG, consoleConfig)
	log.SetAsync()
	self.NFLog = log
	return self
}
