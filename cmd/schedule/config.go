package main

import (
	"github.com/BurntSushi/toml"
	log "github.com/sirupsen/logrus"
)

var conf *yconfig

// Log 日志配置
type Log struct {
	LogPath  string `toml:"logPath"`
	LogDir   string `toml:"logDir"`
	LogName  string `toml:"logName"`
	LogLevel string `toml:"logLevel"`
}

type yconfig struct {
	ListenPort   string  `toml:"listenPort"`
	LogConfig    *Log    `toml:"log"`
	PredictPath  string  `toml:"predictPath"`
	FilworldURL  string  `toml:"filworldURL"`
	ExchangeRate float64 `toml:"exchangeRate"`
	IsTest       bool    `toml:"isTest"`
	Redis        string  `toml:"redis"`
}

// LoadFromFile 加载配置
func LoadFromFile(configPath string) error {
	var config yconfig
	if _, err := toml.DecodeFile(configPath, &config); err != nil {
		log.Errorln(err)
		return err
	}

	conf = &config

	return nil
}

// GetListenPort 获取端口
func GetListenPort() string {
	return conf.ListenPort
}

// GetLogConfig log
func GetLogConfig() *Log {
	return conf.LogConfig
}

// GetPredictPath 测算表
func GetPredictPath() string {
	return conf.PredictPath
}

// GetFilworldURL 浏览器URL
func GetFilworldURL() string {
	return conf.FilworldURL
}

// GetExchangeRate 默认汇率
func GetExchangeRate() float64 {
	return conf.ExchangeRate
}

// GetIsTest 是否测试数据
func GetIsTest() bool {
	return conf.IsTest
}

// GetRedis redis
func GetRedis() string {
	return conf.Redis
}
