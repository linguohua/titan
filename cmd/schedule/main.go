package main

import (
	"os"

	"titan-ultra-network/log"

	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
)

// 调度中心

var ver = "4.6"

// @BasePath /api/v1
func main() {
	logrus.Infoln("start titan-ultra-network-dispatch server...")

	app := cli.NewApp()
	app.Name = "titan-ultra-network-dispatch"
	app.Usage = "a titan-ultra-network-dispatch server"

	app.Flags = []cli.Flag{
		// 有参数则用参数，没参数才会使用环境变量
		&cli.StringFlag{
			Name:  "c",
			Value: "config.toml",
			Usage: "配置地址",
			// Destination: &port,
			// EnvVars: []string{"WALLET_PORT"},
		},
	}

	app.Action = func(c *cli.Context) error {
		configPath := c.String("c")

		err := LoadFromFile(configPath)
		if err != nil {
			return err
		}

		// port := GetListenPort()

		// 日志初始化
		log.InitLogger(GetLogConfig().LogDir,
			GetLogConfig().LogName,
			GetLogConfig().LogLevel)

		log.Info("版本:", ver)

		// 开启Http服务
		// params := fmt.Sprintf(":%s", port)
		// router.StartHTTPServer(params)

		return nil
	}

	err := app.Run(os.Args)
	if err != nil {
		logrus.Fatal(err)
	}
}
