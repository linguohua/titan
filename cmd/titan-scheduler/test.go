package main

import (
	"fmt"
	"time"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/client"
	lcli "github.com/linguohua/titan/cli"
	"github.com/urfave/cli/v2"
)

var initDeviceIDs = &cli.Command{
	Name:  "initDeviceID",
	Usage: "init deviceID",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "api-url",
			Usage: "host address and port the worker api will listen on",
			Value: "127.0.0.1:3456",
		},
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		url := cctx.String("api-url")
		log.Infof("scheduler url:%v", url)

		ctx := lcli.ReqContext(cctx)

		var schedulerAPI api.Scheduler
		var closer func()
		var err error
		for {
			schedulerAPI, closer, err = client.NewScheduler(ctx, url, nil)
			if err == nil {
				_, err = schedulerAPI.Version(ctx)
				if err == nil {
					break
				}
			}
			fmt.Printf("\r\x1b[0KConnecting to miner API... (%s)", err)
			time.Sleep(time.Second)
			continue
		}

		defer closer()

		err = schedulerAPI.InitNodeDeviceIDs(ctx)
		if err != nil {
			log.Infof("InitNodeDeviceIDs err:%v", err)
		}

		return err
	},
}
