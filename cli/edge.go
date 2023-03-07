package cli

import (
	"fmt"
	"os"

	"github.com/urfave/cli/v2"
)

var EdgeCmds = []*cli.Command{
	nodeInfoCmd,
	limitRateCmd,
	cacheStatCmd,
	logFileCmd,
}

var nodeInfoCmd = &cli.Command{
	Name:  "info",
	Usage: "Print node info",
	Action: func(cctx *cli.Context) error {
		api, closer, err := GetEdgeAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := ReqContext(cctx)
		// TODO: print more useful things

		v, err := api.NodeInfo(ctx)
		if err != nil {
			return err
		}
		fmt.Printf("node id: %v \n", v.NodeID)
		fmt.Printf("node name: %v \n", v.NodeName)
		fmt.Printf("node external_ip: %v \n", v.ExternalIP)
		fmt.Printf("node internal_ip: %v \n", v.InternalIP)
		fmt.Printf("node systemVersion: %s \n", v.SystemVersion)
		fmt.Printf("node DiskUsage: %f \n", v.DiskUsage)
		fmt.Printf("node disk space: %f \n", v.DiskSpace)
		fmt.Printf("node fstype: %s \n", v.IoSystem)
		fmt.Printf("node mac: %v \n", v.MacLocation)
		fmt.Printf("node download bandwidth: %v \n", v.BandwidthDown)
		fmt.Printf("node upload bandwidth: %v \n", v.BandwidthUp)
		fmt.Printf("node cpu percent: %v \n", v.CPUUsage)

		return nil
	},
}

var limitRateCmd = &cli.Command{
	Name:  "limit",
	Usage: "limit rate",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "rate",
			Usage: "speed rate",
			Value: "",
		},
	},
	Action: func(cctx *cli.Context) error {
		api, closer, err := GetEdgeAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		speed := cctx.Int64("rate")

		ctx := ReqContext(cctx)

		err = api.SetDownloadSpeed(ctx, speed)
		if err != nil {
			fmt.Printf("Set Download speed failed:%v", err)
			return err
		}
		fmt.Printf("Set download speed %d success", speed)
		return nil
	},
}

var cacheStatCmd = &cli.Command{
	Name:  "stat",
	Usage: "cache stat",
	Flags: []cli.Flag{},
	Action: func(cctx *cli.Context) error {
		api, closer, err := GetEdgeAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := ReqContext(cctx)
		stat, err := api.QueryCacheStat(ctx)
		if err != nil {
			fmt.Printf("Unlimit speed failed:%v", err)
			return err
		}

		fmt.Printf("nodeMgrCache storage count %d, block count %d, wati cache storage count %d", stat.TotalCarfileCount, stat.TotalBlockCount, stat.WaitCacheCarfileCount)
		return nil
	},
}

var logFileCmd = &cli.Command{
	Name:  "log-file",
	Usage: "ls log file or get log file",
	Subcommands: []*cli.Command{
		listLogFileCmd,
		getLogFileCmd,
	},
}

var listLogFileCmd = &cli.Command{
	Name:  "ls",
	Usage: "list log file",
	Action: func(cctx *cli.Context) error {
		edgeAPI, closer, err := GetEdgeAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := ReqContext(cctx)
		info, err := edgeAPI.ShowLogFile(ctx)
		if err != nil {
			return err
		}

		if info == nil {
			fmt.Printf("Log file not exist")
			return nil
		}

		fmt.Printf("%s %dB", info.Name, info.Size)
		return nil
	},
}

var getLogFileCmd = &cli.Command{
	Name:  "get",
	Usage: "get log file",
	Action: func(cctx *cli.Context) error {
		edgeAPI, closer, err := GetEdgeAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := ReqContext(cctx)
		info, err := edgeAPI.ShowLogFile(ctx)
		if err != nil {
			return err
		}

		if info == nil {
			fmt.Printf("Log file not exist")
			return nil
		}

		data, err := edgeAPI.DownloadLogFile(ctx)
		if err != nil {
			return err
		}

		filePath := "./" + info.Name
		return os.WriteFile(filePath, data, 0o644)
	},
}
