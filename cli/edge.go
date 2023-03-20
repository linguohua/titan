package cli

import (
	"fmt"
	"os"

	"github.com/urfave/cli/v2"
)

var EdgeCmds = []*cli.Command{
	nodeInfoCmd,
	cacheStatCmd,
	logFileCmd,
	progressCmd,
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

var progressCmd = &cli.Command{
	Name:  "progress",
	Usage: "get cache progress",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "cid",
			Usage: "carfile cid",
			Value: "",
		},
	},
	Action: func(cctx *cli.Context) error {
		edgeAPI, closer, err := GetEdgeAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		cid := cctx.String("cid")
		ctx := ReqContext(cctx)
		progresses, err := edgeAPI.CachedProgresses(ctx, []string{cid})
		if err != nil {
			return err
		}

		for _, progress := range progresses {
			fmt.Printf("Cache carfile %s %v\n", progress.CarfileCid, progress.Status)
			fmt.Printf("Total block count %d, done block count %d  \n", progress.CarfileBlocksCount, progress.DoneBlocksCount)
			fmt.Printf("Total block size %d, done block siez %d  \n", progress.CarfileSize, progress.DoneSize)
			fmt.Println()
		}
		return nil
	},
}
