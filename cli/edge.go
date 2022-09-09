package cli

import (
	"context"
	"fmt"
	"time"

	API "github.com/linguohua/titan/api"
	"github.com/urfave/cli/v2"
)

var EdgeCmds = []*cli.Command{
	DeviceInfoCmd,
	CacheBlockCmd,
	DeleteBlockCmd,
	ValidateBlockCmd,
	LimitRateCmd,
	DownloadInfoCmd,
	CacheStatCmd,
}

var DeviceInfoCmd = &cli.Command{
	Name:  "info",
	Usage: "Print device info",
	Action: func(cctx *cli.Context) error {
		api, closer, err := GetEdgeAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := ReqContext(cctx)
		// TODO: print more useful things

		v, err := api.DeviceInfo(ctx)
		if err != nil {
			return err
		}
		fmt.Printf("device id: %v \n", v.DeviceId)
		fmt.Printf("device name: %v \n", v.DeviceName)
		fmt.Printf("device external_ip: %v \n", v.ExternalIp)
		fmt.Printf("device internal_ip: %v \n", v.InternalIp)
		fmt.Printf("device systemVersion: %v \n", v.SystemVersion)
		fmt.Printf("device DiskUsage: %v \n", v.DiskUsage)
		fmt.Printf("device mac: %v \n", v.MacLocation)
		fmt.Printf("device download bandwidth: %v \n", v.BandwidthDown)
		fmt.Printf("device upload bandwidth: %v \n", v.BandwidthUp)

		return nil
	},
}

var CacheBlockCmd = &cli.Command{
	Name:  "cache",
	Usage: "cache block content",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "cid",
			Usage: "block cids",
			Value: "",
		},
		&cli.StringFlag{
			Name:  "fid",
			Usage: "block file id",
			Value: "",
		},
	},
	Action: func(cctx *cli.Context) error {
		fmt.Println("start cache data...")
		api, closer, err := GetEdgeAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		cid := cctx.String("cid")
		ctx := ReqContext(cctx)

		reqData := API.ReqCacheData{Cids: []string{cid}, CandidateURL: ""}

		err = api.CacheBlocks(ctx, reqData)
		if err != nil {
			return err
		}

		fmt.Println("cache data success")
		return nil
	},
}

var DeleteBlockCmd = &cli.Command{
	Name:  "delete",
	Usage: "delete blocks",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "cid",
			Usage: "block cid",
			Value: "",
		},
	},
	Action: func(cctx *cli.Context) error {
		api, closer, err := GetEdgeAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		cid := cctx.String("cid")

		results, err := api.AnnounceBlocksWasDelete(context.Background(), []string{cid})
		if err != nil {
			return err
		}

		if len(results) > 0 {
			log.Infof("delete block %s failed %v", cid, results[0].ErrMsg)
			return nil
		}

		log.Infof("delete block %s success", cid)
		return nil
	},
}

var ValidateBlockCmd = &cli.Command{
	Name:  "validate",
	Usage: "validate data",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "fid",
			Usage: "file id",
			Value: "",
		},
		&cli.StringFlag{
			Name:  "edge-url",
			Usage: "edge url",
			Value: "",
		},
	},
	Action: func(cctx *cli.Context) error {
		api, closer, err := GetCandidateAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		fid := cctx.String("fid")
		url := cctx.String("edge-url")
		fmt.Printf("fid:%s,url:%s", fid, url)
		ctx := ReqContext(cctx)
		// TODO: print more useful things
		req := make([]API.ReqValidate, 0)
		seed := time.Now().UnixNano()
		varify := API.ReqValidate{NodeURL: url, Seed: seed, Duration: 10}
		req = append(req, varify)

		err = api.ValidateBlocks(ctx, req)
		if err != nil {
			fmt.Println("err", err)
			return err
		}

		return nil
	},
}

var LimitRateCmd = &cli.Command{
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

var DownloadInfoCmd = &cli.Command{
	Name:  "downinfo",
	Usage: "get download server url and token",
	Flags: []cli.Flag{},
	Action: func(cctx *cli.Context) error {
		api, closer, err := GetEdgeAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := ReqContext(cctx)
		info, err := api.GetDownloadInfo(ctx)
		if err != nil {
			fmt.Printf("Unlimit speed failed:%v", err)
			return err
		}

		fmt.Printf("URL:%s\n", info.URL)
		fmt.Printf("Token:%s\n", info.Token)
		return nil
	},
}

var CacheStatCmd = &cli.Command{
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

		fmt.Printf("Cache block count %d, Wait cache count %d, Caching count %d", stat.CacheBlockCount, stat.WaitCacheBlockNum, stat.DoingCacheBlockNum)
		return nil
	},
}
