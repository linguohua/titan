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
	CacheDataCmd,
	VerfyDataCmd,
	DoVerifyCmd,
	DeleteBlockCmd,
	VerfyDataCmd,
	LimitRateCmd,
	UnlimitRateCmd,
	GenerateTokenCmd,
}

var DeviceInfoCmd = &cli.Command{
	Name:  "deviceinfo",
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
		fmt.Printf("device download srv url: %v \n", v.DownloadSrvURL)
		fmt.Printf("device download bandwidth: %v \n", v.BandwidthDown)
		fmt.Printf("device upload bandwidth: %v \n", v.BandwidthUp)

		return nil
	},
}

var CacheDataCmd = &cli.Command{
	Name:  "cachedata",
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

		err = api.CacheData(ctx, reqData)
		if err != nil {
			return err
		}

		fmt.Println("cache data success")
		return nil
	},
}

var DoVerifyCmd = &cli.Command{
	Name:  "doverify",
	Usage: "do verify edge",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "fid",
			Usage: "block file id",
			Value: "",
		},
		&cli.StringFlag{
			Name:  "url",
			Usage: "candiate node url",
			Value: "",
		},
	},
	Action: func(cctx *cli.Context) error {
		api, closer, err := GetEdgeAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		fid := cctx.String("fid")
		url := cctx.String("url")
		log.Infof("fid:%s, url:%s", fid, url)

		seed := time.Now().UnixNano()
		req := API.ReqVerify{EdgeURL: "", Seed: seed, FIDs: []string{"0"}, Duration: 10}
		err = api.DoVerify(context.Background(), req, url)

		log.Infof("DoVerify success %v", err)
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

		result, err := api.DeleteBlocks(context.Background(), []string{cid})
		if err != nil {
			return err
		}

		if len(result.List) > 0 {
			log.Infof("delete block %s failed %v", cid, result.List[0].ErrMsg)
			return nil
		}

		log.Infof("delete block %s success", cid)
		return nil
	},
}

var VerfyDataCmd = &cli.Command{
	Name:  "verifydata",
	Usage: "verify data",
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
		req := make([]API.ReqVerify, 0)
		seed := time.Now().UnixNano()
		varify := API.ReqVerify{EdgeURL: url, Seed: seed, FIDs: []string{"0"}, Duration: 10}
		req = append(req, varify)

		err = api.VerifyData(ctx, req)
		if err != nil {
			fmt.Println("err", err)
			return err
		}

		// fmt.Println("verify data cid:", cid)
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

var UnlimitRateCmd = &cli.Command{
	Name:  "unlimit",
	Usage: "unlimit rate",
	Flags: []cli.Flag{},
	Action: func(cctx *cli.Context) error {
		api, closer, err := GetEdgeAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := ReqContext(cctx)
		err = api.UnlimitDownloadSpeed(ctx)
		if err != nil {
			fmt.Printf("Unlimit speed failed:%v", err)
			return err
		}
		fmt.Printf("Unlimit speed success")
		return nil
	},
}

var GenerateTokenCmd = &cli.Command{
	Name:  "generatetk",
	Usage: "generate token",
	Flags: []cli.Flag{},
	Action: func(cctx *cli.Context) error {
		api, closer, err := GetEdgeAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := ReqContext(cctx)
		tk, err := api.GenerateDownloadToken(ctx)
		if err != nil {
			fmt.Printf("Unlimit speed failed:%v", err)
			return err
		}

		fmt.Printf("Generate token success %s", tk)
		return nil
	},
}
