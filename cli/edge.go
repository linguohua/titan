package cli

import (
	"fmt"

	API "github.com/linguohua/titan/api"
	"github.com/urfave/cli/v2"
)

var EdgeCmds = []*cli.Command{
	DeviceIDCmd,
	CacheDataCmd,
	VerfyDataCmd,
}

var DeviceIDCmd = &cli.Command{
	Name:  "deviceid",
	Usage: "Print device ID",
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
		fmt.Println("device id: ", v)
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
		fid := cctx.String("fid")
		fmt.Println("cid:", cid)
		ctx := ReqContext(cctx)
		// TODO: print more useful things

		req := make([]API.ReqCacheData, 0)
		reqData := API.ReqCacheData{Cid: cid, ID: fid}
		req = append(req, reqData)
		err = api.CacheData(ctx, req)
		if err != nil {
			return err
		}

		fmt.Println("cache data success")
		return nil
	},
}
