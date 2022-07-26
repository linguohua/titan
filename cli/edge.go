package cli

import (
	"fmt"

	"github.com/urfave/cli/v2"
)

var EdgeCmds = []*cli.Command{
	DeviceIDCmd,
	CacheData,
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

		v, err := api.DeviceID(ctx)
		if err != nil {
			return err
		}
		fmt.Println("device id: ", v)
		return nil
	},
}

var CacheData = &cli.Command{
	Name:  "cachedata",
	Usage: "cache block content",
	Flags: []cli.Flag{
		&cli.StringSliceFlag{
			Name:  "cid",
			Usage: "block cids",
			Value: &cli.StringSlice{},
		},
	},
	Action: func(cctx *cli.Context) error {
		api, closer, err := GetEdgeAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		cids := cctx.StringSlice("cid")
		fmt.Println("cids:", cids)
		ctx := ReqContext(cctx)
		// TODO: print more useful things

		err = api.CacheData(ctx, cids)
		if err != nil {
			return err
		}

		fmt.Println("cache data success")
		return nil
	},
}
