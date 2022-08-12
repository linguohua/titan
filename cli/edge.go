package cli

import (
	"fmt"

	API "github.com/linguohua/titan/api"
	"github.com/urfave/cli/v2"
)

var EdgeCmds = []*cli.Command{
	DeviceInfoCmd,
	CacheDataCmd,
	VerfyDataCmd,
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
		// fid := cctx.String("fid")
		fmt.Println("cid:", cid)
		ctx := ReqContext(cctx)
		// TODO: print more useful things

		ids := []string{
			"QmeUqw4FY1wqnh2FMvuc2v8KAapE7fYwu2Up4qNwhZiRk7",
			"QmbkdBycNb9xv69Q68ZWqFNE6ZRe5KFMtoHtA9D3MGZPhH",
			"QmPDuq1nhSCYwJAaPYgby5hThSGdN1QkosPoFVKUDrP4Ro",
			"bafkreic6vrp57vbzhzmxzetlqu5kqzb4nfcb7nrhnycn6djngp6ka4eziq",
			"QmaP1ifdEt9K14sVVw7Vf2kcnuicxQmwQWDt5j7ShYXjsN",
			"QmR7hNanvqaW9iG6jdszzoDKdDsfru5jPpu5VfQT2723kS",
			"QmewmdJLBotQstj3tff4hAm9NjF1Jb9dbHDJo93PWqJ4Ka",
			// "QmetZk77NyVmS1fDrhMNHBFaSZwtb7KXVPVqSDgXaZivpD",
			"QmVqpiJqXTXEseJDbmsaCduFdFpnyzoSovKaHEzZmNoYD6",
			// "QmWVCvoVHRMevzyDK4TuYr1ZcMzZa22kEq5RaxjnyPQb1y",
		}

		req := make([]API.ReqCacheData, 0)
		for i, id := range ids {
			reqData := API.ReqCacheData{Cid: id, ID: fmt.Sprintf("%d", i)}
			req = append(req, reqData)
		}

		err = api.CacheData(ctx, req)
		if err != nil {
			return err
		}

		fmt.Println("cache data success")
		return nil
	},
}
