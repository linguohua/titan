package cli

import (
	"fmt"
	"time"

	"github.com/linguohua/titan/api"
	API "github.com/linguohua/titan/api"
	"github.com/urfave/cli/v2"
)

var EdgeCmds = []*cli.Command{
	DeviceInfoCmd,
	CacheBlockCmd,
	DeleteBlockCmd,
	ValidateBlockCmd,
	LimitRateCmd,
	CacheStatCmd,
	// StoreKeyCmd,
	DeleteAllBlocksCmd,
	testSyncCmd,
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
		fmt.Printf("device systemVersion: %s \n", v.SystemVersion)
		fmt.Printf("device DiskUsage: %f \n", v.DiskUsage)
		fmt.Printf("device disk space: %f \n", v.DiskSpace)
		fmt.Printf("device fstype: %s \n", v.IoSystem)
		fmt.Printf("device mac: %v \n", v.MacLocation)
		fmt.Printf("device download bandwidth: %v \n", v.BandwidthDown)
		fmt.Printf("device upload bandwidth: %v \n", v.BandwidthUp)
		fmt.Printf("device cpu percent: %v \n", v.CpuUsage)

		return nil
	},
}

var CacheBlockCmd = &cli.Command{
	Name:  "cache-file",
	Usage: "cache carfile",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "cid",
			Usage: "carfile cid",
			Value: "",
		},
		&cli.StringFlag{
			Name:  "source",
			Usage: "download source",
			Value: "",
		},

		&cli.StringFlag{
			Name:  "token",
			Usage: "download token",
			Value: "",
		},
	},
	Action: func(cctx *cli.Context) error {
		fmt.Println("start cache data...")
		adgeAPI, closer, err := GetEdgeAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		carfileCID := cctx.String("cid")
		candidateURL := cctx.String("source")
		candidateToken := cctx.String("token")
		ctx := ReqContext(cctx)

		source := api.DowloadSource{CandidateURL: candidateURL, CandidateToken: candidateToken}

		_, err = adgeAPI.CacheCarfile(ctx, carfileCID, []*api.DowloadSource{&source})
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
		// api, closer, err := GetEdgeAPI(cctx)
		// if err != nil {
		// 	return err
		// }
		// defer closer()

		// cid := cctx.String("cid")

		// results, err := api.AnnounceBlocksWasDelete(context.Background(), []string{cid})
		// if err != nil {
		// 	return err
		// }

		// if len(results) > 0 {
		// 	log.Infof("delete block %s failed %v", cid, results[0].ErrMsg)
		// 	return nil
		// }

		// log.Infof("delete block %s success", cid)
		return nil
	},
}

var ValidateBlockCmd = &cli.Command{
	Name:  "validate",
	Usage: "validate data",
	Flags: []cli.Flag{
		&cli.IntFlag{
			Name:  "max-fid",
			Usage: "max fid",
			Value: 0,
		},
		&cli.IntFlag{
			Name:  "node-type",
			Usage: "edge=1,candidate=2",
			Value: 1,
		},
		&cli.Int64Flag{
			Name:  "round-id",
			Usage: "round id",
			Value: 1,
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

		maxFid := cctx.Int("max-fid")
		nodeType := cctx.Int("node-type")
		url := cctx.String("edge-url")
		roundID := cctx.Int64("round-id")
		fmt.Printf("maxFid:%d,url:%s", maxFid, url)
		ctx := ReqContext(cctx)
		// TODO: print more useful things
		req := make([]API.ReqValidate, 0)
		seed := time.Now().UnixNano()
		varify := API.ReqValidate{NodeURL: url, RandomSeed: seed, Duration: 10, RoundID: roundID, NodeType: nodeType}
		req = append(req, varify)

		err = api.ValidateNodes(ctx, req)
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

var CacheStatCmd = &cli.Command{
	Name:  "stat",
	Usage: "cache stat",
	Flags: []cli.Flag{},
	Action: func(cctx *cli.Context) error {
		// api, closer, err := GetEdgeAPI(cctx)
		// if err != nil {
		// 	return err
		// }
		// defer closer()

		// ctx := ReqContext(cctx)
		// stat, err := api.QueryCacheStat(ctx)
		// if err != nil {
		// 	fmt.Printf("Unlimit speed failed:%v", err)
		// 	return err
		// }

		// fmt.Printf("Cache block count %d, Wait cache count %d, Caching count %d", stat.CacheBlockCount, stat.WaitCacheBlockNum, stat.DoingCacheBlockNum)
		return nil
	},
}

var DeleteAllBlocksCmd = &cli.Command{
	Name:  "flush",
	Usage: "delete all block",
	Flags: []cli.Flag{},
	Action: func(cctx *cli.Context) error {
		// api, closer, err := GetEdgeAPI(cctx)
		// if err != nil {
		// 	return err
		// }
		// defer closer()

		// ctx := ReqContext(cctx)
		// err = api.DeleteAllBlocks(ctx)
		return nil
	},
}

var testSyncCmd = &cli.Command{
	Name:  "sync",
	Usage: "data sync",
	Subcommands: []*cli.Command{
		checksum,
		checksumInRange,
		scrubBlocks,
	},
}

var checksum = &cli.Command{
	Name:  "checksum",
	Usage: "get all check sums",
	Flags: []cli.Flag{},
	Action: func(cctx *cli.Context) error {
		api, closer, err := GetEdgeAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := ReqContext(cctx)
		rsp, err := api.GetAllChecksums(ctx, 10)
		if err != nil {
			return err
		}

		for _, checksum := range rsp.Checksums {
			fmt.Printf("Range %d ~ %d block count %d hash %s\n", checksum.StartFid, checksum.EndFid, checksum.BlockCount, checksum.Hash)
		}
		return nil
	},
}

var checksumInRange = &cli.Command{
	Name:  "range",
	Usage: "get all check sum in range",
	Flags: []cli.Flag{
		&cli.IntFlag{
			Name:  "start-fid",
			Usage: "start fid",
			Value: 0,
		},
		&cli.IntFlag{
			Name:  "end-fid",
			Usage: "end fid",
			Value: 1000,
		},
	},
	Action: func(cctx *cli.Context) error {
		edgeApi, closer, err := GetEdgeAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := ReqContext(cctx)

		startFid := cctx.Int("start-fid")
		endFid := cctx.Int("end-fid")

		req := api.ReqChecksumInRange{StartFid: startFid, EndFid: endFid, MaxGroupNum: 10}
		rsp, err := edgeApi.GetChecksumsInRange(ctx, req)
		if err != nil {
			return err
		}

		for _, checksum := range rsp.Checksums {
			fmt.Printf("Range %d ~ %d block count %d hash %s\n", checksum.StartFid, checksum.EndFid, checksum.BlockCount, checksum.Hash)
		}
		return err
	},
}

var scrubBlocks = &cli.Command{
	Name:  "scrub",
	Usage: "scrub block in range",
	Flags: []cli.Flag{
		&cli.IntFlag{
			Name:  "start-fid",
			Usage: "start fid",
			Value: 0,
		},
		&cli.IntFlag{
			Name:  "end-fid",
			Usage: "end fid",
			Value: 1000,
		},
	},
	Action: func(cctx *cli.Context) error {
		edgeAPI, closer, err := GetEdgeAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := ReqContext(cctx)
		startFid := cctx.Int("start-fid")
		endFid := cctx.Int("end-fid")

		blocks := map[int]string{
			1: "QmTYG4itLJrAC3R9EhpVBb1Hfs65NgMffDDSxP4Pv1KoiX",
			2: "QmRKw91PPKVbKcHK6HAiYE9ouBJtFsDfo16XSQaDZ9H6WL",
			3: "QmeHWv892UWYzDvnYpCtJeR5qKs64G2d6aXh2yXg1WvZdC",
			4: "QmapeQSKYWpNkdkdxhrYsFGca3fLG6FDZFKiojhSxXtYJD",
			5: "Qme6FgRySo1bHDSwBt1jQsz4VaYLTr4Mm7nBrwZw11jjbN",
			// "6": "9b4c0675e1bd4a6ef678d02310c11f28",
		}
		req := api.ScrubBlocks{Blocks: blocks, StartFid: startFid, EndFid: endFid}
		err = edgeAPI.ScrubBlocks(ctx, req)
		return err
	},
}
