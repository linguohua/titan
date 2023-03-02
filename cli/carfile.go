package cli

import (
	"fmt"
	"sort"
	"time"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/cidutil"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

var carfileCmd = &cli.Command{
	Name:  "carfile",
	Usage: "Manage carfile",
	Subcommands: []*cli.Command{
		listCarfilesCmd,
		cacheCarfileCmd,
		showCarfileInfoCmd,
		removeCarfileCmd,
		removeReplicaCmd,
		resetExpirationTimeCmd,
		resetReplicaCacheCountCmd,
		contiuneUndoneCarfileCmd,
	},
}

var contiuneUndoneCarfileCmd = &cli.Command{
	Name:      "execute-incomplete",
	Usage:     "Continue to execute the incomplete carfile task",
	ArgsUsage: "[cid1 cid2 ...]",

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		var hashs []string
		for _, cid := range cctx.Args().Slice() {
			hash, err := cidutil.CIDString2HashString(cid)
			if err != nil {
				log.Errorf("%s CIDString2HashString err:%s", cid, err.Error())
				continue
			}

			hashs = append(hashs, hash)
		}

		return schedulerAPI.ExecuteUndoneCarfilesTask(ctx, hashs)
	},
}

var resetReplicaCacheCountCmd = &cli.Command{
	Name:  "reset-candidate-replica-count",
	Usage: "Reset Number of candidate node replica per carfile",
	Flags: []cli.Flag{
		countFlag,
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		count := cctx.Int("count")

		ctx := ReqContext(cctx)

		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		return schedulerAPI.ResetReplicaCacheCount(ctx, count)
	},
}

var resetExpirationTimeCmd = &cli.Command{
	Name:  "reset-expiration-time",
	Usage: "Reset carfile expiration time",
	Flags: []cli.Flag{
		cidFlag,
		dateFlag,
	},
	Action: func(cctx *cli.Context) error {
		cardileCid := cctx.String("cid")
		dateTime := cctx.String("date-time")

		ctx := ReqContext(cctx)

		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		time, err := time.ParseInLocation("2006-1-2 15:04:05", dateTime, time.Local)
		if err != nil {
			return xerrors.Errorf("date time err:%s", err.Error())
		}

		err = schedulerAPI.ResetCacheExpirationTime(ctx, cardileCid, time)
		if err != nil {
			return err
		}

		return nil
	},
}

var removeReplicaCmd = &cli.Command{
	Name:  "remove-replica",
	Usage: "Remove the carfile replica",
	Flags: []cli.Flag{
		deviceIDFlag,
		cidFlag,
	},
	Action: func(cctx *cli.Context) error {
		deviceID := cctx.String("device-id")
		if deviceID == "" {
			return xerrors.New("device-id is nil")
		}

		cid := cctx.String("cid")

		ctx := ReqContext(cctx)

		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		return schedulerAPI.RemoveCache(ctx, cid, deviceID)
	},
}

var removeCarfileCmd = &cli.Command{
	Name:  "remove",
	Usage: "Remove the carfile record",
	Flags: []cli.Flag{
		cidFlag,
	},
	Action: func(cctx *cli.Context) error {
		cid := cctx.String("cid")

		ctx := ReqContext(cctx)

		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		return schedulerAPI.RemoveCarfile(ctx, cid)
	},
}

var showCarfileInfoCmd = &cli.Command{
	Name:  "info",
	Usage: "Show the carfile info",
	Flags: []cli.Flag{
		cidFlag,
	},
	Action: func(cctx *cli.Context) error {
		cid := cctx.String("cid")

		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		info, err := schedulerAPI.GetCarfileRecordInfo(ctx, cid)
		if err != nil {
			return err
		}

		fmt.Printf("Data CID: %s ,Total Size:%f MB ,Total Blocks:%d ,EdgeReplica:%d/%d ,Expiration Time:%s\n", info.CarfileCid, float64(info.TotalSize)/(1024*1024), info.TotalBlocks, info.EdgeReplica, info.Replica, info.ExpirationTime.Format("2006-01-02 15:04:05"))
		for _, cache := range info.ReplicaInfos {
			fmt.Printf("DeviceID: %s ,Status:%s ,Done Size:%f MB ,Done Blocks:%d ,IsCandidateCache:%v \n",
				cache.DeviceID, cache.Status.String(), float64(cache.DoneSize)/(1024*1024), cache.DoneBlocks, cache.IsCandidate)
		}

		if info.ResultInfo != nil {
			fmt.Printf("Result Msg: %s , edge summary: %s \n", info.ResultInfo.ErrMsg, info.ResultInfo.EdgeNodeCacheSummary)
			if info.ResultInfo.NodeErrs != nil {
				for nodeID, msg := range info.ResultInfo.NodeErrs {
					fmt.Printf("%s,err:%s \n", nodeID, msg)
				}
			}
		}

		return nil
	},
}

var cacheCarfileCmd = &cli.Command{
	Name:  "cache",
	Usage: "Scheduling nodes cache carfile",
	Flags: []cli.Flag{
		cidFlag,
		replicaCountFlag,
		expirationDateFlag,
		deviceIDFlag,
	},
	Action: func(cctx *cli.Context) error {
		cid := cctx.String("cid")
		replicaCount := cctx.Int("replica-count")
		deviceID := cctx.String("device-id")
		date := cctx.String("expiration-date")

		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		if cid == "" {
			return xerrors.New("cid is nil")
		}

		info := &api.CacheCarfileInfo{CarfileCid: cid}
		if deviceID != "" {
			info.DeviceID = deviceID
		} else {
			if date == "" {
				date = time.Now().Add(time.Duration(7*24) * time.Hour).Format("2006-1-2 15:04:05")
			}

			eTime, err := time.ParseInLocation("2006-1-2 15:04:05", date, time.Local)
			if err != nil {
				return xerrors.Errorf("expiration date err:%s", err.Error())
			}

			info.ExpirationTime = eTime
			info.Replicas = replicaCount
		}

		err = schedulerAPI.CacheCarfile(ctx, info)
		if err != nil {
			return err
		}

		return nil
	},
}

var listCarfilesCmd = &cli.Command{
	Name:  "list",
	Usage: "List carfiles",
	Flags: []cli.Flag{
		pageFlag,
		downloadingFlag,
	},
	Action: func(cctx *cli.Context) error {
		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		page := cctx.Int("page")
		if page < 1 {
			return xerrors.New("page need greater than 1")
		}

		isDownloading := cctx.Bool("downloading")
		if isDownloading {
			infos, err := schedulerAPI.GetDownloadingCarfileRecords(ctx)
			if err != nil {
				return err
			}

			sort.Slice(infos, func(i, j int) bool {
				return infos[i].CarfileCid < infos[j].CarfileCid
			})

			for w := 0; w < len(infos); w++ {
				info := infos[w]

				fmt.Printf("\nData CID: %s ,Total Size:%f MB ,Total Blocks:%d \n", info.CarfileCid, float64(info.TotalSize)/(1024*1024), info.TotalBlocks)

				sort.Slice(info.ReplicaInfos, func(i, j int) bool {
					return info.ReplicaInfos[i].DeviceID < info.ReplicaInfos[j].DeviceID
				})

				for j := 0; j < len(info.ReplicaInfos); j++ {
					cache := info.ReplicaInfos[j]
					fmt.Printf("DeviceID: %s , Status:%s ,Done Size:%f MB ,Done Blocks:%d ,IsCandidateCache:%v \n",
						cache.DeviceID, cache.Status.String(), float64(cache.DoneSize)/(1024*1024), cache.DoneBlocks, cache.IsCandidate)
				}
			}

			return nil
		}

		info, err := schedulerAPI.ListCarfileRecords(ctx, page)
		if err != nil {
			return err
		}

		for _, carfile := range info.CarfileRecords {
			fmt.Printf("%s ,EdgeReplica: %d/%d ,Blocks:%d ,Expiration Time:%s \n", carfile.CarfileCid, carfile.EdgeReplica, carfile.Replica, carfile.TotalBlocks, carfile.ExpirationTime.Format("2006-01-02 15:04:05"))
		}
		fmt.Printf("total:%d            %d/%d \n", info.Cids, info.Page, info.TotalPage)

		return nil
	},
}
