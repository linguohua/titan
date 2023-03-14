package cli

import (
	"fmt"
	"github.com/docker/go-units"
	"github.com/fatih/color"
	"github.com/linguohua/titan/lib/tablewriter"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/linguohua/titan/api/types"
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
		carfilesStatusCmd,
		removeCarfileCmd,
		resetExpirationCmd,
		resetReplicaCacheCountCmd,
		contiuneUndoneCarfileCmd,
	},
}

var contiuneUndoneCarfileCmd = &cli.Command{
	Name:      "execute-incomplete",
	Usage:     "Continue to execute the incomplete storage task",
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

		return schedulerAPI.RecacheCarfiles(ctx, hashs)
	},
}

var resetReplicaCacheCountCmd = &cli.Command{
	Name:  "reset-candidate-replica",
	Usage: "Reset Number of candidate node replica per storage",
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

		return schedulerAPI.ResetCandidateReplicaCount(ctx, count)
	},
}

var resetExpirationCmd = &cli.Command{
	Name:  "reset-expiration",
	Usage: "Reset storage expiration",
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

		err = schedulerAPI.ResetCarfileExpiration(ctx, cardileCid, time)
		if err != nil {
			return err
		}

		return nil
	},
}

var removeCarfileCmd = &cli.Command{
	Name:  "remove",
	Usage: "Remove the storage record",
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
	Usage: "Show the storage info",
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

		info, err := schedulerAPI.CarfileRecord(ctx, cid)
		if err != nil {
			return err
		}

		fmt.Printf("Data CID: %s ,Total Size:%f MB ,Total Blocks:%d ,EdgeReplica:%d/%d ,Expiration Time:%s\n", info.CarfileCID, float64(info.TotalSize)/(1024*1024), info.TotalBlocks, info.EdgeReplica, info.NeedEdgeReplica, info.Expiration.Format("2006-01-02 15:04:05"))
		for _, cache := range info.ReplicaInfos {
			fmt.Printf("NodeID: %s ,Status:%s ,Done Size:%f MB ,Done Blocks:%d ,IsCandidateCache:%v \n",
				cache.NodeID, cache.Status.String(), float64(cache.DoneSize)/(1024*1024), cache.DoneBlocks, cache.IsCandidate)
		}

		return nil
	},
}

var cacheCarfileCmd = &cli.Command{
	Name:  "cache",
	Usage: "Scheduling nodes cache storage",
	Flags: []cli.Flag{
		cidFlag,
		replicaCountFlag,
		expirationDateFlag,
	},
	Action: func(cctx *cli.Context) error {
		cid := cctx.String("cid")
		replicaCount := cctx.Int64("replica-count")
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

		info := &types.CacheCarfileInfo{CarfileCid: cid}

		if date == "" {
			date = time.Now().Add(time.Duration(7*24) * time.Hour).Format("2006-1-2 15:04:05")
		}

		eTime, err := time.ParseInLocation("2006-1-2 15:04:05", date, time.Local)
		if err != nil {
			return xerrors.Errorf("expiration date err:%s", err.Error())
		}

		info.Expiration = eTime
		info.Replicas = replicaCount

		err = schedulerAPI.CacheCarfiles(ctx, info)
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
		&cli.BoolFlag{
			Name:  "downloading",
			Usage: "carfiles in downloading states",
			Value: false,
		},
		&cli.BoolFlag{
			Name:  "processes",
			Usage: "carfiles in process",
			Value: false,
		},
		&cli.BoolFlag{
			Name:  "failed",
			Usage: "carfiles in failed states",
			Value: false,
		},
		&cli.BoolFlag{
			Name:  "restart",
			Usage: "restart failed carfiles",
			Value: false,
		},
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

		restart := false

		status := types.CacheStatusUnknown
		if cctx.Bool("downloading") {
			status = types.CacheStatusDownloading
		}
		if cctx.Bool("failed") {
			status = types.CacheStatusFailed
			restart = cctx.Bool("restart")
		}

		tw := tablewriter.New(
			tablewriter.Col("CID"),
			tablewriter.Col("State"),
			tablewriter.Col("Blocks"),
			tablewriter.Col("Size"),
			tablewriter.Col("CreateTime"),
			tablewriter.Col("Expiration"),
			tablewriter.NewLineCol("Processes"),
		)

		info, err := schedulerAPI.CarfileRecords(ctx, page, status)
		if err != nil {
			return err
		}

		for w := 0; w < len(info.CarfileRecords); w++ {
			carfile := info.CarfileRecords[w]
			m := map[string]interface{}{
				"CID":        carfile.CarfileCID,
				"State":      colorState(carfile.State),
				"Blocks":     carfile.TotalBlocks,
				"Size":       units.BytesSize(float64(carfile.TotalSize)),
				"CreateTime": carfile.CreateTime,
				"Expiration": carfile.Expiration,
			}

			sort.Slice(carfile.ReplicaInfos, func(i, j int) bool {
				return carfile.ReplicaInfos[i].NodeID < carfile.ReplicaInfos[j].NodeID
			})

			if cctx.Bool("processes") {
				var processes = "\n"
				for j := 0; j < len(carfile.ReplicaInfos); j++ {
					cache := carfile.ReplicaInfos[j]
					status := cache.Status.String()
					switch cache.Status {
					case types.CacheStatusSucceeded:
						status = color.GreenString(status)
					case types.CacheStatusDownloading:
						status = color.YellowString(status)
					case types.CacheStatusFailed:
						status = color.RedString(status)
					}
					processes += fmt.Sprintf("\t%s(%s): %s\n", cache.NodeID, edgeOrCandidate(cache.IsCandidate), status)
				}
				m["Processes"] = processes
			}

			tw.Write(m)
		}

		if err := tw.Flush(os.Stdout); err != nil {
			return err
		}

		fmt.Printf("\nTotal:%d\t\t%d/%d \n", info.Cids, info.Page, info.TotalPage)

		if restart {
			if info.CarfileRecords == nil || len(info.CarfileRecords) < 1 {
				return nil
			}

			return schedulerAPI.RestartFailedCarfiles(ctx, info.CarfileRecords)
		}

		return nil
	},
}

func edgeOrCandidate(isCandidate bool) string {
	if isCandidate {
		return "candidate"
	}
	return "edge"
}

func colorState(state string) string {
	if strings.Contains(state, "Failed") {
		return color.RedString(state)
	} else if strings.Contains(state, "Finalize") {
		return color.GreenString(state)
	} else {
		return color.YellowString(state)
	}
}

var carfilesStatusCmd = &cli.Command{
	Name:      "status",
	Usage:     "Get the cache status of a carfiles by its id",
	ArgsUsage: "<carfileCID>",
	Flags:     []cli.Flag{},
	Action: func(cctx *cli.Context) error {
		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		if cctx.NArg() != 1 {
			return IncorrectNumArgs(cctx)
		}

		cid := cctx.Args().First()

		hash, err := cidutil.CIDString2HashString(cid)
		if err != nil {
			return xerrors.Errorf("%s cid to hash err:%v", cid, err)
		}

		status, err := schedulerAPI.CarfileStatus(ctx, types.CarfileHash(hash))
		if err != nil {
			return err
		}

		fmt.Printf("CarfileCID:\t%s\n", cid)
		fmt.Printf("Status:\t\t%s\n", status.State)
		fmt.Printf("CarfileHash:\t%s\n", status.CarfileHash)
		fmt.Printf("Replicas:\t%d\n", status.NeedEdgeReplica)
		fmt.Printf("Size:\t%d\n", status.TotalSize)
		fmt.Printf("Blocks:\t%d\n", status.TotalBlocks)
		fmt.Printf("Expiration:\t\t%s\n", status.Expiration)

		return nil
	},
}
