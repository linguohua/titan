package cli

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/docker/go-units"
	"github.com/fatih/color"
	"github.com/linguohua/titan/lib/tablewriter"

	"github.com/linguohua/titan/api/types"
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
		resetExpirationCmd,
		resetReplicaCacheCountCmd,
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

		status := types.CacheStatusUnknown
		if cctx.Bool("downloading") {
			status = types.CacheStatusDownloading
		}
		if cctx.Bool("failed") {
			status = types.CacheStatusFailed
		}

		restart := cctx.Bool("restart")
		if restart && !cctx.Bool("failed") {
			log.Error("only --failed can be restarted")
			return nil
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
				processes := "\n"
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

		if !restart {
			fmt.Printf("\nTotal:%d\t\t%d/%d \n", info.Cids, info.Page, info.TotalPage)
			return tw.Flush(os.Stdout)
		}

		var hashes []types.CarfileHash
		for _, carfile := range info.CarfileRecords {
			if strings.Contains(carfile.State, "Failed") {
				hashes = append(hashes, types.CarfileHash(carfile.CarfileHash))
			}
		}
		return schedulerAPI.RestartFailedCarfiles(ctx, hashes)
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
