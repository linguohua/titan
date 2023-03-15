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

var (
	defaultExpireDays     = 7
	defaultExpiration     = time.Duration(defaultExpireDays) * time.Hour * 24
	defaultDatetimeLayout = "2006-01-02 15:04:05"
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
	Usage: "Reset the number of candidate node replica per storage",
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
	Usage: "Reset the storage expiration",
	Flags: []cli.Flag{
		cidFlag,
		dateFlag,
	},
	Action: func(cctx *cli.Context) error {
		carfileCid := cctx.String("cid")
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

		err = schedulerAPI.ResetCarfileExpiration(ctx, carfileCid, time)
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

		fmt.Printf("CID:\t%s\n", info.CarfileCID)
		fmt.Printf("Hash:\t%s\n", info.CarfileHash)
		fmt.Printf("State:\t%s\n", colorState(info.State))
		fmt.Printf("Blocks:\t%d\n", info.TotalBlocks)
		fmt.Printf("Size:\t%s\n", units.BytesSize(float64(info.TotalSize)))
		fmt.Printf("NeedEdgeReplica:\t%d\n", info.NeedEdgeReplica)
		fmt.Printf("Expiration:\t%v\n", info.Expiration.Format(defaultDatetimeLayout))

		fmt.Printf("--------\nProcesses:\n")
		for _, cache := range info.ReplicaInfos {
			fmt.Printf("%s(%s): %s\t%d/%d\t%s/%s\n", cache.NodeID, edgeOrCandidate(cache.IsCandidate), colorState(cache.Status.String()),
				cache.DoneBlocks, info.TotalBlocks, units.BytesSize(float64(cache.DoneSize)), units.BytesSize(float64(info.TotalSize)))
		}

		return nil
	},
}

var cacheCarfileCmd = &cli.Command{
	Name:  "cache",
	Usage: "publish cache tasks to nodes",
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
			date = time.Now().Add(defaultExpiration).Format(defaultDatetimeLayout)
		}

		eTime, err := time.ParseInLocation(defaultDatetimeLayout, date, time.Local)
		if err != nil {
			return xerrors.Errorf("parse expiration err:%s", err.Error())
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
			Usage: "only show the downloading carfiles",
			Value: false,
		},
		&cli.BoolFlag{
			Name:  "processes",
			Usage: "show the carfiles processes",
			Value: false,
		},
		&cli.BoolFlag{
			Name:  "failed",
			Usage: "only show the failed state carfiles",
			Value: false,
		},
		&cli.BoolFlag{
			Name:  "restart",
			Usage: "restart the failed carfiles, only apply for failed carfile state",
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
			return xerrors.New("the page must greater than 1")
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
				"CreateTime": carfile.CreateTime.Format(defaultDatetimeLayout),
				"Expiration": carfile.Expiration.Format(defaultDatetimeLayout),
			}

			sort.Slice(carfile.ReplicaInfos, func(i, j int) bool {
				return carfile.ReplicaInfos[i].NodeID < carfile.ReplicaInfos[j].NodeID
			})

			if cctx.Bool("processes") {
				processes := "\n"
				for j := 0; j < len(carfile.ReplicaInfos); j++ {
					cache := carfile.ReplicaInfos[j]
					status := colorState(cache.Status.String())
					processes += fmt.Sprintf("\t%s(%s): %s\n", cache.NodeID, edgeOrCandidate(cache.IsCandidate), status)
				}
				m["Processes"] = processes
			}

			tw.Write(m)
		}

		if !restart {
			tw.Flush(os.Stdout)
			fmt.Printf("\nTotal:%d\t\t%d/%d \n", info.Cids, info.Page, info.TotalPage)
			return nil
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
	} else if strings.Contains(state, "Finalize") || strings.Contains(state, "Succeeded") {
		return color.GreenString(state)
	} else {
		return color.YellowString(state)
	}
}
