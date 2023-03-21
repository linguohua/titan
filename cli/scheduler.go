package cli

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/linguohua/titan/api"
	"github.com/urfave/cli/v2"
)

// SchedulerCmds Scheduler Cmds
var SchedulerCmds = []*cli.Command{
	WithCategory("node", nodeCmd),
	WithCategory("carfile", carfileCmd),
	// validator
	startElectionCmd,
	startValidateCmd,
	// other
	edgeUpdaterCmd,
}

var (
	secretFlag = &cli.StringFlag{
		Name:  "secret",
		Usage: "node secret",
		Value: "",
	}

	nodeIDFlag = &cli.StringFlag{
		Name:  "node-id",
		Usage: "node id",
		Value: "",
	}

	cidsPathFlag = &cli.StringFlag{
		Name:  "cids-file-path",
		Usage: "blocks cid file path",
		Value: "",
	}

	cidsFlag = &cli.StringFlag{
		Name:  "cids",
		Usage: "blocks cid",
		Value: "",
	}

	cidFlag = &cli.StringFlag{
		Name:  "cid",
		Usage: "specify the cid of a carfile",
		Value: "",
	}

	ipFlag = &cli.StringFlag{
		Name:  "ip",
		Usage: "ip",
		Value: "",
	}

	replicaCountFlag = &cli.IntFlag{
		Name:        "replica-count",
		Usage:       "Number of replica cached to nodes",
		Value:       2,
		DefaultText: "2",
	}

	nodeTypeFlag = &cli.IntFlag{
		Name:  "node-type",
		Usage: "node type 0:ALL 1:Edge 2:Candidate 3:Scheduler 4:Scheduler",
		Value: 0,
	}

	countFlag = &cli.IntFlag{
		Name:  "count",
		Usage: "count",
		Value: 0,
	}

	pageFlag = &cli.IntFlag{
		Name:        "page",
		Usage:       "the numbering of pages",
		Value:       1,
		DefaultText: "1",
	}

	expirationDateFlag = &cli.StringFlag{
		Name:        "expiration-date",
		Usage:       "Set the carfile expiration, format with '2006-1-2 15:04:05' layout.",
		Value:       "",
		DefaultText: "now + 7 days",
	}

	dateFlag = &cli.StringFlag{
		Name:  "date-time",
		Usage: "date time (2006-1-2 15:04:05)",
		Value: "",
	}

	portFlag = &cli.StringFlag{
		Name:  "port",
		Usage: "port",
		Value: "",
	}
)

var setNodePortCmd = &cli.Command{
	Name:  "set-node-port",
	Usage: "set the node port",
	Flags: []cli.Flag{
		nodeIDFlag,
		portFlag,
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		nodeID := cctx.String("node-id")
		port := cctx.String("port")

		ctx := ReqContext(cctx)

		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		return schedulerAPI.SetNodePort(ctx, nodeID, port)
	},
}

var startValidateCmd = &cli.Command{
	Name:  "start-validate",
	Usage: "validate edges node",

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

		return schedulerAPI.StartOnceValidate(ctx)
	},
}

var startElectionCmd = &cli.Command{
	Name:  "start-election",
	Usage: "Start election validator",

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

		return schedulerAPI.StartOnceElection(ctx)
	},
}

var edgeUpdaterCmd = &cli.Command{
	Name:  "edge-updater",
	Usage: "get edge update info or set edge update info",
	Subcommands: []*cli.Command{
		edgeUpdateInfoCmd,
		setEdgeUpdateInfoCmd,
		DeleteEdgeUpdateInfoCmd,
	},
}

var DeleteEdgeUpdateInfoCmd = &cli.Command{
	Name:  "delete",
	Usage: "delete edge update info",
	Flags: []cli.Flag{
		&cli.IntFlag{
			Name:  "node-type",
			Usage: "node type: edge 1, update 6",
			Value: 1,
		},
	},
	Action: func(cctx *cli.Context) error {
		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		nodeType := cctx.Int("node-type")
		err = schedulerAPI.DeleteEdgeUpdateInfo(ctx, nodeType)
		if err != nil {
			return err
		}
		return nil
	},
}

var edgeUpdateInfoCmd = &cli.Command{
	Name:  "get",
	Usage: "get edge update info",
	Action: func(cctx *cli.Context) error {
		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		updateInfos, err := schedulerAPI.EdgeUpdateInfos(ctx)
		if err != nil {
			return err
		}

		if len(updateInfos) == 0 {
			fmt.Printf("No update info exist\n")
			return nil
		}

		for k, updateInfo := range updateInfos {
			fmt.Printf("AppName:%s\n", updateInfo.AppName)
			fmt.Printf("NodeType:%d\n", k)
			fmt.Printf("Hash:%s\n", updateInfo.Hash)
			fmt.Printf("Version:%s\n", updateInfo.Version)
			fmt.Printf("DownloadURL:%s\n", updateInfo.DownloadURL)
			fmt.Println()
		}

		return nil
	},
}

var setEdgeUpdateInfoCmd = &cli.Command{
	Name:  "set",
	Usage: "set edge update info",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "version",
			Usage: "latest node app version",
			Value: "0.0.1",
		},
		&cli.StringFlag{
			Name:  "hash",
			Usage: "latest node app hash",
			Value: "",
		},
		&cli.StringFlag{
			Name:  "download-url",
			Usage: "latest node app download url",
			Value: "",
		},
		&cli.StringFlag{
			Name:  "app-name",
			Usage: "app name",
			Value: "",
		},
		&cli.IntFlag{
			Name:  "node-type",
			Usage: "node type: 1 is edge, 6 is update",
			Value: 1,
		},
	},

	Action: func(cctx *cli.Context) error {
		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		versionStr := cctx.String("version")
		hash := cctx.String("hash")
		downloadURL := cctx.String("download-url")
		nodeType := cctx.Int("node-type")
		appName := cctx.String("app-name")

		version, err := newVersion(versionStr)
		if err != nil {
			return err
		}

		updateInfo := &api.EdgeUpdateInfo{AppName: appName, NodeType: nodeType, Version: version, Hash: hash, DownloadURL: downloadURL}
		err = schedulerAPI.SetEdgeUpdateInfo(ctx, updateInfo)
		if err != nil {
			return err
		}
		return nil
	},
}

func newVersion(version string) (api.Version, error) {
	stringSplit := strings.Split(version, ".")
	if len(stringSplit) != 3 {
		return api.Version(0), fmt.Errorf("parse version error")
	}

	// major, minor, patch
	major, err := strconv.Atoi(stringSplit[0])
	if err != nil {
		return api.Version(0), fmt.Errorf("parse version major error:%s", err)
	}

	minor, err := strconv.Atoi(stringSplit[1])
	if err != nil {
		return api.Version(0), fmt.Errorf("parse version minor error:%s", err)
	}

	patch, err := strconv.Atoi(stringSplit[2])
	if err != nil {
		return api.Version(0), fmt.Errorf("parse version patch error:%s", err)
	}

	return api.Version(uint32(major)<<16 | uint32(minor)<<8 | uint32(patch)), nil
}
