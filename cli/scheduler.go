package cli

import (
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/linguohua/titan/api"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

// SchedulerCmds Scheduler Cmds
var SchedulerCmds = []*cli.Command{
	cacheBlocksCmd,
	electionCmd,
	validateCmd,
	showOnlineNodeCmd,
	deleteBlocksCmd,
	// initDeviceIDsCmd,
	cachingBlocksCmd,
	cacheStatCmd,
	getDownloadInfoCmd,
	cacheCarFileCmd,
	showDataInfoCmd,
	registerNodeCmd,
}

var (
	// schedulerURLFlag = &cli.StringFlag{
	// 	Name:  "scheduler-url",
	// 	Usage: "host address and port the worker api will listen on",
	// 	Value: "http://127.0.0.1:3456/rpc/v0",
	// }

	deviceIDFlag = &cli.StringFlag{
		Name:  "device-id",
		Usage: "cache node device id",
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
		Usage: "cid",
		Value: "",
	}

	ipFlag = &cli.StringFlag{
		Name:  "ip",
		Usage: "ip",
		Value: "",
	}

	reliabilityFlag = &cli.StringFlag{
		Name:  "reliability",
		Usage: "cache reliability",
		Value: "0",
	}

	nodeTypeFlag = &cli.IntFlag{
		Name:  "node-type",
		Usage: "node type 1:Edge 2:Candidate 3:Scheduler",
		Value: 0,
	}
)

var registerNodeCmd = &cli.Command{
	Name:  "register-node",
	Usage: "register deviceID and secret ",
	Flags: []cli.Flag{
		// schedulerURLFlag,
		// nodeTypeFlag,
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		ctx := ReqContext(cctx)

		schedulerAPI, closer, err := GetSchedulerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		info, err := schedulerAPI.RegisterNode(ctx)
		if err != nil {
			return err
		}

		log.Infof("\nDeviceID:%s\nSecret:%s", info.DeviceID, info.Secret)

		return nil
	},
}

var validateCmd = &cli.Command{
	Name:  "validate",
	Usage: "Validate edge node",
	Flags: []cli.Flag{
		// schedulerURLFlag,
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		// url := cctx.String("scheduler-url")

		ctx := ReqContext(cctx)

		schedulerAPI, closer, err := GetSchedulerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		return schedulerAPI.Validate(ctx)
	},
}

var electionCmd = &cli.Command{
	Name:  "election",
	Usage: "Start election validator",
	Flags: []cli.Flag{
		// schedulerURLFlag,
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		// url := cctx.String("scheduler-url")

		ctx := ReqContext(cctx)

		schedulerAPI, closer, err := GetSchedulerAPI(cctx)
		if err != nil {
			return err
		}

		defer closer()

		return schedulerAPI.ElectionValidators(ctx)
	},
}

var showDataInfoCmd = &cli.Command{
	Name:  "show-data",
	Usage: "show data",
	Flags: []cli.Flag{
		// schedulerURLFlag,
		cidFlag,
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		// url := cctx.String("scheduler-url")
		cid := cctx.String("cid")

		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		// if cid == "" {
		// 	return xerrors.New("cid is nil")
		// }

		str, err := schedulerAPI.ShowDataInfos(ctx, cid)
		if err != nil {
			return err
		}

		log.Infof("%v", str)

		return nil
	},
}

var cacheCarFileCmd = &cli.Command{
	Name:  "cache-file",
	Usage: "specify node cache carfile",
	Flags: []cli.Flag{
		// schedulerURLFlag,
		cidFlag,
		reliabilityFlag,
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		// url := cctx.String("scheduler-url")
		cid := cctx.String("cid")
		reliability := cctx.Int("reliability")

		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		if cid == "" {
			return xerrors.New("cid is nil")
		}

		err = schedulerAPI.CacheCarFile(ctx, cid, reliability)
		if err != nil {
			return err
		}

		return nil
	},
}

var cacheBlocksCmd = &cli.Command{
	Name:  "cache-blocks",
	Usage: "specify node cache blocks",
	Flags: []cli.Flag{
		// schedulerURLFlag,
		cidsFlag,
		deviceIDFlag,
		cidsPathFlag,
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		// url := cctx.String("scheduler-url")
		cids := cctx.String("cids")
		deviceID := cctx.String("device-id")
		cidsPath := cctx.String("cids-file-path")

		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		var cidList []string
		if cids != "" {
			cidList = strings.Split(cids, ",")
		}

		if cidsPath != "" {
			cidList, err = loadCidsFromFile(cidsPath)
			if err != nil {
				log.Errorf("loadFile err:%v", err)
				return err
			}
		}

		errCids, err := schedulerAPI.CacheBlocks(ctx, cidList, deviceID)
		if err != nil {
			return err
		}

		log.Infof("errCids:%v", errCids)

		return nil
	},
}

var getDownloadInfoCmd = &cli.Command{
	Name:  "download-infos",
	Usage: "specify node cache blocks",
	Flags: []cli.Flag{
		// schedulerURLFlag,
		cidsFlag,
		ipFlag,
		cidsPathFlag,
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		// url := cctx.String("scheduler-url")
		cids := cctx.String("cids")
		ip := cctx.String("ip")
		cidsPath := cctx.String("cids-file-path")

		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		var cidList []string
		if cids != "" {
			cidList = strings.Split(cids, ",")
		}

		if cidsPath != "" {
			cidList, err = loadCidsFromFile(cidsPath)
			if err != nil {
				log.Errorf("loadFile err:%v", err)
				return err
			}
		}

		data, err := schedulerAPI.GetDownloadInfoWithBlocks(ctx, cidList, ip)
		if err != nil {
			return err
		}

		log.Infof("data:%v", data)

		return nil
	},
}

var deleteBlocksCmd = &cli.Command{
	Name:  "delete-blocks",
	Usage: "delete cache blocks",
	Flags: []cli.Flag{
		// schedulerURLFlag,
		cidsFlag,
		deviceIDFlag,
		cidsPathFlag,
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		// url := cctx.String("scheduler-url")
		cids := cctx.String("cids")
		deviceID := cctx.String("device-id")
		cidsPath := cctx.String("cids-file-path")

		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		var cidList []string
		if cids != "" {
			cidList = strings.Split(cids, ",")
		}

		if cidsPath != "" {
			cidList, err = loadCidsFromFile(cidsPath)
			if err != nil {
				log.Errorf("loadFile err:%v", err)
				return err
			}
		}

		errorCids, err := schedulerAPI.DeleteBlocks(ctx, deviceID, cidList)
		if err != nil {
			return err
		}
		log.Infof("errorCids:%v", errorCids)

		return nil
	},
}

var showOnlineNodeCmd = &cli.Command{
	Name:  "show-nodes",
	Usage: "show all online node",
	Flags: []cli.Flag{
		// schedulerURLFlag,
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		// url := cctx.String("scheduler-url")

		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		nodes, err := schedulerAPI.GetOnlineDeviceIDs(ctx, api.TypeNameAll)

		log.Infof("Online nodes:%v", nodes)

		return err
	},
}

// var initDeviceIDsCmd = &cli.Command{
// 	Name:  "init-devices",
// 	Usage: "init deviceIDs",
// 	Flags: []cli.Flag{
// 		schedulerURLFlag,
// 	},

// 	Before: func(cctx *cli.Context) error {
// 		return nil
// 	},
// 	Action: func(cctx *cli.Context) error {
// 		// url := cctx.String("scheduler-url")

// 		ctx := ReqContext(cctx)
// 		schedulerAPI, closer, err := GetSchedulerAPI(cctx)
// 		if err != nil {
// 			return err
// 		}
// 		defer closer()

// 		return schedulerAPI.InitNodeDeviceIDs(ctx)
// 	},
// }

var cachingBlocksCmd = &cli.Command{
	Name:  "caching-blocks",
	Usage: "show caching blocks from node",
	Flags: []cli.Flag{
		// schedulerURLFlag,
		deviceIDFlag,
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		// url := cctx.String("scheduler-url")
		// log.Infof("scheduler url:%v", url)
		deviceID := cctx.String("device-id")

		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		body, err := schedulerAPI.QueryCachingBlocksWithNode(ctx, deviceID)
		if err != nil {
			return err
		}

		log.Infof("caching blocks:%v", body)

		return nil
	},
}

var cacheStatCmd = &cli.Command{
	Name:  "cache-stat",
	Usage: "show cache stat from node",
	Flags: []cli.Flag{
		// schedulerURLFlag,
		deviceIDFlag,
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		// url := cctx.String("scheduler-url")
		deviceID := cctx.String("device-id")

		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		body, err := schedulerAPI.QueryCacheStatWithNode(ctx, deviceID)
		if err != nil {
			return err
		}

		log.Infof("cache stat:%v", body)

		return nil
	},
}

type yconfig struct {
	Cids []string `toml:"cids"`
}

// loadFile
func loadCidsFromFile(configPath string) ([]string, error) {
	var config yconfig
	if _, err := toml.DecodeFile(configPath, &config); err != nil {
		return nil, err
	}

	c := &config
	return c.Cids, nil
}
