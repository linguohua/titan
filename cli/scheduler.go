package cli

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/filecoin-project/go-jsonrpc/auth"
	"github.com/linguohua/titan/api"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

// SchedulerCmds Scheduler Cmds
var SchedulerCmds = []*cli.Command{
	// cacheBlocksCmd,
	electionCmd,
	validateCmd,
	showOnlineNodeCmd,
	// deleteBlocksCmd,
	// initDeviceIDsCmd,
	cachingBlocksCmd,
	cacheStatCmd,
	getDownloadInfoCmd,
	cacheCarfileCmd,
	showDataInfoCmd,
	registerNodeCmd,
	cacheContinueCmd,
	listDataCmd,
	validateSwitchCmd,
	removeCarfileCmd,
	removeCacheCmd,
	showDatasInfoCmd,
	nodeTokenCmd,
	listEventCmd,
	resetCacheExpiredTimeCmd,
	replenishCacheExpiredTimeCmd,
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

	reliabilityFlag = &cli.IntFlag{
		Name:  "reliability",
		Usage: "cache reliability",
		Value: 0,
	}

	nodeTypeFlag = &cli.IntFlag{
		Name:  "node-type",
		Usage: "node type 1:Edge 2:Candidate 3:Scheduler",
		Value: 0,
	}

	areaFlag = &cli.StringFlag{
		Name:  "area",
		Usage: "area",
		Value: "CN-GD-Shenzhen",
	}

	cacheIDFlag = &cli.StringFlag{
		Name:  "cache-id",
		Usage: "cache id",
		Value: "",
	}

	pageFlag = &cli.IntFlag{
		Name:  "page",
		Usage: "page",
		Value: 0,
	}

	expiredTimeFlag = &cli.IntFlag{
		Name:  "expired-time",
		Usage: "the cache expired time (unit:hour)",
		Value: 0,
	}

	dateFlag = &cli.StringFlag{
		Name:  "date-time",
		Usage: "date time",
		Value: "",
	}
)

var resetCacheExpiredTimeCmd = &cli.Command{
	Name:  "reset-cache-expired",
	Usage: "reset cache expired time",
	Flags: []cli.Flag{
		cidFlag,
		cacheIDFlag,
		dateFlag,
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		cardileCid := cctx.String("cid")
		cacheID := cctx.String("cache-id")
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

		err = schedulerAPI.ResetCacheExpiredTime(ctx, cardileCid, cacheID, time)
		if err != nil {
			return err
		}

		return nil
	},
}

var replenishCacheExpiredTimeCmd = &cli.Command{
	Name:  "replenish-cache-expired",
	Usage: "replenish cache expired time",
	Flags: []cli.Flag{
		cidFlag,
		cacheIDFlag,
		expiredTimeFlag,
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		cardileCid := cctx.String("cid")
		cacheID := cctx.String("cache-id")
		hour := cctx.Int("expired-time")

		ctx := ReqContext(cctx)

		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		if hour <= 0 {
			return xerrors.Errorf("expired time err:%d", hour)
		}

		err = schedulerAPI.ReplenishCacheExpiredTime(ctx, cardileCid, cacheID, hour)
		if err != nil {
			return err
		}

		return nil
	},
}

var nodeTokenCmd = &cli.Command{
	Name:  "get-node-token",
	Usage: "get node token with secret ",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "secret",
			Usage: "node secret",
			Value: "",
		},
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		secret := cctx.String("secret")
		ctx := ReqContext(cctx)

		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		os := []auth.Permission{api.PermRead, api.PermWrite}
		info, err := schedulerAPI.AuthNodeNew(ctx, os, secret)
		if err != nil {
			return err
		}

		fmt.Println(string(info))
		return nil
	},
}

var registerNodeCmd = &cli.Command{
	Name:  "register-node",
	Usage: "register deviceID and secret ",
	Flags: []cli.Flag{
		// schedulerURLFlag,
		nodeTypeFlag,
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		t := cctx.Int("node-type")
		ctx := ReqContext(cctx)

		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		if t != int(api.NodeEdge) && t != int(api.NodeCandidate) && t != int(api.NodeScheduler) {
			return xerrors.Errorf("node-type err:%d", t)
		}

		info, err := schedulerAPI.RegisterNode(ctx, api.NodeType(t))
		if err != nil {
			return err
		}

		fmt.Printf("\nDeviceID:%s\nSecret:%s", info.DeviceID, info.Secret)
		return nil
	},
}

var removeCarfileCmd = &cli.Command{
	Name:  "remove-carfile",
	Usage: "remove a carfile",
	Flags: []cli.Flag{
		cacheIDFlag,
		cidFlag,
	},

	Before: func(cctx *cli.Context) error {
		return nil
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

var removeCacheCmd = &cli.Command{
	Name:  "remove-cache",
	Usage: "remove a cache",
	Flags: []cli.Flag{
		cacheIDFlag,
		cidFlag,
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		cacheID := cctx.String("cache-id")
		cid := cctx.String("cid")

		ctx := ReqContext(cctx)

		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		return schedulerAPI.RemoveCache(ctx, cid, cacheID)
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

		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
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

		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}

		defer closer()

		return schedulerAPI.ElectionValidators(ctx)
	},
}

var listDataCmd = &cli.Command{
	Name:  "list-data",
	Usage: "list data",
	Flags: []cli.Flag{
		pageFlag,
	},

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

		page := cctx.Int("page")
		if page < 1 {
			return xerrors.New("page need greater than 1")
		}

		info, err := schedulerAPI.ListDatas(ctx, page)
		if err != nil {
			return err
		}

		for _, str := range info.CidList {
			fmt.Println(str)
		}
		fmt.Printf("total:%d            %d/%d \n", info.Cids, info.Page, info.TotalPage)

		return nil
	},
}

var listEventCmd = &cli.Command{
	Name:  "list-event",
	Usage: "list event",
	Flags: []cli.Flag{
		pageFlag,
	},

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

		page := cctx.Int("page")
		if page < 1 {
			return xerrors.New("page need greater than 1")
		}

		info, err := schedulerAPI.ListEvents(ctx, page)
		if err != nil {
			return err
		}

		for _, event := range info.EventList {
			fmt.Printf("CID:%s,Event:%s,CacheID:%s,Msg:%s,User:%s,Time:%s \n", event.CID, event.Event, event.CacheID, event.Msg, event.User, event.CreateTime.String())
		}
		fmt.Printf("total:%d            %d/%d \n", info.Count, info.Page, info.TotalPage)

		return nil
	},
}

// var listEventCmd = &cli.Command{
// 	Name:  "list-event",
// 	Usage: "list events",
// 	Flags: []cli.Flag{
// 		pageFlag,
// 	},

// 	Before: func(cctx *cli.Context) error {
// 		return nil
// 	},
// 	Action: func(cctx *cli.Context) error {
// 		ctx := ReqContext(cctx)
// 		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
// 		if err != nil {
// 			return err
// 		}
// 		defer closer()

// 		page := cctx.Int("page")
// 		if page < 1 {
// 			return xerrors.New("page need greater than 1")
// 		}

// 		info, err := schedulerAPI.ListDatas(ctx, page)
// 		if err != nil {
// 			return err
// 		}

// 		for _, str := range info.CidList {
// 			fmt.Println(str)
// 		}
// 		fmt.Printf("total:%d            %d/%d \n", info.Cids, info.Page, info.TotalPage)

// 		return nil
// 	},
// }

var validateSwitchCmd = &cli.Command{
	Name:  "validate-switch",
	Usage: "validate switch",
	Flags: []cli.Flag{
		// schedulerURLFlag,
		&cli.StringFlag{
			Name:  "open",
			Usage: "is open",
			Value: "false",
		},
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		// url := cctx.String("scheduler-url")
		open := cctx.Bool("open")

		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		err = schedulerAPI.ValidateSwitch(ctx, open)
		if err != nil {
			return err
		}

		return nil
	},
}

var showDatasInfoCmd = &cli.Command{
	Name:  "show-running-datas",
	Usage: "show data",
	Flags: []cli.Flag{
		// schedulerURLFlag,
		// cidFlag,
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		// url := cctx.String("scheduler-url")

		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		infos, err := schedulerAPI.ShowDataTasks(ctx)
		if err != nil {
			return err
		}

		statusToStr := func(s int) string {
			switch s {
			case 1:
				return "running"
			case 3:
				return "done"
			case 4:
				return "time out"
			default:
				return "failed"
			}
		}

		sort.Slice(infos, func(i, j int) bool {
			return infos[i].CarfileCid < infos[j].CarfileCid
		})

		for w := 0; w < len(infos); w++ {
			info := infos[w]
			fmt.Printf("Data CID:%s , Total Size:%f MB , Total Blocks:%d \n", info.CarfileCid, float64(info.TotalSize)/(1024*1024), info.Blocks)

			sort.Slice(info.CacheInfos, func(i, j int) bool {
				return info.CacheInfos[i].CacheID < info.CacheInfos[j].CacheID
			})

			for j := 0; j < len(info.CacheInfos); j++ {
				cache := info.CacheInfos[j]
				fmt.Printf("TaskID:%s ,  Status:%s , Done Size:%f MB ,Done Blocks:%d , Nodes:%d\n",
					cache.CacheID, statusToStr(cache.Status), float64(cache.DoneSize)/(1024*1024), cache.DoneBlocks, cache.Nodes)
			}
		}

		return nil
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
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		info, err := schedulerAPI.ShowDataTask(ctx, cid)
		if err != nil {
			return err
		}

		statusToStr := func(s int) string {
			switch s {
			case 1:
				return "running"
			case 3:
				return "done"
			case 4:
				return "time out"
			default:
				return "failed"
			}
		}

		fmt.Printf("Data CID:%s , Total Size:%f MB , Total Blocks:%d , Nodes:%d \n", info.CarfileCid, float64(info.TotalSize)/(1024*1024), info.Blocks, info.Nodes)
		for _, cache := range info.CacheInfos {
			fmt.Printf("TaskID:%s ,  Status:%s , Done Size:%f MB ,Done Blocks:%d , Nodes:%d\n",
				cache.CacheID, statusToStr(cache.Status), float64(cache.DoneSize)/(1024*1024), cache.DoneBlocks, cache.Nodes)
		}

		return nil
	},
}

var cacheCarfileCmd = &cli.Command{
	Name:  "cache-file",
	Usage: "specify node cache carfile",
	Flags: []cli.Flag{
		// schedulerURLFlag,
		cidFlag,
		reliabilityFlag,
		expiredTimeFlag,
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		// url := cctx.String("scheduler-url")
		cid := cctx.String("cid")
		reliability := cctx.Int("reliability")
		if reliability == 0 {
			return xerrors.New("reliability is 0")
		}

		expiredTime := cctx.Int("expired-time")
		if expiredTime <= 0 {
			return xerrors.New("expiredTime not 0")
		}

		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		if cid == "" {
			return xerrors.New("cid is nil")
		}

		err = schedulerAPI.CacheCarfile(ctx, cid, reliability, expiredTime)
		if err != nil {
			return err
		}

		return nil
	},
}

var cacheContinueCmd = &cli.Command{
	Name:  "cache-continue",
	Usage: "cache continue",
	Flags: []cli.Flag{
		// schedulerURLFlag,
		cidFlag,
		cacheIDFlag,
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		// url := cctx.String("scheduler-url")
		cid := cctx.String("cid")
		cacgeID := cctx.String("cache-id")

		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		err = schedulerAPI.CacheContinue(ctx, cid, cacgeID)
		if err != nil {
			return err
		}

		return nil
	},
}

// var cacheBlocksCmd = &cli.Command{
// 	Name:  "cache-blocks",
// 	Usage: "specify node cache blocks",
// 	Flags: []cli.Flag{
// 		// schedulerURLFlag,
// 		cidsFlag,
// 		deviceIDFlag,
// 		cidsPathFlag,
// 	},

// 	Before: func(cctx *cli.Context) error {
// 		return nil
// 	},
// 	Action: func(cctx *cli.Context) error {
// 		// url := cctx.String("scheduler-url")
// 		cids := cctx.String("cids")
// 		deviceID := cctx.String("device-id")
// 		cidsPath := cctx.String("cids-file-path")

// 		ctx := ReqContext(cctx)
// 		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
// 		if err != nil {
// 			return err
// 		}
// 		defer closer()

// 		var cidList []string
// 		if cids != "" {
// 			cidList = strings.Split(cids, ",")
// 		}

// 		if cidsPath != "" {
// 			cidList, err = loadCidsFromFile(cidsPath)
// 			if err != nil {
// 				return fmt.Errorf("loadFile err:%s", err.Error())
// 			}
// 		}

// 		errCids, err := schedulerAPI.CacheBlocks(ctx, cidList, deviceID)
// 		if err != nil {
// 			return err
// 		}

// 		fmt.Printf("errCids:%v", errCids)

// 		return nil
// 	},
// }

var getDownloadInfoCmd = &cli.Command{
	Name:  "download-infos",
	Usage: "specify node cache blocks",
	Flags: []cli.Flag{
		// schedulerURLFlag,
		cidsFlag,
		// ipFlag,
		cidsPathFlag,
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		// url := cctx.String("scheduler-url")
		cids := cctx.String("cids")
		// ip := cctx.String("ip")
		cidsPath := cctx.String("cids-file-path")

		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
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
				return fmt.Errorf("loadFile err:%s", err.Error())
			}
		}

		privateKey, _ := generatePrivateKey(1024)
		publicKey := publicKey2Pem(&privateKey.PublicKey)

		data, err := schedulerAPI.GetDownloadInfoWithBlocks(ctx, cidList, publicKey)
		if err != nil {
			return err
		}

		fmt.Printf("data:%v", data)

		return nil
	},
}

func generatePrivateKey(bits int) (*rsa.PrivateKey, error) {
	privateKey, err := rsa.GenerateKey(rand.Reader, bits)
	if err != nil {
		return nil, err
	}

	return privateKey, nil
}

func publicKey2Pem(publicKey *rsa.PublicKey) string {
	if publicKey == nil {
		return ""
	}

	public := x509.MarshalPKCS1PublicKey(publicKey)

	publicKeyBytes := pem.EncodeToMemory(
		&pem.Block{
			Type:  "RSA PUBLIC KEY",
			Bytes: public,
		},
	)

	return string(publicKeyBytes)
}

// var deleteBlocksCmd = &cli.Command{
// 	Name:  "delete-blocks",
// 	Usage: "delete cache blocks",
// 	Flags: []cli.Flag{
// 		// schedulerURLFlag,
// 		cidsFlag,
// 		deviceIDFlag,
// 		cidsPathFlag,
// 	},

// 	Before: func(cctx *cli.Context) error {
// 		return nil
// 	},
// 	Action: func(cctx *cli.Context) error {
// 		// url := cctx.String("scheduler-url")
// 		cids := cctx.String("cids")
// 		deviceID := cctx.String("device-id")
// 		cidsPath := cctx.String("cids-file-path")

// 		ctx := ReqContext(cctx)
// 		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
// 		if err != nil {
// 			return err
// 		}
// 		defer closer()

// 		var cidList []string
// 		if cids != "" {
// 			cidList = strings.Split(cids, ",")
// 		}

// 		if cidsPath != "" {
// 			cidList, err = loadCidsFromFile(cidsPath)
// 			if err != nil {
// 				return fmt.Errorf("loadFile err:%s", err.Error())
// 			}
// 		}

// 		errorCids, err := schedulerAPI.DeleteBlocks(ctx, deviceID, cidList)
// 		if err != nil {
// 			return err
// 		}
// 		fmt.Printf("errorCids:%v", errorCids)

// 		return nil
// 	},
// }

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
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		nodes, err := schedulerAPI.GetOnlineDeviceIDs(ctx, api.TypeNameAll)

		fmt.Printf("Online nodes:%v", nodes)

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
// 		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
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
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		body, err := schedulerAPI.QueryCachingBlocksWithNode(ctx, deviceID)
		if err != nil {
			return err
		}

		fmt.Printf("caching blocks:%v", body)

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
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		body, err := schedulerAPI.QueryCacheStatWithNode(ctx, deviceID)
		if err != nil {
			return err
		}

		fmt.Printf("cache stat:%v", body)

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
