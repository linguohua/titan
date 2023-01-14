package cli

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"sort"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/filecoin-project/go-jsonrpc/auth"
	"github.com/linguohua/titan/api"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

// SchedulerCmds Scheduler Cmds
var SchedulerCmds = []*cli.Command{
	// cache
	listDataCmd,
	cacheContinueCmd,
	cacheCarfileCmd,
	showDataInfoCmd,
	removeCarfileCmd,
	removeCacheCmd,
	showDatasInfoCmd,
	resetCacheExpiredTimeCmd,
	replenishCacheExpiredTimeCmd,
	nodeQuitCmd,
	stopCacheCmd,
	// validate
	electionCmd,
	validateCmd,
	validateSwitchCmd,
	// node
	showOnlineNodeCmd,
	nodeTokenCmd,
	registerNodeCmd,
	redressInfoCmd,
	// other
	cachingBlocksCmd,
	cacheStatCmd,
	getDownloadInfoCmd,
	// listEventCmd,
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
		Usage: "cache reliability (default:2)",
		Value: 2,
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

	expiredDateFlag = &cli.StringFlag{
		Name:  "expired-date",
		Usage: "date time ('2006-1-2 15:04:05') (default:7 day later)",
		Value: "",
	}

	dateFlag = &cli.StringFlag{
		Name:  "date-time",
		Usage: "date time (2006-1-2 15:04:05)",
		Value: "",
	}
)

var redressInfoCmd = &cli.Command{
	Name:  "redress-node-info",
	Usage: "redress node info",
	Flags: []cli.Flag{
		deviceIDFlag,
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		deviceID := cctx.String("device-id")

		ctx := ReqContext(cctx)

		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		return schedulerAPI.RedressDeveiceInfo(ctx, deviceID)
	},
}

var nodeQuitCmd = &cli.Command{
	Name:  "node-quit",
	Usage: "node quit the titan",
	Flags: []cli.Flag{
		deviceIDFlag,
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		deviceID := cctx.String("device-id")

		ctx := ReqContext(cctx)

		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		err = schedulerAPI.NodeQuit(ctx, deviceID)
		if err != nil {
			return err
		}

		return nil
	},
}

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

var stopCacheCmd = &cli.Command{
	Name:  "stop-cache",
	Usage: "stop cache task",
	Flags: []cli.Flag{
		cidFlag,
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		cardileCid := cctx.String("cid")

		ctx := ReqContext(cctx)

		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		err = schedulerAPI.StopCacheTask(ctx, cardileCid)
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
		&cli.StringFlag{
			Name:  "device-id",
			Usage: "device id",
			Value: "",
		},
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		secret := cctx.String("secret")
		deviceID := cctx.String("device-id")
		ctx := ReqContext(cctx)

		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		perms := []auth.Permission{api.PermRead, api.PermWrite}
		info, err := schedulerAPI.AuthNodeNew(ctx, perms, deviceID, secret)
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

		infos, err := schedulerAPI.RegisterNode(ctx, api.NodeType(t), 1)
		if err != nil {
			return err
		}

		fmt.Printf("\nDeviceID:%s\nSecret:%s", infos[0].DeviceID, infos[0].Secret)
		return nil
	},
}

var removeCarfileCmd = &cli.Command{
	Name:  "remove-carfile",
	Usage: "remove a carfile",
	Flags: []cli.Flag{
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

		return schedulerAPI.ValidateStart(ctx)
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

		info, err := schedulerAPI.ListCarfileRecords(ctx, page)
		if err != nil {
			return err
		}

		for _, info := range info.CacheInfos {
			fmt.Printf("%s ,Reliabilit: %d/%d , Blocks:%d , \n", info.CarfileCid, info.Reliability, info.NeedReliability, info.TotalBlocks)
		}
		fmt.Printf("total:%d            %d/%d \n", info.Cids, info.Page, info.TotalPage)

		return nil
	},
}

// var listEventCmd = &cli.Command{
// 	Name:  "list-event",
// 	Usage: "list event",
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

// 		info, err := schedulerAPI.ListEvents(ctx, page)
// 		if err != nil {
// 			return err
// 		}

// 		for _, event := range info.EventList {
// 			fmt.Printf("CID:%s,Event:%s,DeviceID:%s,Msg:%s,User:%s,Time:%s \n", event.CID, event.Event, event.DeviceID, event.Msg, event.User, event.CreateTime.String())
// 		}
// 		fmt.Printf("total:%d            %d/%d \n", info.Count, info.Page, info.TotalPage)

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

		schedulerAPI.ValidateSwitch(ctx, open)

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

		infos, err := schedulerAPI.ShowRunningCarfileRecords(ctx)
		if err != nil {
			return err
		}

		statusToStr := func(s api.CacheStatus) string {
			switch s {
			case api.CacheStatusSuccess:
				return "done"
			default:
				return "running"
			}
		}

		sort.Slice(infos, func(i, j int) bool {
			return infos[i].CarfileCid < infos[j].CarfileCid
		})

		for w := 0; w < len(infos); w++ {
			info := infos[w]

			fmt.Printf("\nData CID:%s , Total Size:%f MB , Total Blocks:%d \n", info.CarfileCid, float64(info.TotalSize)/(1024*1024), info.TotalBlocks)

			sort.Slice(info.CacheInfos, func(i, j int) bool {
				return info.CacheInfos[i].DeviceID < info.CacheInfos[j].DeviceID
			})

			for j := 0; j < len(info.CacheInfos); j++ {
				cache := info.CacheInfos[j]

				timeout := ""
				if cache.DataTimeout > 0 {
					timeout = fmt.Sprintf(", Task Timeout:%s", cache.DataTimeout.String())
				}

				fmt.Printf("DeviceID:%s ,  Status:%s , Done Size:%f MB , Done Blocks:%d , IsRootCache:%v %s\n",
					cache.DeviceID, statusToStr(cache.Status), float64(cache.DoneSize)/(1024*1024), cache.DoneBlocks, cache.RootCache, timeout)
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

		info, err := schedulerAPI.GetCarfileRecord(ctx, cid)
		if err != nil {
			return err
		}

		statusToStr := func(s api.CacheStatus) string {
			switch s {
			case api.CacheStatusCreate:
				return "running"
			case api.CacheStatusSuccess:
				return "done"
			case api.CacheStatusTimeout:
				return "time out"
			default:
				return "failed"
			}
		}

		fmt.Printf("Data CID:%s , Total Size:%f MB , Total Blocks:%d \n", info.CarfileCid, float64(info.TotalSize)/(1024*1024), info.TotalBlocks)
		for _, cache := range info.CacheInfos {

			timeout := ""
			if cache.DataTimeout > 0 {
				timeout = fmt.Sprintf(", Task Timeout:%s", cache.DataTimeout.String())
			}

			fmt.Printf("DeviceID:%s ,  Status:%s , Done Size:%f MB ,Done Blocks:%d , IsRootCache:%v %s\n",
				cache.DeviceID, statusToStr(cache.Status), float64(cache.DoneSize)/(1024*1024), cache.DoneBlocks, cache.RootCache, timeout)
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
		expiredDateFlag,
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

		expiredDate := cctx.String("expired-date")

		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		if cid == "" {
			return xerrors.New("cid is nil")
		}

		if expiredDate == "" {
			expiredDate = time.Now().Add(time.Duration(7*24) * time.Hour).Format("2006-1-2 15:04:05")
		}

		time, err := time.ParseInLocation("2006-1-2 15:04:05", expiredDate, time.Local)
		if err != nil {
			return xerrors.Errorf("expired date err:%s", err.Error())
		}

		err = schedulerAPI.CacheCarfile(ctx, cid, reliability, time)
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
		cacheID := cctx.String("cache-id")

		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		err = schedulerAPI.CacheContinue(ctx, cid, cacheID)
		if err != nil {
			return err
		}

		return nil
	},
}

var getDownloadInfoCmd = &cli.Command{
	Name:  "download-infos",
	Usage: "specify node cache blocks",
	Flags: []cli.Flag{
		// schedulerURLFlag,
		cidFlag,
		// ipFlag,
		// cidsPathFlag,
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		// url := cctx.String("scheduler-url")
		cid := cctx.String("cid")
		// ip := cctx.String("ip")
		// cidsPath := cctx.String("cids-file-path")

		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		// var cidList []string
		// if cid != "" {
		// 	cidList = strings.Split(cids, ",")
		// }

		// if cidsPath != "" {
		// 	cidList, err = loadCidsFromFile(cidsPath)
		// 	if err != nil {
		// 		return fmt.Errorf("loadFile err:%s", err.Error())
		// 	}
		// }

		privateKey, _ := generatePrivateKey(1024)
		publicKey := publicKey2Pem(&privateKey.PublicKey)

		datas, err := schedulerAPI.GetDownloadInfosWithCarfile(ctx, cid, publicKey)
		if err != nil {
			return err
		}

		fmt.Printf("datas:%v", datas)

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
