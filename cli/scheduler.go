package cli

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"sort"
	"strconv"
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
	// cache
	listCarfilesCmd,
	cacheCarfileCmd,
	showCarfileInfoCmd,
	removeCarfileCmd,
	removeCacheCmd,
	showRunningCarfilesCmd,
	resetCarfileExpiredTimeCmd,
	replenishCarfileExpiredTimeCmd,
	nodeQuitCmd,
	stopCarfileCmd,
	resetBackupCacheCountCmd,
	listUndoneCarfilesCmd,
	// validate
	electionCmd,
	validateCmd,
	validateSwitchCmd,
	// node
	showOnlineNodeCmd,
	nodeTokenCmd,
	registerNodeCmd,
	redressInfoCmd,
	showNodeInfoCmd,
	// other
	cachingCarfilesCmd,
	cacheStatCmd,
	getDownloadInfoCmd,
	nodeAppUpdateCmd,
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
		Usage: "device id",
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

	countFlag = &cli.IntFlag{
		Name:  "count",
		Usage: "count",
		Value: 0,
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
var showNodeInfoCmd = &cli.Command{
	Name:  "show-node-info",
	Usage: "show node info",
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

		info, err := schedulerAPI.GetNodeInfoByID(ctx, deviceID)
		if err != nil {
			return err
		}

		fmt.Printf("device id: %s \n", info.DeviceId)
		fmt.Printf("device name: %s \n", info.DeviceName)
		fmt.Printf("device external_ip: %s \n", info.ExternalIp)
		fmt.Printf("device internal_ip: %s \n", info.InternalIp)
		fmt.Printf("device system version: %s \n", info.SystemVersion)
		fmt.Printf("device disk usage: %.2f %s\n", info.DiskUsage, "%")
		fmt.Printf("device disk space: %.2f GB \n", info.DiskSpace/1024/1024/1024)
		fmt.Printf("device fstype: %s \n", info.IoSystem)
		fmt.Printf("device mac: %s \n", info.MacLocation)
		fmt.Printf("device download bandwidth: %.2f GB \n", info.BandwidthDown/1024/1024/1024)
		fmt.Printf("device upload bandwidth: %.2f GB \n", info.BandwidthUp/1024/1024/1024)
		fmt.Printf("device cpu percent: %.2f %s \n", info.CpuUsage, "%")
		//
		fmt.Printf("device DownloadCount: %d \n", info.DownloadCount)

		return nil
	},
}

var resetBackupCacheCountCmd = &cli.Command{
	Name:  "reset-backup-count",
	Usage: "reset backup cache count",
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

		return schedulerAPI.ResetBackupCacheCount(ctx, count)
	},
}

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

var resetCarfileExpiredTimeCmd = &cli.Command{
	Name:  "reset-carfile-expired",
	Usage: "reset carfile expired time",
	Flags: []cli.Flag{
		cidFlag,
		dateFlag,
	},

	Before: func(cctx *cli.Context) error {
		return nil
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

		err = schedulerAPI.ResetCacheExpiredTime(ctx, cardileCid, time)
		if err != nil {
			return err
		}

		return nil
	},
}

var replenishCarfileExpiredTimeCmd = &cli.Command{
	Name:  "replenish-carfile-expired",
	Usage: "replenish carfile expired time",
	Flags: []cli.Flag{
		cidFlag,
		expiredTimeFlag,
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		cardileCid := cctx.String("cid")
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

		err = schedulerAPI.ReplenishCacheExpiredTime(ctx, cardileCid, hour)
		if err != nil {
			return err
		}

		return nil
	},
}

var stopCarfileCmd = &cli.Command{
	Name:  "stop-carfile",
	Usage: "stop carfile task",
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
		deviceIDFlag,
		cidFlag,
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		deviceID := cctx.String("device-id")
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

var listUndoneCarfilesCmd = &cli.Command{
	Name:  "list-undone-carfiles",
	Usage: "list undone carfiles",
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

		info, err := schedulerAPI.GetUndoneCarfileRecords(ctx, page)
		if err != nil {
			return err
		}

		for _, carfile := range info.CarfileRecords {
			fmt.Printf("%s ,Reliabilit: %d/%d ,Blocks:%d ,Expired Time:%s \n", carfile.CarfileCid, carfile.Reliability, carfile.NeedReliability, carfile.TotalBlocks, carfile.ExpiredTime.Format("2006-01-02 15:04:05"))
		}
		fmt.Printf("total:%d            %d/%d \n", info.Cids, info.Page, info.TotalPage)

		return nil
	},
}

var listCarfilesCmd = &cli.Command{
	Name:  "list-carfile",
	Usage: "list carfile",
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

		for _, carfile := range info.CarfileRecords {
			fmt.Printf("%s ,Reliabilit: %d/%d ,Blocks:%d ,Expired Time:%s \n", carfile.CarfileCid, carfile.Reliability, carfile.NeedReliability, carfile.TotalBlocks, carfile.ExpiredTime.Format("2006-01-02 15:04:05"))
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

var showRunningCarfilesCmd = &cli.Command{
	Name:  "show-running-carfiles",
	Usage: "show running carfiles",
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

		infos, err := schedulerAPI.GetRunningCarfileRecords(ctx)
		if err != nil {
			return err
		}

		sort.Slice(infos, func(i, j int) bool {
			return infos[i].CarfileCid < infos[j].CarfileCid
		})

		for w := 0; w < len(infos); w++ {
			info := infos[w]

			fmt.Printf("\nData CID: %s ,Total Size:%f MB ,Total Blocks:%d \n", info.CarfileCid, float64(info.TotalSize)/(1024*1024), info.TotalBlocks)

			sort.Slice(info.CacheInfos, func(i, j int) bool {
				return info.CacheInfos[i].DeviceID < info.CacheInfos[j].DeviceID
			})

			for j := 0; j < len(info.CacheInfos); j++ {
				cache := info.CacheInfos[j]
				fmt.Printf("DeviceID: %s , Status:%s ,Done Size:%f MB ,Done Blocks:%d ,IsRootCache:%v \n",
					cache.DeviceID, statusToStr(cache.Status), float64(cache.DoneSize)/(1024*1024), cache.DoneBlocks, cache.CandidateCache)
			}
		}

		return nil
	},
}

var showCarfileInfoCmd = &cli.Command{
	Name:  "show-carfile",
	Usage: "show carfile",
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

		fmt.Printf("Data CID: %s ,Total Size:%f MB ,Total Blocks:%d ,Expired Time:%s\n", info.CarfileCid, float64(info.TotalSize)/(1024*1024), info.TotalBlocks, info.ExpiredTime.Format("2006-01-02 15:04:05"))
		for _, cache := range info.CacheInfos {
			fmt.Printf("DeviceID: %s ,Status:%s ,Done Size:%f MB ,Done Blocks:%d ,IsRootCache:%v \n",
				cache.DeviceID, statusToStr(cache.Status), float64(cache.DoneSize)/(1024*1024), cache.DoneBlocks, cache.CandidateCache)
		}

		return nil
	},
}

func statusToStr(s api.CacheStatus) string {
	switch s {
	case api.CacheStatusCreate:
		return "create"
	case api.CacheStatusRunning:
		return "running"
	case api.CacheStatusSuccess:
		return "done"
	case api.CacheStatusTimeout:
		return "time out"
	default:
		return "failed"
	}
}

var cacheCarfileCmd = &cli.Command{
	Name:  "cache-carfile",
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
	Name:  "show-online-nodes",
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

		fmt.Println("Online nodes count:", len(nodes))
		for _, node := range nodes {
			fmt.Println(node)
		}

		return err
	},
}

var cachingCarfilesCmd = &cli.Command{
	Name:  "caching-carfiles",
	Usage: "show caching carfile from node",
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

		body, err := schedulerAPI.QueryCachingCarfileWithNode(ctx, deviceID)
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

var nodeAppUpdateCmd = &cli.Command{
	Name:  "node-update",
	Usage: "get node update info or set node update info",
	Subcommands: []*cli.Command{
		getNodeAppUpdateInfoCmd,
		setNodeAppUpdateInfoCmd,
	},
}

var getNodeAppUpdateInfoCmd = &cli.Command{
	Name:  "get",
	Usage: "get node update info",
	Action: func(cctx *cli.Context) error {
		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		updateInfos, err := schedulerAPI.GetNodeAppUpdateInfos(ctx)
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

var setNodeAppUpdateInfoCmd = &cli.Command{
	Name:  "set",
	Usage: "set node update info",
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

		updateInfo := &api.NodeAppUpdateInfo{AppName: appName, NodeType: nodeType, Version: version, Hash: hash, DownloadURL: downloadURL}
		err = schedulerAPI.SetNodeAppUpdateInfo(ctx, updateInfo)
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

	//major, minor, patch
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
