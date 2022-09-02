package main

import (
	"context"
	"net"
	"net/http"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/client"
	"github.com/linguohua/titan/build"
	lcli "github.com/linguohua/titan/cli"
	"github.com/linguohua/titan/lib/ulimit"
	"github.com/linguohua/titan/metrics"
	"github.com/linguohua/titan/node/scheduler"
	"github.com/linguohua/titan/node/scheduler/db"
	"github.com/linguohua/titan/region"
	"github.com/urfave/cli/v2"
	"go.opencensus.io/tag"
	"golang.org/x/xerrors"
)

var (
	schedulerURLFlag = &cli.StringFlag{
		Name:  "scheduler-url",
		Usage: "host address and port the worker api will listen on",
		Value: "127.0.0.1:3456",
	}

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
)

var runCmd = &cli.Command{
	Name:  "run",
	Usage: "Start titan scheduler node",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "listen",
			Usage: "host address and port the worker api will listen on",
			Value: "0.0.0.0:3456",
		},
		&cli.StringFlag{
			Name:  "cachedb-url",
			Usage: "cachedb url",
			Value: "127.0.0.1:6378",
		},
		&cli.StringFlag{
			Name:  "geodb-path",
			Usage: "geodb path",
			Value: "../../geoip/geolite2_city/city.mmdb",
		},
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		log.Info("Starting titan scheduler node")

		limit, _, err := ulimit.GetLimit()
		switch {
		case err == ulimit.ErrUnsupported:
			log.Errorw("checking file descriptor limit failed", "error", err)
		case err != nil:
			return xerrors.Errorf("checking fd limit: %w", err)
		default:
			if limit < build.EdgeFDLimit {
				return xerrors.Errorf("soft file descriptor limit (ulimit -n) too low, want %d, current %d", build.EdgeFDLimit, limit)
			}
		}

		// Connect to scheduler
		ctx := lcli.ReqContext(cctx)

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		// init mysql db
		// db.GMysqlDb = db.GormMysql()
		// TODO
		cURL := cctx.String("cachedb-url")
		err = db.NewCacheDB(cURL, db.TypeRedis())
		if err != nil {
			log.Panic(err.Error())
		}

		gPath := cctx.String("geodb-path")
		err = region.NewRegion(gPath, region.TypeGeoLite())
		if err != nil {
			log.Panic(err.Error())
		}

		schedulerAPI := scheduler.NewLocalScheduleNode()

		srv := &http.Server{
			Handler: schedulerHandler(schedulerAPI, true),
			BaseContext: func(listener net.Listener) context.Context {
				ctx, _ := tag.New(context.Background(), tag.Upsert(metrics.APIInterface, "titan-edge"))
				return ctx
			},
		}

		go func() {
			<-ctx.Done()
			log.Warn("Shutting down...")
			if err := srv.Shutdown(context.TODO()); err != nil {
				log.Errorf("shutting down RPC server failed: %s", err)
			}
			log.Warn("Graceful shutdown successful")
		}()

		address := cctx.String("listen")

		nl, err := net.Listen("tcp", address)
		if err != nil {
			return err
		}

		log.Info("titan scheduler listen with:", address)

		return srv.Serve(nl)
	},
}

var validateCmd = &cli.Command{
	Name:  "validate",
	Usage: "Validate edge node",
	Flags: []cli.Flag{
		schedulerURLFlag,
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		url := cctx.String("scheduler-url")

		ctx := lcli.ReqContext(cctx)

		schedulerAPI, closer, err := client.NewScheduler(ctx, url, nil)
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
		schedulerURLFlag,
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		url := cctx.String("scheduler-url")

		ctx := lcli.ReqContext(cctx)

		schedulerAPI, closer, err := client.NewScheduler(ctx, url, nil)
		if err != nil {
			return err
		}

		defer closer()

		return schedulerAPI.ElectionValidators(ctx)
	},
}

var cacheDatasCmd = &cli.Command{
	Name:  "cache-data",
	Usage: "specify node cache data",
	Flags: []cli.Flag{
		schedulerURLFlag,
		cidsFlag,
		deviceIDFlag,
		cidsPathFlag,
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		url := cctx.String("scheduler-url")
		cids := cctx.String("cids")
		deviceID := cctx.String("device-id")
		cidsPath := cctx.String("cids-file-path")

		ctx := lcli.ReqContext(cctx)
		schedulerAPI, closer, err := client.NewScheduler(ctx, url, nil)
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

		errCids, err := schedulerAPI.CacheDatas(ctx, cidList, deviceID)
		if err != nil {
			return err
		}

		log.Infof("errCids:%v", errCids)

		return nil
	},
}

var delDatasCmd = &cli.Command{
	Name:  "delete-datas",
	Usage: "delete cache datas",
	Flags: []cli.Flag{
		schedulerURLFlag,
		cidsFlag,
		deviceIDFlag,
		cidsPathFlag,
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		url := cctx.String("scheduler-url")
		cids := cctx.String("cids")
		deviceID := cctx.String("device-id")
		cidsPath := cctx.String("cids-file-path")

		ctx := lcli.ReqContext(cctx)
		schedulerAPI, closer, err := client.NewScheduler(ctx, url, nil)
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

		errorCids, err := schedulerAPI.DeleteDatas(ctx, deviceID, cidList)
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
		schedulerURLFlag,
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		url := cctx.String("scheduler-url")

		ctx := lcli.ReqContext(cctx)
		schedulerAPI, closer, err := client.NewScheduler(ctx, url, nil)
		if err != nil {
			return err
		}
		defer closer()

		nodes, err := schedulerAPI.GetOnlineDeviceIDs(ctx, api.TypeNameAll)

		log.Infof("Online nodes:%v", nodes)

		return err
	},
}

var initDeviceIDsCmd = &cli.Command{
	Name:  "init-devices",
	Usage: "init deviceIDs",
	Flags: []cli.Flag{
		schedulerURLFlag,
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		url := cctx.String("scheduler-url")

		ctx := lcli.ReqContext(cctx)
		schedulerAPI, closer, err := client.NewScheduler(ctx, url, nil)
		if err != nil {
			return err
		}
		defer closer()

		return schedulerAPI.InitNodeDeviceIDs(ctx)
	},
}

var cachingBlocksCmd = &cli.Command{
	Name:  "caching-blocks",
	Usage: "show caching blocks from node",
	Flags: []cli.Flag{
		schedulerURLFlag,
		deviceIDFlag,
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		url := cctx.String("scheduler-url")
		// log.Infof("scheduler url:%v", url)
		deviceID := cctx.String("device-id")

		ctx := lcli.ReqContext(cctx)
		schedulerAPI, closer, err := client.NewScheduler(ctx, url, nil)
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
		schedulerURLFlag,
		deviceIDFlag,
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		url := cctx.String("scheduler-url")
		deviceID := cctx.String("device-id")

		ctx := lcli.ReqContext(cctx)
		schedulerAPI, closer, err := client.NewScheduler(ctx, url, nil)
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
