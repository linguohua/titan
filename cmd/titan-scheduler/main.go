package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/build"
	lcli "github.com/linguohua/titan/cli"
	"github.com/linguohua/titan/lib/titanlog"
	"github.com/linguohua/titan/lib/ulimit"
	"github.com/linguohua/titan/metrics"
	"github.com/linguohua/titan/node/repo"
	"github.com/linguohua/titan/node/scheduler"
	"github.com/linguohua/titan/node/scheduler/db/cache"
	"github.com/linguohua/titan/region"
	"go.opencensus.io/tag"
	"golang.org/x/xerrors"

	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
)

var log = logging.Logger("main")

const (
	// FlagWorkerRepo Flag
	FlagWorkerRepo = "scheduler-repo"

	// FlagWorkerRepoDeprecation Flag
	FlagWorkerRepoDeprecation = "schedulerrepo"
)

func main() {
	api.RunningNodeType = api.NodeScheduler

	titanlog.SetupLogLevels()

	local := []*cli.Command{
		runCmd,
	}

	local = append(local, lcli.SchedulerCmds...)

	app := &cli.App{
		Name:                 "titan-scheduler",
		Usage:                "Titan scheduler node",
		Version:              build.UserVersion(),
		EnableBashCompletion: true,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    FlagWorkerRepo,
				Aliases: []string{FlagWorkerRepoDeprecation},
				EnvVars: []string{"TITAN_SCHEDULER_PATH", "SCHEDULER_PATH"},
				Value:   "~/.titanscheduler", // TODO: Consider XDG_DATA_HOME
				Usage:   fmt.Sprintf("Specify worker repo path. flag %s and env TITAN_SCHEDULER_PATH are DEPRECATION, will REMOVE SOON", FlagWorkerRepoDeprecation),
			},
			&cli.StringFlag{
				Name:    "panic-reports",
				EnvVars: []string{"TITAN_PANIC_REPORT_PATH"},
				Hidden:  true,
				Value:   "~/.titanscheduler", // should follow --repo default
			},
		},

		After: func(c *cli.Context) error {
			if r := recover(); r != nil {
				// Generate report in LOTUS_PATH and re-raise panic
				build.GeneratePanicReport(c.String("panic-reports"), c.String(FlagWorkerRepo), c.App.Name)
				log.Panic(r)
			}
			return nil
		},
		Commands: local,
	}
	app.Setup()
	app.Metadata["repoType"] = repo.Worker

	if err := app.Run(os.Args); err != nil {
		log.Warnf("%+v", err)
		return
	}
}

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
		err = cache.NewCacheDB(cURL, cache.TypeRedis())
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
