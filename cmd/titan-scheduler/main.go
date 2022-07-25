package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"titan/api"
	"titan/api/client"
	"titan/build"
	lcli "titan/cli"
	"titan/lib/titanlog"
	"titan/metrics"
	"titan/node/repo"
	"titan/node/scheduler"
	"titan/node/scheduler/redishelper"

	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
	"go.opencensus.io/tag"
)

var log = logging.Logger("main")

const (
	FlagWorkerRepo = "scheduler-repo"

	// TODO remove after deprecation period
	FlagWorkerRepoDeprecation = "schedulerrepo"
)

func main() {
	api.RunningNodeType = api.NodeScheduler

	titanlog.SetupLogLevels()

	local := []*cli.Command{
		runCmd,
		cacheCmd,
	}

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
				panic(r)
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
			Name:  "redis",
			Usage: "used when 'listen' is unspecified. must be a valid duration recognized by golang's time.ParseDuration function",
			Value: "127.0.0.1:6379",
		},
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		log.Info("Starting titan scheduler node")

		// limit, _, err := ulimit.GetLimit()
		// switch {
		// case err == ulimit.ErrUnsupported:
		// 	log.Errorw("checking file descriptor limit failed", "error", err)
		// case err != nil:
		// 	return xerrors.Errorf("checking fd limit: %w", err)
		// default:
		// 	if limit < build.EdgeFDLimit {
		// 		return xerrors.Errorf("soft file descriptor limit (ulimit -n) too low, want %d, current %d", build.EdgeFDLimit, limit)
		// 	}
		// }

		// Connect to scheduler
		ctx := lcli.ReqContext(cctx)

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		schedulerAPI := scheduler.NewLocalScheduleNode()

		// log.Info("Setting up control endpoint at " + address)

		srv := &http.Server{
			Handler: schedulerHandler(schedulerAPI, false),
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
		redis := cctx.String("redis")

		redishelper.InitPool(redis)

		nl, err := net.Listen("tcp", address)
		if err != nil {
			return err
		}

		return srv.Serve(nl)
	},
}

var cacheCmd = &cli.Command{
	Name:  "cache",
	Usage: "Start titan test scheduler node",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "api-url",
			Usage: "host address and port the worker api will listen on",
			Value: "127.0.0.1:3456",
		},
		&cli.StringFlag{
			Name:  "cid",
			Usage: "used when 'listen' is unspecified. must be a valid duration recognized by golang's time.ParseDuration function",
			Value: "",
		},
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		arg0 := cctx.Args().Get(0)

		url := cctx.String("api-url")
		cid := cctx.String("cid")
		deviceID := cctx.String("deviceID")
		log.Infof("test cid:%v,url:%v,deviceID:%v,arg:%v", cid, url, deviceID, arg0)

		ctx := lcli.ReqContext(cctx)

		var schedulerAPI api.Scheduler
		var closer func()
		var err error
		for {
			schedulerAPI, closer, err = client.NewScheduler(ctx, url, nil)
			if err == nil {
				_, err = schedulerAPI.Version(ctx)
				if err == nil {
					break
				}
			}
			fmt.Printf("\r\x1b[0KConnecting to miner API... (%s)", err)
			time.Sleep(time.Second)
			continue
		}

		defer closer()

		schedulerAPI.CacheData(ctx, []string{cid}, []string{deviceID})

		return nil
	},
}
