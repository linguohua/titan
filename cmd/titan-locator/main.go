package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/build"
	lcli "github.com/linguohua/titan/cli"
	"github.com/linguohua/titan/lib/titanlog"
	"github.com/linguohua/titan/lib/ulimit"
	"github.com/linguohua/titan/metrics"
	"github.com/linguohua/titan/node/locator"
	"github.com/linguohua/titan/node/repo"
	"github.com/linguohua/titan/region"
	"github.com/shirou/gopsutil/v3/cpu"

	"github.com/google/uuid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
	"golang.org/x/xerrors"

	"github.com/linguohua/titan/stores"
)

var log = logging.Logger("main")

const FlagLocatorRepo = "locator-repo"

// TODO remove after deprecation period
const FlagLocatorRepoDeprecation = "locatorrepo"

func main() {
	api.RunningNodeType = api.NodeLocator
	cpu.Percent(0, false)
	titanlog.SetupLogLevels()

	local := []*cli.Command{
		runCmd,
	}

	local = append(local, lcli.CommonCommands...)

	app := &cli.App{
		Name:                 "titan-locator",
		Usage:                "Titan locator node",
		Version:              build.UserVersion(),
		EnableBashCompletion: true,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    FlagLocatorRepo,
				Aliases: []string{FlagLocatorRepoDeprecation},
				EnvVars: []string{"TITAN_LOCATION_PATH", "LOCATION_PATH"},
				Value:   "~/.titanlocator", // TODO: Consider XDG_DATA_HOME
				Usage:   fmt.Sprintf("Specify locator repo path. flag %s and env TITAN_EDGE_PATH are DEPRECATION, will REMOVE SOON", FlagLocatorRepoDeprecation),
			},
			&cli.StringFlag{
				Name:    "panic-reports",
				EnvVars: []string{"TITAN_PANIC_REPORT_PATH"},
				Hidden:  true,
				Value:   "~/.titanlocator", // should follow --repo default
			},
		},

		After: func(c *cli.Context) error {
			if r := recover(); r != nil {
				// Generate report in LOTUS_PATH and re-raise panic
				build.GeneratePanicReport(c.String("panic-reports"), c.String(FlagLocatorRepo), c.App.Name)
				panic(r)
			}
			return nil
		},
		Commands: append(local, lcli.LocationCmds...),
	}
	app.Setup()
	app.Metadata["repoType"] = repo.Locator

	if err := app.Run(os.Args); err != nil {
		log.Errorf("%+v", err)
		return
	}
}

var runCmd = &cli.Command{
	Name:  "run",
	Usage: "Start titan edge node",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "listen",
			Usage: "host address and port the worker api will listen on",
			Value: "0.0.0.0:5000",
		},
		&cli.StringFlag{
			Name:   "address",
			Hidden: true,
		},
		&cli.StringFlag{
			Name:  "timeout",
			Usage: "used when 'listen' is unspecified. must be a valid duration recognized by golang's time.ParseDuration function",
			Value: "30m",
		},
		&cli.StringFlag{
			Name:  "geodb-path",
			Usage: "ip location",
			Value: "./city.mmdb",
		},
		&cli.StringFlag{
			Name:  "accesspoint-file",
			Usage: "accesspoint config",
			Value: "./cfg.toml",
		},
		&cli.StringFlag{
			Name:  "accesspoint-db",
			Usage: "use mysql to save config",
			Value: "user01:sql001@tcp(127.0.0.1:3306)/locator",
		},
		// &cli.StringFlag{
		// 	Name:  "scheduler-token",
		// 	Usage: "connect to scheduler",
		// 	Value: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJBbGxvdyI6WyJyZWFkIiwid3JpdGUiLCJzaWduIiwiYWRtaW4iXX0.Ewk-SQFNQi7ovTWfRI8ZfOnrDVNyU6iv3w40wsu-O4Y",
		// },
		&cli.StringFlag{
			Name:  "uuid",
			Usage: "locator uuid",
			Value: "f936b0dc-9e59-476b-a16c-36e3d2d7a752",
		},
	},

	Before: func(cctx *cli.Context) error {
		if cctx.IsSet("address") {
			log.Warnf("The '--address' flag is deprecated, it has been replaced by '--listen'")
			if err := cctx.Set("listen", cctx.String("address")); err != nil {
				return err
			}
		}

		return nil
	},
	Action: func(cctx *cli.Context) error {
		log.Info("Starting titan locator node")

		limit, _, err := ulimit.GetLimit()
		switch {
		case err == ulimit.ErrUnsupported:
			log.Errorw("checking file descriptor limit failed", "error", err)
		case err != nil:
			return xerrors.Errorf("checking fd limit: %w", err)
		default:
			if limit < build.DefaultFDLimit {
				return xerrors.Errorf("soft file descriptor limit (ulimit -n) too low, want %d, current %d", build.DefaultFDLimit, limit)
			}
		}

		gPath := cctx.String("geodb-path")
		err = region.NewRegion(gPath, region.TypeGeoLite())
		if err != nil {
			log.Panic(err.Error())
		}

		ctx := lcli.ReqContext(cctx)
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		// Register all metric views
		if err := view.Register(
			metrics.DefaultViews...,
		); err != nil {
			log.Fatalf("Cannot register the view: %v", err)
		}

		// Open repo
		repoPath := cctx.String(FlagLocatorRepo)
		r, err := repo.NewFS(repoPath)
		if err != nil {
			return err
		}

		ok, err := r.Exists()
		if err != nil {
			return err
		}
		if !ok {
			if err := r.Init(repo.Locator); err != nil {
				return err
			}

			lr, err := r.Lock(repo.Locator)
			if err != nil {
				return err
			}

			var localPaths []stores.LocalPath

			if !cctx.Bool("no-local-storage") {
				b, err := json.MarshalIndent(&stores.LocalStorageMeta{
					ID:       uuid.New().String(),
					Weight:   10,
					CanSeal:  true,
					CanStore: false,
				}, "", "  ")
				if err != nil {
					return xerrors.Errorf("marshaling storage config: %w", err)
				}

				if err := ioutil.WriteFile(filepath.Join(lr.Path(), "sectorstore.json"), b, 0o644); err != nil {
					return xerrors.Errorf("persisting storage metadata (%s): %w", filepath.Join(lr.Path(), "sectorstore.json"), err)
				}

				localPaths = append(localPaths, stores.LocalPath{
					Path: lr.Path(),
				})
			}

			if err := lr.SetStorage(func(sc *stores.StorageConfig) {
				sc.StoragePaths = append(sc.StoragePaths, localPaths...)
			}); err != nil {
				return xerrors.Errorf("set storage config: %w", err)
			}

			{
				// init datastore for r.Exists
				_, err := lr.Datastore(context.Background(), "/metadata")
				if err != nil {
					return err
				}
			}
			if err := lr.Close(); err != nil {
				return xerrors.Errorf("close repo: %w", err)
			}
		}

		lr, err := r.Lock(repo.Locator)
		if err != nil {
			return err
		}
		defer func() {
			if err := lr.Close(); err != nil {
				log.Error("closing repo", err)
			}
		}()

		if err != nil {
			log.Errorf("SetConfig err:%s", err.Error())
		}

		dbAddr := cctx.String("accesspoint-db")
		uuid := cctx.String("uuid")

		address := cctx.String("listen")
		addrSplit := strings.Split(address, ":")
		if len(addrSplit) < 2 {
			return fmt.Errorf("Listen address %s is error", address)
		}

		port, err := strconv.Atoi(addrSplit[1])
		if err != nil {
			return err
		}

		srv := &http.Server{
			Handler: WorkerHandler(locator.NewLocalLocator(ctx, lr, dbAddr, uuid, port), true),
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

		nl, err := net.Listen("tcp", address)
		if err != nil {
			return err
		}

		log.Infof("Titan locator server listen on %s", address)

		return srv.Serve(nl)
	},
}
