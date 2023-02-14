package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"path"
	"strings"
	"time"

	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/go-jsonrpc/auth"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/client"
	"github.com/linguohua/titan/build"
	lcli "github.com/linguohua/titan/cli"
	"github.com/linguohua/titan/lib/titanlog"
	"github.com/linguohua/titan/lib/ulimit"
	"github.com/linguohua/titan/metrics"
	"github.com/linguohua/titan/node/carfile/carfilestore"
	"github.com/linguohua/titan/node/config"
	"github.com/linguohua/titan/node/device"
	"github.com/linguohua/titan/node/helper"
	"github.com/linguohua/titan/node/repo"

	"github.com/google/uuid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
	"golang.org/x/xerrors"

	"github.com/linguohua/titan/node/edge"
	"github.com/linguohua/titan/stores"
)

var log = logging.Logger("main")

const (
	FlagEdgeRepo            = "edge-repo"
	FlagEdgeRepoDeprecation = "edgerepo"
	DefaultCarfileStoreDir  = "carfilestore"
)

func main() {
	api.RunningNodeType = api.NodeEdge
	titanlog.SetupLogLevels()
	local := []*cli.Command{
		runCmd,
	}

	local = append(local, lcli.CommonCommands...)

	app := &cli.App{
		Name:                 "titan-edge",
		Usage:                "Titan edge node",
		Version:              build.UserVersion(),
		EnableBashCompletion: true,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    FlagEdgeRepo,
				Aliases: []string{FlagEdgeRepoDeprecation},
				EnvVars: []string{"TITAN_EDGE_PATH", "EDGE_PATH"},
				Value:   "~/.titanedge", // TODO: Consider XDG_DATA_HOME
				Usage:   fmt.Sprintf("Specify worker repo path. flag %s and env TITAN_EDGE_PATH are DEPRECATION, will REMOVE SOON", FlagEdgeRepoDeprecation),
			},
			&cli.StringFlag{
				Name:    "panic-reports",
				EnvVars: []string{"TITAN_PANIC_REPORT_PATH"},
				Hidden:  true,
				Value:   "~/.titanedge", // should follow --repo default
			},
		},

		After: func(c *cli.Context) error {
			if r := recover(); r != nil {
				// Generate report in LOTUS_PATH and re-raise panic
				build.GeneratePanicReport(c.String("panic-reports"), c.String(FlagEdgeRepo), c.App.Name)
				panic(r)
			}
			return nil
		},
		Commands: append(local, lcli.EdgeCmds...),
	}
	app.Setup()
	app.Metadata["repoType"] = repo.Edge

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
			Required: true,
			Name:     "device-id",
			Usage:    "example: --device-id=b26fb231-e986-42de-a5d9-7b512a35543d",
			Value:    "",
		},
		&cli.StringFlag{
			Required: true,
			Name:     "secret",
			EnvVars:  []string{"TITAN_SCHEDULER_KEY", "SCHEDULER_KEY"},
			Usage:    "used auth edge node when connect to scheduler",
			Value:    "",
		},
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		log.Info("Starting titan edge node")

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
		// Register all metric views
		if err := view.Register(
			metrics.DefaultViews...,
		); err != nil {
			log.Fatalf("Cannot register the view: %v", err)
		}

		// Open repo
		repoPath := cctx.String(FlagEdgeRepo)
		r, err := repo.NewFS(repoPath)
		if err != nil {
			return err
		}

		ok, err := r.Exists()
		if err != nil {
			return err
		}
		if !ok {
			if err := r.Init(repo.Edge); err != nil {
				return err
			}

			lr, err := r.Lock(repo.Edge)
			if err != nil {
				return err
			}

			// init datastore for r.Exists
			_, err = lr.Datastore(context.Background(), "/metadata")
			if err != nil {
				return err
			}
			if err := lr.Close(); err != nil {
				return xerrors.Errorf("close repo: %w", err)
			}
		}

		lr, err := r.Lock(repo.Edge)
		if err != nil {
			return err
		}
		defer func() {
			if err := lr.Close(); err != nil {
				log.Error("closing repo", err)
			}
		}()
		ds, err := lr.Datastore(context.Background(), "/metadata")
		if err != nil {
			return err
		}

		log.Info("Opening local storage; connecting to scheduler")

		cfg, err := lr.Config()
		if err != nil {
			return err
		}

		edgeCfg := cfg.(*config.EdgeCfg)

		deviceID := cctx.String("device-id")
		secret := cctx.String("secret")

		// Connect to scheduler
		var schedulerAPI api.Scheduler
		var closer func()
		if edgeCfg.Locator {
			schedulerAPI, closer, err = newSchedulerAPI(cctx, deviceID, secret)
		} else {
			schedulerAPI, closer, err = lcli.GetSchedulerAPI(cctx, deviceID)
		}
		if err != nil {
			return err
		}
		defer closer()

		ctx := lcli.ReqContext(cctx)
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		v, err := getSchedulerVersion(schedulerAPI)
		if err != nil {
			return err
		}

		if v.APIVersion != api.SchedulerAPIVersion0 {
			return xerrors.Errorf("titan-scheduler API version doesn't match: expected: %s", api.APIVersion{APIVersion: api.SchedulerAPIVersion0})
		}
		log.Infof("Remote version %s", v)

		internalIP, err := extractRoutableIP(cctx, edgeCfg)
		if err != nil {
			return err
		}

		externalIP, err := getExternalIP(schedulerAPI)
		if err != nil {
			return err
		}

		carfileStorePath := edgeCfg.CarfilestorePath
		if len(carfileStorePath) == 0 {
			carfileStorePath = path.Join(lr.Path(), DefaultCarfileStoreDir)
		}

		log.Infof("carfilestorePath:%s", carfileStorePath)

		carfileStore := carfilestore.NewCarfileStore(carfileStorePath, edgeCfg.CarfilestoreType)

		device := device.NewDevice(
			deviceID,
			internalIP,
			edgeCfg.BandwidthUp,
			edgeCfg.BandwidthDown,
			carfileStore)

		params := &edge.EdgeParams{
			DS:           ds,
			Scheduler:    schedulerAPI,
			CarfileStore: carfileStore,
		}

		edgeApi := edge.NewLocalEdgeNode(context.Background(), device, params)

		srv := &http.Server{
			Handler: WorkerHandler(schedulerAPI.AuthVerify, edgeApi, true),
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

		address := edgeCfg.ListenAddress
		nl, err := net.Listen("tcp", address)
		if err != nil {
			return err
		}

		log.Infof("Edge listen on %s", address)

		addressSlice := strings.Split(address, ":")
		rpcURL := fmt.Sprintf("http://%s:%s/rpc/v0", externalIP, addressSlice[1])

		minerSession, err := getSchedulerSession(schedulerAPI, deviceID)
		if err != nil {
			return xerrors.Errorf("getting miner session: %w", err)
		}

		waitQuietCh := func() chan struct{} {
			out := make(chan struct{})
			go func() {
				ctx2 := context.Background()
				edgeApi.WaitQuiet(ctx2)
				close(out)
			}()
			return out
		}

		go func() {
			heartbeats := time.NewTicker(stores.HeartbeatInterval)
			defer heartbeats.Stop()

			var readyCh chan struct{}
			for {
				// TODO: we could get rid of this, but that requires tracking resources for restarted tasks correctly
				if readyCh == nil {
					log.Info("Making sure no local tasks are running")
					readyCh = waitQuietCh()
				}

				errCount := 0
				for {
					curSession, err := getSchedulerSession(schedulerAPI, deviceID)
					if err != nil {
						errCount++
						log.Errorf("heartbeat: checking remote session failed: %+v", err)
					} else {
						if curSession != minerSession {
							minerSession = curSession
							break
						}

						if errCount > 0 {
							break
						}
					}

					select {
					case <-readyCh:
						err := connectToScheduler(schedulerAPI, rpcURL, "")
						if err != nil {
							log.Errorf("Registering edge failed: %+v", err)
							cancel()
							return
						}

						edge := edgeApi.(*edge.Edge)
						edge.LoadPublicKey()
						log.Info("Edge registered successfully, waiting for tasks")
						errCount = 0
						readyCh = nil
					case <-heartbeats.C:
					case <-ctx.Done():
						return // graceful shutdown
					}
				}

				log.Errorf("TITAN-EDGE CONNECTION LOST")
			}
		}()

		return srv.Serve(nl)
	},
}

func connectToScheduler(api api.Scheduler, rpcURL string, downloadSrvURL string) error {
	ctx, cancel := context.WithTimeout(context.Background(), helper.SchedulerApiTimeout*time.Second)
	defer cancel()
	return api.EdgeNodeConnect(ctx, rpcURL, downloadSrvURL)
}

func getSchedulerSession(api api.Scheduler, deviceID string) (uuid.UUID, error) {
	ctx, cancel := context.WithTimeout(context.Background(), helper.SchedulerApiTimeout*time.Second)
	defer cancel()

	return api.Session(ctx, deviceID)
}

func getExternalIP(api api.Scheduler) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), helper.SchedulerApiTimeout*time.Second)
	defer cancel()

	return api.GetExternalIP(ctx)
}

func getSchedulerVersion(api api.Scheduler) (api.APIVersion, error) {
	ctx, cancel := context.WithTimeout(context.Background(), helper.SchedulerApiTimeout*time.Second)
	defer cancel()

	return api.Version(ctx)
}

func extractRoutableIP(cctx *cli.Context, edgeCfg *config.EdgeCfg) (string, error) {
	timeout, err := time.ParseDuration(edgeCfg.Timeout)
	if err != nil {
		return "", err
	}

	ainfo, err := lcli.GetAPIInfo(cctx, repo.Scheduler)
	if err != nil {
		return "", xerrors.Errorf("could not get scheduler API info: %w", err)
	}

	schedulerAddr := strings.Split(ainfo.Addr, "/")
	conn, err := net.DialTimeout("tcp", schedulerAddr[2], timeout)
	if err != nil {
		return "", err
	}
	defer conn.Close() //nolint:errcheck

	localAddr := conn.LocalAddr().(*net.TCPAddr)

	return strings.Split(localAddr.IP.String(), ":")[0], nil
}

func newAuthTokenFromScheduler(schedulerURL, deviceID, secret string) ([]byte, error) {
	schedulerAPI, closer, err := client.NewScheduler(context.Background(), schedulerURL, nil)
	if err != nil {
		return nil, err
	}

	defer closer()

	ctx, cancel := context.WithTimeout(context.Background(), helper.SchedulerApiTimeout*time.Second)
	defer cancel()

	perms := []auth.Permission{api.PermRead, api.PermWrite}

	return schedulerAPI.AuthNodeNew(ctx, perms, deviceID, secret)

}

func newSchedulerAPI(cctx *cli.Context, deviceID string, securityKey string) (api.Scheduler, jsonrpc.ClientCloser, error) {
	locator, closer, err := lcli.GetLocatorAPI(cctx)
	if err != nil {
		return nil, nil, err
	}
	defer closer()

	ctx, cancel := context.WithTimeout(context.TODO(), 15*time.Second)
	defer cancel()

	schedulerURLs, err := locator.GetAccessPoints(ctx, deviceID)
	if err != nil {
		return nil, nil, err
	}

	if len(schedulerURLs) <= 0 {
		return nil, nil, fmt.Errorf("edge %s can not get access point", deviceID)
	}

	schedulerURL := schedulerURLs[0]

	tokenBuf, err := newAuthTokenFromScheduler(schedulerURL, deviceID, securityKey)
	if err != nil {
		return nil, nil, err
	}

	token := string(tokenBuf)

	headers := http.Header{}
	headers.Add("Authorization", "Bearer "+token)
	headers.Add("Device-ID", deviceID)

	schedulerAPI, closer, err := client.NewScheduler(ctx, schedulerURL, headers)
	if err != nil {
		return nil, nil, err
	}
	log.Infof("scheduler url:%s, token:%s", schedulerURL, token)
	os.Setenv("SCHEDULER_API_INFO", token+":"+schedulerURL)
	return schedulerAPI, closer, nil
}
