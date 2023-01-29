package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path"
	"path/filepath"
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
	"github.com/linguohua/titan/node/device"
	"github.com/linguohua/titan/node/helper"
	"github.com/linguohua/titan/node/repo"
	"github.com/shirou/gopsutil/v3/cpu"

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
	FlagWorkerRepo            = "edge-repo"
	FlagWorkerRepoDeprecation = "edgerepo"
	DefaultCarfileStoreDir    = "carfilestore"
)

func main() {
	api.RunningNodeType = api.NodeEdge
	cpu.Percent(0, false)
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
				Name:    FlagWorkerRepo,
				Aliases: []string{FlagWorkerRepoDeprecation},
				EnvVars: []string{"TITAN_EDGE_PATH", "EDGE_PATH"},
				Value:   "~/.titanedge", // TODO: Consider XDG_DATA_HOME
				Usage:   fmt.Sprintf("Specify worker repo path. flag %s and env TITAN_EDGE_PATH are DEPRECATION, will REMOVE SOON", FlagWorkerRepoDeprecation),
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
				build.GeneratePanicReport(c.String("panic-reports"), c.String(FlagWorkerRepo), c.App.Name)
				panic(r)
			}
			return nil
		},
		Commands: append(local, lcli.EdgeCmds...),
	}
	app.Setup()
	app.Metadata["repoType"] = repo.Worker

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
			Value: "0.0.0.0:1234",
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
			Name:  "device-id",
			Usage: "network external ip, example: --device-id=b26fb231-e986-42de-a5d9-7b512a35543d",
			Value: "525e7729506711ed8c2c902e1671f843",
		},
		&cli.StringFlag{
			Name:  "carfilestore-path",
			Usage: "block store path, example: --carfilestore-path=./carfilestore",
			Value: "",
		},
		&cli.StringFlag{
			Name:  "blockstore-type",
			Usage: "block store type is FileStore or RocksDB, example: --blockstore-type=FileStore",
			Value: "FileStore",
		},
		&cli.StringFlag{
			Name:  "download-srv-key",
			Usage: "download server key for who download block, example: --download-srv-key=KK20FeKPsE3qwQgR",
			Value: "KK20FeKPsE3qwQgR",
		},
		&cli.StringFlag{
			Name:  "download-srv-addr",
			Usage: "download server address for who download block, example: --download-srv-addr=192.168.0.136:3000",
			Value: "0.0.0.0:3000",
		},
		&cli.StringFlag{
			Name:  "bandwidth-up",
			Usage: "upload file bandwidth, unit is B/s example set 100MB/s: --bandwidth-up=104857600",
			Value: "104857600",
		},
		&cli.StringFlag{
			Name:  "bandwidth-down",
			Usage: "download file bandwidth, unit is B/s example set 100MB/s: --bandwidth-down=104857600",
			Value: "1073741824",
		},
		&cli.StringFlag{
			Name:    "secret",
			EnvVars: []string{"TITAN_SCHEDULER_KEY", "SCHEDULER_KEY"},
			Usage:   "connect to scheduler key",
			Value:   "2521c39087cecd74a853850dd56e9c859b786fbc",
		},
		&cli.BoolFlag{
			Name:  "locator",
			Usage: "connect to locator get scheduler url",
			Value: false,
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
		isLocator := cctx.Bool("locator")
		deviceID := cctx.String("device-id")
		securityKey := cctx.String("secret")
		// Connect to scheduler
		var schedulerAPI api.Scheduler
		var closer func()
		if isLocator {
			schedulerAPI, closer, err = newSchedulerAPI(cctx, deviceID, securityKey)
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

		// tk, err := schedulerAPI.GetToken(ctx, deviceID, securityKey)
		// if err != nil {
		// 	return err
		// }

		// Register all metric views
		if err := view.Register(
			metrics.DefaultViews...,
		); err != nil {
			log.Fatalf("Cannot register the view: %v", err)
		}

		// Open repo
		repoPath := cctx.String(FlagWorkerRepo)
		r, err := repo.NewFS(repoPath)
		if err != nil {
			return err
		}

		ok, err := r.Exists()
		if err != nil {
			return err
		}
		if !ok {
			if err := r.Init(repo.Worker); err != nil {
				return err
			}

			lr, err := r.Lock(repo.Worker)
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

		lr, err := r.Lock(repo.Worker)
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

		internalIP, err := extractRoutableIP(cctx)
		if err != nil {
			return err
		}

		externalIP, err := getExternalIP(schedulerAPI)
		if err != nil {
			return err
		}

		carfileStorePath := cctx.String("carfilestore-path")
		if len(carfileStorePath) == 0 {
			carfileStorePath = path.Join(lr.Path(), DefaultCarfileStoreDir)
		}

		log.Infof("carfilestorePath:%s", carfileStorePath)

		carfileStore := carfilestore.NewCarfileStore(carfileStorePath, cctx.String("blockstore-type"))

		device := device.NewDevice(
			deviceID,
			externalIP,
			internalIP,
			cctx.Int64("bandwidth-up"),
			cctx.Int64("bandwidth-down"),
			carfileStore)

		params := &helper.NodeParams{
			DS:              ds,
			Scheduler:       schedulerAPI,
			CarfileStore:    carfileStore,
			DownloadSrvKey:  cctx.String("download-srv-key"),
			DownloadSrvAddr: cctx.String("download-srv-addr"),
			IPFSAPI:         cctx.String("ipfs-api"),
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

		address := cctx.String("listen")
		nl, err := net.Listen("tcp", address)
		if err != nil {
			return err
		}

		log.Infof("Edge listen on %s", address)

		addressSlice := strings.Split(address, ":")
		rpcURL := fmt.Sprintf("http://%s:%s/rpc/v0", externalIP, addressSlice[1])

		edge := edgeApi.(*edge.Edge)
		downloadSrvURL := edge.GetDownloadSrvURL()

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
						err := edgeNodeConnect(schedulerAPI, rpcURL, downloadSrvURL)
						if err != nil {
							log.Errorf("Registering edge failed: %+v", err)
							cancel()
							return
						}

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

func edgeNodeConnect(api api.Scheduler, rpcURL string, downloadSrvURL string) error {
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

func extractRoutableIP(cctx *cli.Context) (string, error) {
	timeout, err := time.ParseDuration(cctx.String("timeout"))
	if err != nil {
		return "", err
	}

	ainfo, err := lcli.GetAPIInfo(cctx, repo.FullNode)
	if err != nil {
		return "", xerrors.Errorf("could not get FullNode API info: %w", err)
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
	os.Setenv("FULLNODE_API_INFO", token+":"+schedulerURL)
	return schedulerAPI, closer, nil
}
