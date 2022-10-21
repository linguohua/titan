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
	"strings"
	"time"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/build"
	lcli "github.com/linguohua/titan/cli"
	"github.com/linguohua/titan/lib/titanlog"
	"github.com/linguohua/titan/lib/ulimit"
	"github.com/linguohua/titan/metrics"
	"github.com/linguohua/titan/node/device"
	"github.com/linguohua/titan/node/helper"
	"github.com/linguohua/titan/node/repo"
	"github.com/shirou/gopsutil/v3/cpu"

	"github.com/google/uuid"
	logging "github.com/ipfs/go-log/v2"
	manet "github.com/multiformats/go-multiaddr/net"
	"github.com/urfave/cli/v2"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
	"golang.org/x/xerrors"

	"github.com/linguohua/titan/node/candidate"
	"github.com/linguohua/titan/stores"
)

var log = logging.Logger("main")

const FlagWorkerRepo = "candidate-repo"

// TODO remove after deprecation period
const FlagWorkerRepoDeprecation = "candidaterepo"

func main() {
	api.RunningNodeType = api.NodeEdge
	cpu.Percent(0, false)
	titanlog.SetupLogLevels()

	local := []*cli.Command{
		runCmd,
	}

	local = append(local, lcli.CommonCommands...)

	app := &cli.App{
		Name:                 "titan-candidate",
		Usage:                "Titan candidate node",
		Version:              build.UserVersion(),
		EnableBashCompletion: true,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    FlagWorkerRepo,
				Aliases: []string{FlagWorkerRepoDeprecation},
				EnvVars: []string{"TITAN_CANDIDATE_PATH", "CANDIDATE_PATH"},
				Value:   "~/.titancandidate", // TODO: Consider XDG_DATA_HOME
				Usage:   fmt.Sprintf("Specify worker repo path. flag %s and env TITAN_EDGE_PATH are DEPRECATION, will REMOVE SOON", FlagWorkerRepoDeprecation),
			},
			&cli.StringFlag{
				Name:    "panic-reports",
				EnvVars: []string{"TITAN_PANIC_REPORT_PATH"},
				Hidden:  true,
				Value:   "~/.titancandidate", // should follow --repo default
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
	Usage: "Start titan candidate node",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "listen",
			Usage: "host address and port the worker api will listen on",
			Value: "0.0.0.0:3456",
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
			Value: "123456789000000001", // should follow --repo default
		},
		&cli.StringFlag{
			Name:  "public-ip",
			Usage: "network external ip, example: --public-ip=218.72.111.105",
			Value: "218.72.111.105", // should follow --repo default
		},
		&cli.StringFlag{
			Name:  "blockstore-path",
			Usage: "block store path, example: --blockstore-path=./blockstore",
			Value: "./blockstore", // should follow --repo default
		},
		&cli.StringFlag{
			Name:  "blockstore-type",
			Usage: "block store type is FileStore or RocksDB, example: --blockstore-type=FileStore",
			Value: "FileStore", // should follow --repo default
		},
		&cli.StringFlag{
			Name:  "download-srv-key",
			Usage: "download server key for who download block, example: --download-srv-key=KK20FeKPsE3qwQgR",
			Value: "KK20FeKPsE3qwQgR", // should follow --repo default
		},
		&cli.StringFlag{
			Name:  "download-srv-addr",
			Usage: "download server address for who download block, example: --download-srv-addr=192.168.0.136:3000",
			Value: "0.0.0.0:3000", // should follow --repo default
		},
		&cli.Int64Flag{
			Name:  "bandwidth-up",
			Usage: "upload file bandwidth, unit is B/s example set 100MB/s: --bandwidth-up=104857600",
			Value: 1073741824, // should follow --repo default
		},
		&cli.Int64Flag{
			Name:  "bandwidth-down",
			Usage: "download file bandwidth, unit is B/s example set 100MB/s: --bandwidth-down=104857600",
			Value: 1073741824, // should follow --repo default
		},
		&cli.StringFlag{
			Name:  "tcp-srv-addr",
			Usage: "tcp server addr, use by edge node validate data: --tcp-srv-addr=0.0.0.0:4000",
			Value: "0.0.0.0:4000", // should follow --repo default
		},
		&cli.BoolFlag{
			Name:  "is-external",
			Usage: "internal network or externa network",
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

		// Connect to scheduler
		var schedulerAPI api.Scheduler
		var closer func()
		schedulerAPI, closer, err = lcli.GetSchedulerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := lcli.ReqContext(cctx)
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		v, err := schedulerAPI.Version(ctx)
		if err != nil {
			return err
		}
		if v.APIVersion != api.SchedulerAPIVersion0 {
			return xerrors.Errorf("titan-scheduler API version doesn't match: expected: %s", api.APIVersion{APIVersion: api.SchedulerAPIVersion0})
		}
		log.Infof("Remote version %s", v)

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
		const unspecifiedAddress = "0.0.0.0"
		address := cctx.String("listen")
		addressSlice := strings.Split(address, ":")
		if ip := net.ParseIP(addressSlice[0]); ip != nil && !cctx.Bool("is-external") {
			if ip.String() == unspecifiedAddress {
				timeout, err := time.ParseDuration(cctx.String("timeout"))
				if err != nil {
					return err
				}
				rip, err := extractRoutableIP(timeout)
				if err != nil {
					return err
				}
				address = rip + ":" + addressSlice[1]
			}
		}

		deviceID := cctx.String("device-id")
		blockStore := stores.NewBlockStore(cctx.String("blockstore-path"), cctx.String("blockstore-type"))
		device := device.NewDevice(
			blockStore,
			deviceID,
			cctx.String("public-ip"),
			strings.Split(address, ":")[0],
			cctx.Int64("bandwidth-up"),
			cctx.Int64("bandwidth-down"))

		nodeParams := &helper.NodeParams{
			DS:              ds,
			Scheduler:       schedulerAPI,
			BlockStore:      blockStore,
			DownloadSrvKey:  cctx.String("download-srv-key"),
			DownloadSrvAddr: cctx.String("download-srv-addr"),
			IsExternal:      cctx.Bool("is-external"),
		}

		tcpSrvAddr := cctx.String("tcp-srv-addr")

		candidateApi := candidate.NewLocalCandidateNode(context.Background(), tcpSrvAddr, device, nodeParams)

		log.Info("Setting up control endpoint at " + address)

		srv := &http.Server{
			Handler: WorkerHandler(schedulerAPI.AuthVerify, candidateApi, true),
			BaseContext: func(listener net.Listener) context.Context {
				ctx, _ := tag.New(context.Background(), tag.Upsert(metrics.APIInterface, "titan-candidate"))
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

		{
			a, err := net.ResolveTCPAddr("tcp", address)
			if err != nil {
				return xerrors.Errorf("parsing address: %w", err)
			}

			ma, err := manet.FromNetAddr(a)
			if err != nil {
				return xerrors.Errorf("creating api multiaddress: %w", err)
			}

			if err := lr.SetAPIEndpoint(ma); err != nil {
				return xerrors.Errorf("setting api endpoint: %w", err)
			}

			ainfo, err := lcli.GetAPIInfo(cctx, repo.StorageMiner)
			if err != nil {
				return xerrors.Errorf("could not get miner API info: %w", err)
			}

			// TODO: ideally this would be a token with some permissions dropped
			if err := lr.SetAPIToken(ainfo.Token); err != nil {
				return xerrors.Errorf("setting api token: %w", err)
			}
		}

		minerSession, err := schedulerAPI.Session(ctx, deviceID)
		if err != nil {
			return xerrors.Errorf("getting miner session: %w", err)
		}

		waitQuietCh := func() chan struct{} {
			out := make(chan struct{})
			go func() {
				ctx2 := context.Background()
				candidateApi.WaitQuiet(ctx2)
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

				for {
					curSession, err := schedulerAPI.Session(ctx, deviceID)
					if err != nil {
						log.Errorf("heartbeat: checking remote session failed: %+v", err)
					} else {
						if curSession != minerSession {
							minerSession = curSession
							break
						}
					}

					select {
					case <-readyCh:
						if cctx.Bool("is-external") {
							address = cctx.String("public-ip") + ":" + addressSlice[1]
						}
						if err := schedulerAPI.CandidateNodeConnect(ctx, "http://"+address+"/rpc/v0", ""); err != nil {
							log.Errorf("Registering worker failed: %+v", err)
							cancel()
							return
						}

						log.Info("Worker registered successfully, waiting for tasks")

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

func extractRoutableIP(timeout time.Duration) (string, error) {
	schedulerMultiAddrKey := "SCHEDULER_API_INFO"
	env, ok := os.LookupEnv(schedulerMultiAddrKey)
	if !ok {
		// TODO remove after deprecation period
		return "", xerrors.New("SCHEDULER_API_INFO environment variable required to extract IP")
	}

	schedulerAddr := strings.Split(env, "/")
	conn, err := net.DialTimeout("tcp", schedulerAddr[2]+":"+schedulerAddr[4], timeout)
	if err != nil {
		return "", err
	}
	defer conn.Close() //nolint:errcheck

	localAddr := conn.LocalAddr().(*net.TCPAddr)

	return strings.Split(localAddr.IP.String(), ":")[0], nil
}
