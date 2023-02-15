package main

import (
	"context"
	"crypto/tls"
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
	cliutil "github.com/linguohua/titan/cli/util"
	"github.com/linguohua/titan/lib/titanlog"
	"github.com/linguohua/titan/lib/ulimit"
	"github.com/linguohua/titan/metrics"
	"github.com/linguohua/titan/node/carfile/carfilestore"
	"github.com/linguohua/titan/node/config"
	"github.com/linguohua/titan/node/device"
	"github.com/linguohua/titan/node/repo"
	"github.com/quic-go/quic-go/http3"

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
				Usage:   fmt.Sprintf("Specify edge repo path. flag %s and env TITAN_EDGE_PATH are DEPRECATION, will REMOVE SOON", FlagEdgeRepoDeprecation),
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
				// Generate report in TITAN_EDGE_PATH and re-raise panic
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

		cfg, err := lr.Config()
		if err != nil {
			return err
		}

		edgeCfg := cfg.(*config.EdgeCfg)

		udpPacketConn, err := net.ListenPacket("udp", edgeCfg.ListenAddress)
		if err != nil {
			return err
		}
		defer udpPacketConn.Close()

		httpClient := cliutil.NewHttp3Client(udpPacketConn, edgeCfg.RootCertificatePath)
		jsonrpc.SetHttp3Client(httpClient)

		timeout, err := time.ParseDuration(edgeCfg.Timeout)
		if err != nil {
			return err
		}

		deviceID := cctx.String("device-id")
		secret := cctx.String("secret")

		// Connect to scheduler
		var schedulerAPI api.Scheduler
		var closer func()
		if edgeCfg.Locator {
			schedulerAPI, closer, err = newSchedulerAPI(cctx, deviceID, secret, timeout)
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

		v, err := getSchedulerVersion(schedulerAPI, timeout)
		if err != nil {
			return err
		}

		if v.APIVersion != api.SchedulerAPIVersion0 {
			return xerrors.Errorf("titan-scheduler API version doesn't match: expected: %s", api.APIVersion{APIVersion: api.SchedulerAPIVersion0})
		}
		log.Infof("Remote version %s", v)

		internalIP, err := extractRoutableIP(cctx, edgeCfg, timeout)
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
			Scheduler:    schedulerAPI,
			CarfileStore: carfileStore,
			Device:       device,
			PConn:        udpPacketConn,
		}

		edgeApi := edge.NewLocalEdgeNode(context.Background(), params)
		handler := EdgeHandler(schedulerAPI.AuthVerify, edgeApi, true)

		srv := &http.Server{
			Handler: handler,
			BaseContext: func(listener net.Listener) context.Context {
				ctx, _ := tag.New(context.Background(), tag.Upsert(metrics.APIInterface, "titan-edge"))
				return ctx
			},
		}

		go startUDPServer(udpPacketConn, handler, edgeCfg.CertificatePath, edgeCfg.PrivateKeyPath)

		go func() {
			<-ctx.Done()
			log.Warn("Shutting down...")
			if err := srv.Shutdown(context.TODO()); err != nil {
				log.Errorf("shutting down RPC server failed: %s", err)
			}
			log.Warn("Graceful shutdown successful")
		}()

		nl, err := net.Listen("tcp", edgeCfg.ListenAddress)
		if err != nil {
			return err
		}

		log.Infof("Edge listen on tcp %s", edgeCfg.ListenAddress)

		schedulerSession, err := getSchedulerSession(schedulerAPI, timeout)
		if err != nil {
			return xerrors.Errorf("getting scheduler session: %w", err)
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
					curSession, err := getSchedulerSession(schedulerAPI, timeout)
					if err != nil {
						errCount++
						log.Errorf("heartbeat: checking remote session failed: %+v", err)
					} else {
						if curSession != schedulerSession {
							schedulerSession = curSession
							break
						}

						if errCount > 0 {
							break
						}
					}

					select {
					case <-readyCh:
						err := connectToScheduler(schedulerAPI, timeout)
						if err != nil {
							log.Errorf("Registering edge failed: %+v", err)
							cancel()
							return
						}

						edge := edgeApi.(*edge.Edge)
						edge.LoadPublicKey(timeout)
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

func connectToScheduler(api api.Scheduler, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return api.EdgeNodeConnect(ctx)
}

func getSchedulerSession(api api.Scheduler, timeout time.Duration) (uuid.UUID, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	return api.Session(ctx)
}

func getExternalIP(api api.Scheduler, timeout time.Duration) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	return api.GetExternalIP(ctx)
}

func getSchedulerVersion(api api.Scheduler, timeout time.Duration) (api.APIVersion, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	return api.Version(ctx)
}

func extractRoutableIP(cctx *cli.Context, edgeCfg *config.EdgeCfg, timeout time.Duration) (string, error) {
	ainfo, err := lcli.GetAPIInfo(cctx, repo.Scheduler)
	if err != nil {
		return "", xerrors.Errorf("could not get scheduler API info: %w", err)
	}

	schedulerAddr := strings.Split(ainfo.Addr, "/")
	conn, err := net.DialTimeout("tcp", schedulerAddr[2], timeout)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.TCPAddr)

	return strings.Split(localAddr.IP.String(), ":")[0], nil
}

func newAuthTokenFromScheduler(schedulerURL, deviceID, secret string, timeout time.Duration) ([]byte, error) {
	schedulerAPI, closer, err := client.NewScheduler(context.Background(), schedulerURL, nil)
	if err != nil {
		return nil, err
	}

	defer closer()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	perms := []auth.Permission{api.PermRead, api.PermWrite}

	return schedulerAPI.AuthNodeNew(ctx, perms, deviceID, secret)
}

func newSchedulerAPI(cctx *cli.Context, deviceID string, securityKey string, timeout time.Duration) (api.Scheduler, jsonrpc.ClientCloser, error) {
	locator, closer, err := lcli.GetLocatorAPI(cctx)
	if err != nil {
		return nil, nil, err
	}
	defer closer()

	ctx, cancel := context.WithTimeout(context.TODO(), timeout)
	defer cancel()

	schedulerURLs, err := locator.GetAccessPoints(ctx, deviceID)
	if err != nil {
		return nil, nil, err
	}

	if len(schedulerURLs) <= 0 {
		return nil, nil, fmt.Errorf("edge %s can not get access point", deviceID)
	}

	schedulerURL := schedulerURLs[0]

	tokenBuf, err := newAuthTokenFromScheduler(schedulerURL, deviceID, securityKey, timeout)
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

func startUDPServer(conn net.PacketConn, handler http.Handler, certPath, privPath string) error {
	cert, err := tls.LoadX509KeyPair(certPath, privPath)
	if err != nil {
		log.Errorf("startUDPServer, LoadX509KeyPair error:%s", err.Error())
		return err
	}

	config := &tls.Config{
		Certificates:       []tls.Certificate{cert},
		InsecureSkipVerify: false,
	}

	srv := http3.Server{
		TLSConfig: config,
		Handler:   handler,
	}

	return srv.Serve(conn)
}
