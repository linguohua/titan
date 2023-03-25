package main

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"os"
	"path"
	"strings"
	"time"

	"github.com/linguohua/titan/node"
	"github.com/linguohua/titan/node/carfile/store"
	"github.com/linguohua/titan/node/gateway"
	"github.com/linguohua/titan/node/modules/dtypes"

	"github.com/filecoin-project/go-jsonrpc"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/client"
	"github.com/linguohua/titan/api/types"
	"github.com/linguohua/titan/build"
	lcli "github.com/linguohua/titan/cli"
	cliutil "github.com/linguohua/titan/cli/util"
	"github.com/linguohua/titan/lib/titanlog"
	"github.com/linguohua/titan/metrics"
	"github.com/linguohua/titan/node/config"
	"github.com/linguohua/titan/node/repo"
	titanrsa "github.com/linguohua/titan/node/rsa"
	"github.com/quic-go/quic-go/http3"

	"github.com/google/uuid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
	"golang.org/x/xerrors"

	"github.com/linguohua/titan/stores"
)

var log = logging.Logger("main")

const (
	FlagEdgeRepo            = "edge-repo"
	FlagEdgeRepoDeprecation = "edgerepo"
	DefaultCarfileStoreDir  = "carfilestore"
)

func main() {
	types.RunningNodeType = types.NodeEdge
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
			Name:    "key-path",
			EnvVars: []string{"TITAN_SCHEDULER_KEY", "SCHEDULER_KEY"},
			Usage:   "private key path",
			Value:   "",
		},
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		log.Info("Starting titan edge node")

		// Register all metric views
		if err := view.Register(
			metrics.DefaultViews...,
		); err != nil {
			log.Fatalf("Cannot register the view: %v", err)
		}

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

		cfg, err := lr.Config()
		if err != nil {
			return err
		}

		edgeCfg := cfg.(*config.EdgeCfg)

		err = lr.Close()
		if err != nil {
			return err
		}

		// Connect to scheduler
		nodeID := cctx.String("device-id")

		privateKey, err := loadPrivateKey(cctx.String("key-path"), r)
		if err != nil {
			return err
		}

		connectTimeout, err := time.ParseDuration(edgeCfg.Timeout)
		if err != nil {
			return err
		}

		udpPacketConn, err := net.ListenPacket("udp", edgeCfg.ListenAddress)
		if err != nil {
			return err
		}
		defer udpPacketConn.Close() //nolint:errcheck  // ignore error

		// all jsonrpc client use udp
		httpClient, err := cliutil.NewHTTP3Client(udpPacketConn, edgeCfg.InsecureSkipVerify, edgeCfg.CaCertificatePath)
		if err != nil {
			return xerrors.Errorf("new http3 client error %w", err)
		}
		jsonrpc.SetHttp3Client(httpClient)

		url, err := schedulerURL(cctx, nodeID, edgeCfg.Locator)
		if err != nil {
			return err
		}

		schedulerAPI, closer, err := newSchedulerAPI(cctx, url, nodeID, privateKey)
		if err != nil {
			return err
		}
		defer closer()

		ctx := lcli.ReqContext(cctx)
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		v, err := getSchedulerVersion(schedulerAPI, connectTimeout)
		if err != nil {
			return err
		}

		if v.APIVersion != api.SchedulerAPIVersion0 {
			return xerrors.Errorf("titan-scheduler API version doesn't match: expected: %s", api.APIVersion{APIVersion: api.SchedulerAPIVersion0})
		}
		log.Infof("Remote version %s", v)

		var gw *gateway.Gateway
		var edgeAPI api.Edge
		stop, err := node.New(cctx.Context,
			node.Edge(&edgeAPI),
			node.Base(),
			node.Repo(r),
			node.Override(new(dtypes.NodeID), dtypes.NodeID(nodeID)),
			node.Override(new(api.Scheduler), schedulerAPI),
			node.Override(new(net.PacketConn), udpPacketConn),
			node.Override(new(dtypes.CarfileStorePath), func() dtypes.CarfileStorePath {
				carfileStorePath := edgeCfg.CarfileStorePath
				if len(carfileStorePath) == 0 {
					carfileStorePath = path.Join(lr.Path(), DefaultCarfileStoreDir)
				}

				log.Infof("carfilestorePath:%s", carfileStorePath)
				return dtypes.CarfileStorePath(carfileStorePath)
			}),
			node.Override(new(dtypes.InternalIP), func() (dtypes.InternalIP, error) {
				ainfo, err := lcli.GetAPIInfo(cctx, repo.Scheduler)
				if err != nil {
					return "", xerrors.Errorf("could not get scheduler API info: %w", err)
				}

				schedulerAddr := strings.Split(ainfo.Addr, "/")
				conn, err := net.DialTimeout("tcp", schedulerAddr[2], connectTimeout)
				if err != nil {
					return "", err
				}

				defer conn.Close() //nolint:errcheck
				localAddr := conn.LocalAddr().(*net.TCPAddr)

				return dtypes.InternalIP(strings.Split(localAddr.IP.String(), ":")[0]), nil
			}),

			node.Override(node.RunGateway, func(cs *store.CarfileStore) error {
				gw = gateway.NewGateway(cs, schedulerAPI, privateKey)

				return err
			}),
		)
		if err != nil {
			return xerrors.Errorf("creating node: %w", err)
		}

		handler := EdgeHandler(edgeAPI.AuthVerify, edgeAPI, true)
		handler = gw.NewHandler(handler)

		srv := &http.Server{
			ReadHeaderTimeout: 30 * time.Second,
			Handler:           handler,
			BaseContext: func(listener net.Listener) context.Context {
				ctx, _ := tag.New(context.Background(), tag.Upsert(metrics.APIInterface, "titan-edge"))
				return ctx
			},
		}

		go startUDPServer(udpPacketConn, handler, edgeCfg) //nolint:errcheck

		go func() {
			<-ctx.Done()
			log.Warn("Shutting down...")
			if err := srv.Shutdown(context.TODO()); err != nil {
				log.Errorf("shutting down RPC server failed: %s", err)
			}
			stop(ctx) //nolint:errcheck
			log.Warn("Graceful shutdown successful")
		}()

		nl, err := net.Listen("tcp", edgeCfg.ListenAddress)
		if err != nil {
			return err
		}

		log.Infof("Edge listen on tcp %s", edgeCfg.ListenAddress)

		schedulerSession, err := getSchedulerSession(schedulerAPI, connectTimeout)
		if err != nil {
			return xerrors.Errorf("getting scheduler session: %w", err)
		}

		waitQuietCh := func() chan struct{} {
			out := make(chan struct{})
			go func() {
				ctx2 := context.Background()
				err = edgeAPI.WaitQuiet(ctx2)
				if err != nil {
					log.Errorf("wait quiet error %s", err.Error())
				}
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

				token, err := edgeAPI.AuthNew(cctx.Context, api.AllPermissions)
				if err != nil {
					log.Errorf("auth new error %s", err.Error())
					return
				}

				errCount := 0
				for {
					curSession, err := getSchedulerSession(schedulerAPI, connectTimeout)
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
						if err := schedulerAPI.EdgeNodeConnect(ctx, token); err != nil {
							log.Errorf("Registering edge failed: %s", err.Error())
							cancel()
							return
						}

						pk, err := getSchedulerPublicKey(schedulerAPI)
						if err != nil {
							log.Errorf("get scheduler public key failed: %s", err.Error())
							cancel()
							return
						}

						gw.SetSchedulerPublicKey(pk)
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

func getSchedulerSession(api api.Scheduler, timeout time.Duration) (uuid.UUID, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	return api.Session(ctx)
}

func getSchedulerVersion(api api.Scheduler, timeout time.Duration) (api.APIVersion, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	return api.Version(ctx)
}

func newAuthTokenFromScheduler(schedulerURL, nodeID string, privateKey *rsa.PrivateKey) (string, error) {
	schedulerAPI, closer, err := client.NewScheduler(context.Background(), schedulerURL, nil)
	if err != nil {
		return "", err
	}

	defer closer()

	rsa := titanrsa.New(crypto.SHA256, crypto.SHA256.New())
	sign, err := rsa.Sign(privateKey, []byte(nodeID))
	if err != nil {
		return "", err
	}

	return schedulerAPI.NodeAuthNew(context.Background(), nodeID, hex.EncodeToString(sign))
}

func getAccessPoint(cctx *cli.Context, nodeID string) (string, error) {
	locator, closer, err := lcli.GetLocatorAPI(cctx)
	if err != nil {
		return "", err
	}
	defer closer()

	schedulerURLs, err := locator.GetAccessPoints(context.Background(), nodeID)
	if err != nil {
		return "", err
	}

	if len(schedulerURLs) <= 0 {
		return "", fmt.Errorf("edge %s can not get access point", nodeID)
	}

	return schedulerURLs[0], nil
}

func schedulerURL(cctx *cli.Context, nodeID string, isPassLocator bool) (string, error) {
	if isPassLocator {
		schedulerURL, err := getAccessPoint(cctx, nodeID)
		if err != nil {
			return "", err
		}

		return schedulerURL, nil
	}

	schedulerURL, _, err := lcli.GetRawAPI(cctx, repo.Scheduler, "v0")
	if err != nil {
		return "", err
	}

	return schedulerURL, nil
}

func newSchedulerAPI(cctx *cli.Context, schedulerURL, nodeID string, privateKey *rsa.PrivateKey) (api.Scheduler, jsonrpc.ClientCloser, error) {
	token, err := newAuthTokenFromScheduler(schedulerURL, nodeID, privateKey)
	if err != nil {
		return nil, nil, err
	}

	headers := http.Header{}
	headers.Add("Authorization", "Bearer "+token)
	headers.Add("Node-ID", nodeID)

	schedulerAPI, closer, err := client.NewScheduler(context.Background(), schedulerURL, headers)
	if err != nil {
		return nil, nil, err
	}
	log.Infof("scheduler url:%s, token:%s", schedulerURL, token)

	if err := os.Setenv("SCHEDULER_API_INFO", token+":"+schedulerURL); err != nil {
		log.Errorf("set env error: %s", err.Error())
	}

	return schedulerAPI, closer, nil
}

func startUDPServer(conn net.PacketConn, handler http.Handler, edgeCfg *config.EdgeCfg) error {
	var tlsConfig *tls.Config
	if edgeCfg.InsecureSkipVerify {
		config, err := defaultTLSConfig()
		if err != nil {
			log.Errorf("startUDPServer, defaultTLSConfig error:%s", err.Error())
			return err
		}
		tlsConfig = config
	} else {
		cert, err := tls.LoadX509KeyPair(edgeCfg.CaCertificatePath, edgeCfg.PrivateKeyPath)
		if err != nil {
			log.Errorf("startUDPServer, LoadX509KeyPair error:%s", err.Error())
			return err
		}

		tlsConfig = &tls.Config{
			MinVersion:         tls.VersionTLS12,
			Certificates:       []tls.Certificate{cert},
			InsecureSkipVerify: false,
		}
	}

	srv := http3.Server{
		TLSConfig: tlsConfig,
		Handler:   handler,
	}

	return srv.Serve(conn)
}

func defaultTLSConfig() (*tls.Config, error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, err
	}
	template := x509.Certificate{SerialNumber: big.NewInt(1)}
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		MinVersion:         tls.VersionTLS12,
		Certificates:       []tls.Certificate{tlsCert},
		InsecureSkipVerify: true, //nolint:gosec // skip verify in default config
	}, nil
}

func loadPrivateKey(path string, r *repo.FsRepo) (*rsa.PrivateKey, error) {
	if len(path) > 0 {
		pem, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		return titanrsa.Pem2PrivateKey(pem)
	}

	pem, err := r.PrivateKey()
	if err != nil {
		return nil, err
	}
	return titanrsa.Pem2PrivateKey(pem)

}

func getSchedulerPublicKey(schedulerAPI api.Scheduler) (*rsa.PublicKey, error) {
	pem, err := schedulerAPI.PublicKey(context.Background())
	if err != nil {
		return nil, err
	}

	return titanrsa.Pem2PublicKey([]byte(pem))
}
