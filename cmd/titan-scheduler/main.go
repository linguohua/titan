package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/go-jsonrpc/auth"
	"github.com/gbrlsnchs/jwt/v3"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/build"
	lcli "github.com/linguohua/titan/cli"
	cliutil "github.com/linguohua/titan/cli/util"
	"github.com/linguohua/titan/lib/titanlog"
	"github.com/linguohua/titan/lib/ulimit"
	"github.com/linguohua/titan/metrics"
	"github.com/linguohua/titan/node/config"
	"github.com/linguohua/titan/node/repo"
	"github.com/linguohua/titan/node/scheduler"
	"github.com/linguohua/titan/node/scheduler/db/cache"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"github.com/linguohua/titan/node/secret"
	"github.com/linguohua/titan/region"
	"github.com/quic-go/quic-go/http3"
	"go.opencensus.io/tag"
	"golang.org/x/xerrors"

	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
)

var log = logging.Logger("main")

const (
	// FlagSchedulerRepo Flag
	FlagSchedulerRepo = "scheduler-repo"

	// FlagSchedulerRepoDeprecation Flag
	FlagSchedulerRepoDeprecation = "schedulerrepo"
)

func main() {
	api.RunningNodeType = api.NodeScheduler

	titanlog.SetupLogLevels()

	local := []*cli.Command{
		runCmd,
		getAPIKeyCmd,
	}

	local = append(local, lcli.SchedulerCmds...)
	local = append(local, lcli.CommonCommands...)

	app := &cli.App{
		Name:                 "titan-scheduler",
		Usage:                "Titan scheduler node",
		Version:              build.UserVersion(),
		EnableBashCompletion: true,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    FlagSchedulerRepo,
				Aliases: []string{FlagSchedulerRepoDeprecation},
				EnvVars: []string{"TITAN_SCHEDULER_PATH", "SCHEDULER_PATH"},
				Value:   "~/.titanscheduler", // TODO: Consider XDG_DATA_HOME
				Usage:   fmt.Sprintf("Specify scheduler repo path. flag %s and env TITAN_SCHEDULER_PATH are DEPRECATION, will REMOVE SOON", FlagSchedulerRepoDeprecation),
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
				// Generate report in TITAN_SCHEDULER_PATH and re-raise panic
				build.GeneratePanicReport(c.String("panic-reports"), c.String(FlagSchedulerRepo), c.App.Name)
				log.Panic(r)
			}
			return nil
		},
		Commands: local,
	}
	app.Setup()
	app.Metadata["repoType"] = repo.Scheduler

	if err := app.Run(os.Args); err != nil {
		log.Warnf("%+v", err)
		return
	}
}

type jwtPayload struct {
	Allow []auth.Permission
}

var getAPIKeyCmd = &cli.Command{
	Name:  "get-api-key",
	Usage: "Generate API Key",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "perm",
			Usage: "permission to assign to the token, one of: read, write, sign, admin",
			Value: "",
		},
	},
	Action: func(cctx *cli.Context) error {
		lr, err := openRepo(cctx)
		if err != nil {
			return err
		}
		defer lr.Close() // nolint

		perm := cctx.String("perm")

		p := jwtPayload{}

		idx := 0
		for i, p := range api.AllPermissions {
			if auth.Permission(perm) == p {
				idx = i + 1
			}
		}

		if idx == 0 {
			return fmt.Errorf("--perm flag has to be one of: %s", api.AllPermissions)
		}

		p.Allow = api.AllPermissions[:idx]

		authKey, err := secret.APISecret(lr)
		if err != nil {
			return xerrors.Errorf("setting up api secret: %w", err)
		}

		k, err := jwt.Sign(&p, (*jwt.HMACSHA)(authKey))
		if err != nil {
			return xerrors.Errorf("jwt sign: %w", err)
		}

		fmt.Println(string(k))
		return nil
	},
}

var runCmd = &cli.Command{
	Name:  "run",
	Usage: "Start titan scheduler node",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Required: true,
			Name:     "cachedb-url",
			Usage:    "cachedb url",
			Value:    "127.0.0.1:6379",
		},
		&cli.StringFlag{
			Required: true,
			Name:     "geodb-path",
			Usage:    "geodb path, example: --geodb-path=../../geoip/geolite2_city/city.mmdb",
			Value:    "../../geoip/geolite2_city/city.mmdb",
		},
		&cli.StringFlag{
			Required: true,
			Name:     "persistentdb-url",
			Usage:    "persistentdb url",
			Value:    "user01:sql001@tcp(127.0.0.1:3306)/test",
		},
		&cli.StringFlag{
			Required: true,
			Name:     "server-id",
			Usage:    "server uniquely identifies",
		},
		&cli.StringFlag{
			Required: true,
			Name:     "area",
			Usage:    "area",
			Value:    "CN-GD-Shenzhen",
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

		// Open repo
		lr, err := openRepo(cctx)
		if err != nil {
			return err
		}

		cfg, err := lr.Config()
		if err != nil {
			return err
		}

		schedulerCfg := cfg.(*config.SchedulerCfg)

		// Connect to scheduler
		ctx := lcli.ReqContext(cctx)

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		cURL := cctx.String("cachedb-url")
		sID := cctx.String("server-id")
		area := cctx.String("area")
		if area == "" {
			log.Panic("area is nil")
		}

		if sID == "" {
			log.Panic("server-id is nil")
		}

		err = cache.NewCacheDB(cURL, cache.TypeRedis(), sID)
		if err != nil {
			log.Panic("Cache connect error: " + err.Error())
		}

		gPath := cctx.String("geodb-path")
		err = region.NewRegion(gPath, region.TypeGeoLite(), area)
		if err != nil {
			log.Panic("Load region file error: " + err.Error())
		}

		pPath := cctx.String("persistentdb-url")
		err = persistent.NewDB(pPath, persistent.TypeSQL(), sID, area)
		if err != nil {
			log.Panic("DB connect error: " + err.Error())
		}

		address := schedulerCfg.ListenAddress
		addressList := strings.Split(address, ":")
		portStr := addressList[1]
		port, err := strconv.Atoi(portStr)
		if err != nil {
			log.Panic("Parse port error: " + err.Error())
		}
		schedulerAPI := scheduler.NewLocalScheduleNode(lr, port)
		handler := schedulerHandler(schedulerAPI, true)

		srv := &http.Server{
			Handler: handler,
			BaseContext: func(listener net.Listener) context.Context {
				ctx, _ := tag.New(context.Background(), tag.Upsert(metrics.APIInterface, "titan-edge"))
				return ctx
			},
		}

		udpPacketConn, err := net.ListenPacket("udp", address)
		if err != nil {
			return err
		}
		defer udpPacketConn.Close()

		httpClient := cliutil.NewHttp3Client(udpPacketConn, schedulerCfg.InsecureSkipVerify, schedulerCfg.CaCertificatePath)
		jsonrpc.SetHttp3Client(httpClient)

		go startUDPServer(udpPacketConn, handler, schedulerCfg)

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

		log.Info("titan scheduler listen with:", address)

		return srv.Serve(nl)
	},
}

func openRepo(cctx *cli.Context) (repo.LockedRepo, error) {
	repoPath := cctx.String(FlagSchedulerRepo)
	r, err := repo.NewFS(repoPath)
	if err != nil {
		return nil, err
	}

	ok, err := r.Exists()
	if err != nil {
		return nil, err
	}
	if !ok {
		if err := r.Init(repo.Scheduler); err != nil {
			return nil, err
		}
	}

	lr, err := r.Lock(repo.Scheduler)
	if err != nil {
		return nil, err
	}

	return lr, nil
}

func startUDPServer(conn net.PacketConn, handler http.Handler, schedulerCfg *config.SchedulerCfg) error {
	var tlsConfig *tls.Config
	if schedulerCfg.InsecureSkipVerify {
		config, err := generateTLSConfig()
		if err != nil {
			log.Errorf("startUDPServer, generateTLSConfig error:%s", err.Error())
			return err
		}
		tlsConfig = config
	} else {
		cert, err := tls.LoadX509KeyPair(schedulerCfg.CaCertificatePath, schedulerCfg.PrivateKeyPath)
		if err != nil {
			log.Errorf("startUDPServer, LoadX509KeyPair error:%s", err.Error())
			return err
		}

		tlsConfig = &tls.Config{
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

func generateTLSConfig() (*tls.Config, error) {
	key, err := rsa.GenerateKey(rand.Reader, 1024)
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
		Certificates:       []tls.Certificate{tlsCert},
		InsecureSkipVerify: true,
	}, nil
}
