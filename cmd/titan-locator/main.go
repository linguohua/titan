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
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/build"
	lcli "github.com/linguohua/titan/cli"
	cliutil "github.com/linguohua/titan/cli/util"
	"github.com/linguohua/titan/lib/titanlog"
	"github.com/linguohua/titan/lib/ulimit"
	"github.com/linguohua/titan/metrics"
	"github.com/linguohua/titan/node/config"
	"github.com/linguohua/titan/node/locator"
	"github.com/linguohua/titan/node/repo"
	"github.com/linguohua/titan/region"
	"github.com/quic-go/quic-go/http3"

	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
	"golang.org/x/xerrors"
)

var log = logging.Logger("main")

const FlagLocatorRepo = "locator-repo"

// TODO remove after deprecation period
const FlagLocatorRepoDeprecation = "locatorrepo"

func main() {
	api.RunningNodeType = api.NodeLocator
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
				// Generate report in TITAN_LOCATOR_PATH and re-raise panic
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
			Required: true,
			Name:     "certificate-path",
			EnvVars:  []string{"TITAN_LOCATOR_CERTIFICATE_PATH", "LOCATOR_CERTIFICATE_PATH"},
			Usage:    "example: --certificate-path=./cert.pem",
			Value:    "",
		},
		&cli.StringFlag{
			Required: true,
			Name:     "private-key-path",
			EnvVars:  []string{"TITAN_LOCATOR_PRIVATE_KEY_PATH", "LOCATOR_PRIVATE_KEY_PATH"},
			Usage:    "example: --private-key-path=./priv.key",
			Value:    "",
		},
		&cli.StringFlag{
			Name:  "ca-certificate-path",
			Usage: "example: --ca-certificate-path=./ca.pem",
			Value: "",
		},
	},

	Before: func(cctx *cli.Context) error {
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

		cfg, err := lr.Config()
		if err != nil {
			return err
		}

		locatorCfg := cfg.(*config.LocatorCfg)

		err = region.NewRegion(locatorCfg.GeodbPath, region.TypeGeoLite(), "")
		if err != nil {
			log.Panic(err.Error())
		}

		address := locatorCfg.ListenAddress
		addrSplit := strings.Split(address, ":")
		if len(addrSplit) < 2 {
			return fmt.Errorf("Listen address %s is error", address)
		}

		port, err := strconv.Atoi(addrSplit[1])
		if err != nil {
			return err
		}

		ctx := lcli.ReqContext(cctx)
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		handler := LocatorHandler(locator.NewLocalLocator(ctx, lr, locatorCfg.DBAddrss, locatorCfg.UUID, port), true)
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

		httpClient := cliutil.NewHttp3Client(udpPacketConn, locatorCfg.InsecureSkipVerify, locatorCfg.CaCertificatePath)
		jsonrpc.SetHttp3Client(httpClient)

		go startUDPServer(udpPacketConn, handler, locatorCfg)

		go func() {
			<-ctx.Done()
			log.Warn("Shutting down...")
			if err := srv.Shutdown(context.TODO()); err != nil {
				log.Errorf("shutting down RPC server failed: %s", err)
			}

			// udpPacketConn.Close()
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

func startUDPServer(conn net.PacketConn, handler http.Handler, locatorCfg *config.LocatorCfg) error {
	var tlsConfig *tls.Config
	if locatorCfg.InsecureSkipVerify {
		config, err := generateTLSConfig()
		if err != nil {
			log.Errorf("startUDPServer, generateTLSConfig error:%s", err.Error())
			return err
		}
		tlsConfig = config
	} else {
		cert, err := tls.LoadX509KeyPair(locatorCfg.CaCertificatePath, locatorCfg.PrivateKeyPath)
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
