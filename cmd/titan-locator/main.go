package main

import (
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

	"github.com/filecoin-project/go-jsonrpc"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/types"
	"github.com/linguohua/titan/build"
	lcli "github.com/linguohua/titan/cli"
	cliutil "github.com/linguohua/titan/cli/util"
	"github.com/linguohua/titan/lib/titanlog"
	"github.com/linguohua/titan/node"
	"github.com/linguohua/titan/node/config"
	"github.com/linguohua/titan/node/modules/dtypes"
	"github.com/linguohua/titan/node/repo"
	"github.com/quic-go/quic-go/http3"

	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

var log = logging.Logger("main")

const FlagLocatorRepo = "locator-repo"

// TODO remove after deprecation period
const FlagLocatorRepoDeprecation = "locatorrepo"

func main() {
	types.RunningNodeType = types.NodeLocator
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
	Usage: "Start titan locator node",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "geodb-path",
			Usage: "geodb path, example: --geodb-path=../../geoip/geolite2_city/city.mmdb",
			Value: "../../geoip/geolite2_city/city.mmdb",
		},
		&cli.StringFlag{
			Name:  "accesspoint-db",
			Usage: "mysql db, example: --accesspoint-db=user01:sql001@tcp(127.0.0.1:3306)/test",
			Value: "user01:sql001@tcp(127.0.0.1:3306)/test",
		},
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		log.Info("Starting titan locator node")

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

		cfg, err := lr.Config()
		if err != nil {
			return err
		}

		locatorCfg := cfg.(*config.LocatorCfg)

		err = lr.Close()
		if err != nil {
			return err
		}

		shutdownChan := make(chan struct{})

		var locatorAPI api.Locator
		stop, err := node.New(cctx.Context,
			node.Locator(&locatorAPI),
			node.Base(),
			node.Repo(r),
			node.ApplyIf(func(s *node.Settings) bool { return cctx.IsSet("accesspoint-db") },
				node.Override(new(dtypes.DatabaseAddress), func() dtypes.DatabaseAddress {
					return dtypes.DatabaseAddress(cctx.String("accesspoint-db"))
				})),
			node.ApplyIf(func(s *node.Settings) bool { return cctx.IsSet("geodb-path") },
				node.Override(new(dtypes.GeoDBPath), func() dtypes.GeoDBPath {
					return dtypes.GeoDBPath(cctx.String("geodb-path"))
				})),
		)
		if err != nil {
			return xerrors.Errorf("creating node: %w", err)
		}

		// Instantiate the locator node handler.
		handler, err := node.LocatorHandler(locatorAPI, true)
		if err != nil {
			return xerrors.Errorf("failed to instantiate rpc handler: %w", err)
		}

		udpPacketConn, err := net.ListenPacket("udp", locatorCfg.ListenAddress)
		if err != nil {
			return err
		}
		defer udpPacketConn.Close()

		go startUDPServer(udpPacketConn, handler, locatorCfg) // nolint:errcheck

		httpClient, err := cliutil.NewHTTP3Client(udpPacketConn, locatorCfg.InsecureSkipVerify, locatorCfg.CaCertificatePath)
		if err != nil {
			return xerrors.Errorf("new http3 client error %w", err)
		}
		jsonrpc.SetHttp3Client(httpClient)

		// Serve the RPC.
		rpcStopper, err := node.ServeRPC(handler, "titan-locator", locatorCfg.ListenAddress)
		if err != nil {
			return fmt.Errorf("failed to start json-rpc endpoint: %s", err)
		}

		log.Infof("Titan locator server listen on %s", locatorCfg.ListenAddress)

		// Monitor for shutdown.
		finishCh := node.MonitorShutdown(shutdownChan,
			node.ShutdownHandler{Component: "rpc server", StopFunc: rpcStopper},
			node.ShutdownHandler{Component: "locator", StopFunc: stop},
		)

		<-finishCh
		return nil
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
