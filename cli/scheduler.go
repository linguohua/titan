package cli

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"strconv"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/linguohua/titan/api"
	"github.com/urfave/cli/v2"
)

// SchedulerCmds Scheduler Cmds
var SchedulerCmds = []*cli.Command{
	WithCategory("node", nodeCmd),
	WithCategory("carfile", carfileCmd),
	// validator
	electionCmd,
	validateCmd,
	validateSwitchCmd,
	// other
	nodeAppUpdateCmd,
}

var (
	secretFlag = &cli.StringFlag{
		Name:  "secret",
		Usage: "node secret",
		Value: "",
	}

	nodeIDFlag = &cli.StringFlag{
		Name:  "node-id",
		Usage: "node id",
		Value: "",
	}

	cidsPathFlag = &cli.StringFlag{
		Name:  "cids-file-path",
		Usage: "blocks cid file path",
		Value: "",
	}

	cidsFlag = &cli.StringFlag{
		Name:  "cids",
		Usage: "blocks cid",
		Value: "",
	}

	cidFlag = &cli.StringFlag{
		Name:  "cid",
		Usage: "cid",
		Value: "",
	}

	ipFlag = &cli.StringFlag{
		Name:  "ip",
		Usage: "ip",
		Value: "",
	}

	replicaCountFlag = &cli.IntFlag{
		Name:  "replica-count",
		Usage: "Number of replica cached to nodes (default:2)",
		Value: 2,
	}

	nodeTypeFlag = &cli.IntFlag{
		Name:  "node-type",
		Usage: "node type 0:ALL 1:Edge 2:Candidate 3:Scheduler 4:Scheduler (default: 0)",
		Value: 0,
	}

	countFlag = &cli.IntFlag{
		Name:  "count",
		Usage: "count",
		Value: 0,
	}

	pageFlag = &cli.IntFlag{
		Name:  "page",
		Usage: "page",
		Value: 0,
	}

	expirationDateFlag = &cli.StringFlag{
		Name:  "expiration-date",
		Usage: "date time ('2006-1-2 15:04:05') (default:7 day later)",
		Value: "",
	}

	dateFlag = &cli.StringFlag{
		Name:  "date-time",
		Usage: "date time (2006-1-2 15:04:05)",
		Value: "",
	}

	portFlag = &cli.StringFlag{
		Name:  "port",
		Usage: "port",
		Value: "",
	}

	downloadingFlag = &cli.BoolFlag{
		Name:  "downloading",
		Usage: "storage is downloading",
		Value: false,
	}
)

var setNodePortCmd = &cli.Command{
	Name:  "set-node-port",
	Usage: "set node port",
	Flags: []cli.Flag{
		nodeIDFlag,
		portFlag,
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		nodeID := cctx.String("node-id")
		port := cctx.String("port")

		ctx := ReqContext(cctx)

		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		return schedulerAPI.SetNodePort(ctx, nodeID, port)
	},
}

var validateCmd = &cli.Command{
	Name:  "start-validator",
	Usage: "Validate edge node",

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		ctx := ReqContext(cctx)

		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		return schedulerAPI.ValidateStart(ctx)
	},
}

var electionCmd = &cli.Command{
	Name:  "start-election",
	Usage: "Start election validator",

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		ctx := ReqContext(cctx)

		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}

		defer closer()

		return schedulerAPI.ElectionValidators(ctx)
	},
}

var validateSwitchCmd = &cli.Command{
	Name:  "validator-switch",
	Usage: "validator switch",
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:  "enable",
			Usage: "is enable",
			Value: false,
		},
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		enable := cctx.Bool("enable")

		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		schedulerAPI.ValidateSwitch(ctx, enable)

		return nil
	},
}

func generatePrivateKey(bits int) (*rsa.PrivateKey, error) {
	privateKey, err := rsa.GenerateKey(rand.Reader, bits)
	if err != nil {
		return nil, err
	}

	return privateKey, nil
}

func publicKey2Pem(publicKey *rsa.PublicKey) string {
	if publicKey == nil {
		return ""
	}

	public := x509.MarshalPKCS1PublicKey(publicKey)

	publicKeyBytes := pem.EncodeToMemory(
		&pem.Block{
			Type:  "RSA PUBLIC KEY",
			Bytes: public,
		},
	)

	return string(publicKeyBytes)
}

type yconfig struct {
	Cids []string `toml:"cids"`
}

// loadFile
func loadCidsFromFile(configPath string) ([]string, error) {
	var config yconfig
	if _, err := toml.DecodeFile(configPath, &config); err != nil {
		return nil, err
	}

	c := &config
	return c.Cids, nil
}

var nodeAppUpdateCmd = &cli.Command{
	Name:  "node-update",
	Usage: "get node update info or set node update info",
	Subcommands: []*cli.Command{
		getNodeAppUpdateInfoCmd,
		setNodeAppUpdateInfoCmd,
		DeleteNodeAppUpdateInfoCmd,
	},
}

var DeleteNodeAppUpdateInfoCmd = &cli.Command{
	Name:  "delete",
	Usage: "delete node update info",
	Flags: []cli.Flag{
		&cli.IntFlag{
			Name:  "node-type",
			Usage: "node type: edge 1, update 6",
			Value: 1,
		},
	},
	Action: func(cctx *cli.Context) error {
		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		nodeType := cctx.Int("node-type")
		err = schedulerAPI.DeleteNodeAppUpdateInfos(ctx, nodeType)
		if err != nil {
			return err
		}
		return nil
	},
}

var getNodeAppUpdateInfoCmd = &cli.Command{
	Name:  "get",
	Usage: "get node update info",
	Action: func(cctx *cli.Context) error {
		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		updateInfos, err := schedulerAPI.GetNodeAppUpdateInfos(ctx)
		if err != nil {
			return err
		}

		if len(updateInfos) == 0 {
			fmt.Printf("No update info exist\n")
			return nil
		}

		for k, updateInfo := range updateInfos {
			fmt.Printf("AppName:%s\n", updateInfo.AppName)
			fmt.Printf("NodeType:%d\n", k)
			fmt.Printf("Hash:%s\n", updateInfo.Hash)
			fmt.Printf("Version:%s\n", updateInfo.Version)
			fmt.Printf("DownloadURL:%s\n", updateInfo.DownloadURL)
			fmt.Println()
		}

		return nil
	},
}

var setNodeAppUpdateInfoCmd = &cli.Command{
	Name:  "set",
	Usage: "set node update info",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "version",
			Usage: "latest node app version",
			Value: "0.0.1",
		},
		&cli.StringFlag{
			Name:  "hash",
			Usage: "latest node app hash",
			Value: "",
		},
		&cli.StringFlag{
			Name:  "download-url",
			Usage: "latest node app download url",
			Value: "",
		},
		&cli.StringFlag{
			Name:  "app-name",
			Usage: "app name",
			Value: "",
		},
		&cli.IntFlag{
			Name:  "node-type",
			Usage: "node type: 1 is edge, 6 is update",
			Value: 1,
		},
	},

	Action: func(cctx *cli.Context) error {
		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		versionStr := cctx.String("version")
		hash := cctx.String("hash")
		downloadURL := cctx.String("download-url")
		nodeType := cctx.Int("node-type")
		appName := cctx.String("app-name")

		version, err := newVersion(versionStr)
		if err != nil {
			return err
		}

		updateInfo := &api.NodeAppUpdateInfo{AppName: appName, NodeType: nodeType, Version: version, Hash: hash, DownloadURL: downloadURL}
		err = schedulerAPI.SetNodeAppUpdateInfo(ctx, updateInfo)
		if err != nil {
			return err
		}
		return nil
	},
}

func newVersion(version string) (api.Version, error) {
	stringSplit := strings.Split(version, ".")
	if len(stringSplit) != 3 {
		return api.Version(0), fmt.Errorf("parse version error")
	}

	// major, minor, patch
	major, err := strconv.Atoi(stringSplit[0])
	if err != nil {
		return api.Version(0), fmt.Errorf("parse version major error:%s", err)
	}

	minor, err := strconv.Atoi(stringSplit[1])
	if err != nil {
		return api.Version(0), fmt.Errorf("parse version minor error:%s", err)
	}

	patch, err := strconv.Atoi(stringSplit[2])
	if err != nil {
		return api.Version(0), fmt.Errorf("parse version patch error:%s", err)
	}

	return api.Version(uint32(major)<<16 | uint32(minor)<<8 | uint32(patch)), nil
}
