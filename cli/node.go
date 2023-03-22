package cli

import (
	"crypto"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/linguohua/titan/api/types"
	titanrsa "github.com/linguohua/titan/node/rsa"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

var nodeCmd = &cli.Command{
	Name:  "node",
	Usage: "Manage node",
	Subcommands: []*cli.Command{
		showOnlineNodeCmd,
		nodeTokenCmd,
		registerNodeCmd,
		showNodeInfoCmd,
		downloadNodeLogFileCmd,
		showNodeLogInfoCmd,
		deleteNodeLogFileCmd,
		nodeQuitCmd,
		setNodePortCmd,
		edgeExternalAddrCmd,
	},
}

var showOnlineNodeCmd = &cli.Command{
	Name:  "list-online",
	Usage: "List online nodes",
	Flags: []cli.Flag{
		nodeTypeFlag,
	},
	Action: func(cctx *cli.Context) error {
		t := cctx.Int("node-type")

		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		nodes, err := schedulerAPI.OnlineNodeList(ctx, types.NodeType(t))

		fmt.Println("Online nodes count:", len(nodes))
		for _, node := range nodes {
			fmt.Println(node)
		}

		return err
	},
}

var nodeTokenCmd = &cli.Command{
	Name:  "token",
	Usage: "Get node token with secret",
	Flags: []cli.Flag{
		nodeIDFlag,
		&cli.StringFlag{
			Name:  "key-path",
			Usage: "node private key path",
			Value: "",
		},
	},
	Action: func(cctx *cli.Context) error {
		keyPath := cctx.String("key-path")
		nodeID := cctx.String("node-id")
		if nodeID == "" {
			return xerrors.New("node-id is nil")
		}

		pem, err := ioutil.ReadFile(keyPath)
		if err != nil {
			return err
		}

		privateKey, err := titanrsa.Pem2PrivateKey(pem)
		if err != nil {
			return err
		}

		ctx := ReqContext(cctx)

		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		rsa := titanrsa.New(crypto.SHA256, crypto.SHA256.New())
		sign, err := rsa.Sign(privateKey, []byte(nodeID))
		if err != nil {
			return err
		}

		tk, err := schedulerAPI.NodeAuthNew(ctx, nodeID, hex.EncodeToString(sign))
		if err != nil {
			return err
		}

		fmt.Println(tk)
		return nil
	},
}

var registerNodeCmd = &cli.Command{
	Name:  "register",
	Usage: "Register nodeID and secret ",
	Flags: []cli.Flag{
		nodeTypeFlag,
		nodeIDFlag,
		&cli.StringFlag{
			Name:  "public-key-path",
			Usage: "node public key path",
			Value: "",
		},
	},
	Action: func(cctx *cli.Context) error {
		t := cctx.Int("node-type")
		nID := cctx.String("node-id")
		publicKeyPath := cctx.String("public-key-path")

		if t != int(types.NodeEdge) && t != int(types.NodeCandidate) {
			return xerrors.Errorf("node-type err:%d", t)
		}

		if nID == "" {
			return xerrors.New("node-id is nil")
		}

		if publicKeyPath == "" {
			return xerrors.New("public-key-path is nil")
		}

		pem, err := os.ReadFile(publicKeyPath)
		if err != nil {
			return err
		}

		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		return schedulerAPI.RegisterNode(ctx, nID, string(pem), types.NodeType(t))
	},
}

var showNodeInfoCmd = &cli.Command{
	Name:  "info",
	Usage: "Show node info",
	Flags: []cli.Flag{
		nodeIDFlag,
	},
	Action: func(cctx *cli.Context) error {
		nodeID := cctx.String("node-id")
		if nodeID == "" {
			return xerrors.New("node-id is nil")
		}

		ctx := ReqContext(cctx)

		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		info, err := schedulerAPI.NodeInfo(ctx, nodeID)
		if err != nil {
			return err
		}

		natType, _ := schedulerAPI.NodeNatType(ctx, nodeID)

		fmt.Printf("node id: %s \n", info.NodeID)
		fmt.Printf("node name: %s \n", info.NodeName)
		fmt.Printf("node external_ip: %s \n", info.ExternalIP)
		fmt.Printf("node internal_ip: %s \n", info.InternalIP)
		fmt.Printf("node system version: %s \n", info.SystemVersion)
		fmt.Printf("node disk usage: %.2f %s\n", info.DiskUsage, "%")
		fmt.Printf("node disk space: %.2f GB \n", info.DiskSpace/1024/1024/1024)
		fmt.Printf("node fstype: %s \n", info.IoSystem)
		fmt.Printf("node mac: %s \n", info.MacLocation)
		fmt.Printf("node download bandwidth: %.2f GB \n", info.BandwidthDown/1024/1024/1024)
		fmt.Printf("node upload bandwidth: %.2f GB \n", info.BandwidthUp/1024/1024/1024)
		fmt.Printf("node cpu percent: %.2f %s \n", info.CPUUsage, "%")
		//
		fmt.Printf("node DownloadCount: %d \n", info.DownloadBlocks)
		fmt.Printf("node NatType: %s \n", natType.String())

		return nil
	},
}

var downloadNodeLogFileCmd = &cli.Command{
	Name:  "download-log",
	Usage: "Download node log file",
	Flags: []cli.Flag{
		nodeIDFlag,
	},
	Action: func(cctx *cli.Context) error {
		nodeID := cctx.String("node-id")

		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		info, err := schedulerAPI.NodeLogFileInfo(ctx, nodeID)
		if err != nil {
			return err
		}

		if info == nil {
			fmt.Printf("Log file not exist")
			return nil
		}

		data, err := schedulerAPI.NodeLogFile(ctx, nodeID)
		if err != nil {
			return err
		}

		filePath := "./" + info.Name
		return os.WriteFile(filePath, data, 0o644)
	},
}

var showNodeLogInfoCmd = &cli.Command{
	Name:  "log-info",
	Usage: "Show node log info",
	Flags: []cli.Flag{
		nodeIDFlag,
	},
	Action: func(cctx *cli.Context) error {
		nodeID := cctx.String("node-id")

		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		file, err := schedulerAPI.NodeLogFileInfo(ctx, nodeID)
		if err != nil {
			return err
		}

		if file == nil {
			fmt.Printf("Log file not exist")
			return nil
		}

		fmt.Printf("%s %dB", file.Name, file.Size)
		return nil
	},
}

var deleteNodeLogFileCmd = &cli.Command{
	Name:  "delete-log",
	Usage: "Delete log file with node",
	Flags: []cli.Flag{
		nodeIDFlag,
	},
	Action: func(cctx *cli.Context) error {
		nodeID := cctx.String("node-id")

		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		return schedulerAPI.DeleteNodeLogFile(ctx, nodeID)
	},
}

var nodeQuitCmd = &cli.Command{
	Name:  "quit",
	Usage: "Node quit the titan",
	Flags: []cli.Flag{
		nodeIDFlag,
		secretFlag,
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		nodeID := cctx.String("node-id")
		if nodeID == "" {
			return xerrors.New("node-id is nil")
		}

		secret := cctx.String("secret")

		ctx := ReqContext(cctx)

		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		err = schedulerAPI.NodeQuit(ctx, nodeID, secret)
		if err != nil {
			return err
		}

		return nil
	},
}

var edgeExternalAddrCmd = &cli.Command{
	Name:  "external-addr",
	Usage: "get edge external addr",
	Flags: []cli.Flag{
		nodeIDFlag,
		&cli.StringFlag{
			Name:  "scheduler-url",
			Usage: "scheduler url",
			Value: "http://localhost:3456/rpc/v0",
		},
	},
	Action: func(cctx *cli.Context) error {
		nodeID := cctx.String("node-id")
		schedulerURL := cctx.String("scheduler-url")

		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		addr, err := schedulerAPI.EdgeExternalServiceAddress(ctx, nodeID, schedulerURL)
		if err != nil {
			return err
		}

		fmt.Printf("edge external addr:%s\n", addr)
		return nil
	},
}
