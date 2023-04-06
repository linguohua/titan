package cli

import (
	"fmt"
	"os"

	"github.com/docker/go-units"
	"github.com/linguohua/titan/api/types"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

var nodeCmd = &cli.Command{
	Name:  "node",
	Usage: "Manage node",
	Subcommands: []*cli.Command{
		showOnlineNodeCmd,
		registerNodeCmd,
		showNodeInfoCmd,
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

		nodes, err := schedulerAPI.GetOnlineNodeList(ctx, types.NodeType(t))

		fmt.Println("Online nodes count:", len(nodes))
		for _, node := range nodes {
			fmt.Println(node)
		}

		return err
	},
}

var registerNodeCmd = &cli.Command{
	Name:  "register",
	Usage: "Register nodeID and public key ",
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

		return schedulerAPI.RegisterNewNode(ctx, nID, string(pem), types.NodeType(t))
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

		info, err := schedulerAPI.RetrieveNodeInfo(ctx, nodeID)
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
		fmt.Printf("node disk space: %s \n", units.BytesSize(info.DiskSpace))
		fmt.Printf("node fsType: %s \n", info.IoSystem)
		fmt.Printf("node mac: %s \n", info.MacLocation)
		fmt.Printf("node download bandwidth: %s \n", units.BytesSize(info.BandwidthDown))
		fmt.Printf("node upload bandwidth: %s \n", units.BytesSize(info.BandwidthUp))
		fmt.Printf("node cpu percent: %.2f %s \n", info.CPUUsage, "%")
		//
		fmt.Printf("node DownloadCount: %d \n", info.DownloadBlocks)
		fmt.Printf("node NatType: %s \n", natType.String())

		return nil
	},
}

var nodeQuitCmd = &cli.Command{
	Name:  "quit",
	Usage: "Node quit the titan",
	Flags: []cli.Flag{
		nodeIDFlag,
	},

	Before: func(cctx *cli.Context) error {
		return nil
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

		err = schedulerAPI.NodeQuit(ctx, nodeID)
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
