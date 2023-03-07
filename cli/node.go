package cli

import (
	"fmt"
	"os"

	"github.com/filecoin-project/go-jsonrpc/auth"
	"github.com/linguohua/titan/api"
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

		nodes, err := schedulerAPI.GetOnlineNodeIDs(ctx, api.NodeType(t))

		fmt.Println("Online nodes count:", len(nodes))
		for _, node := range nodes {
			fmt.Println(node)
		}

		return err
	},
}

var nodeTokenCmd = &cli.Command{
	Name:  "token",
	Usage: "Get node token with secret ",
	Flags: []cli.Flag{
		nodeIDFlag,
		secretFlag,
	},
	Action: func(cctx *cli.Context) error {
		secret := cctx.String("secret")
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

		perms := []auth.Permission{api.PermRead, api.PermWrite}
		info, err := schedulerAPI.AuthNodeNew(ctx, perms, nodeID, secret)
		if err != nil {
			return err
		}

		fmt.Println(string(info))
		return nil
	},
}

var registerNodeCmd = &cli.Command{
	Name:  "register",
	Usage: "Register nodeID and secret ",
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

		if t != int(api.NodeEdge) && t != int(api.NodeCandidate) && t != int(api.NodeScheduler) {
			return xerrors.Errorf("node-type err:%d", t)
		}

		infos, err := schedulerAPI.AllocateNodes(ctx, api.NodeType(t), 1)
		if err != nil {
			return err
		}

		fmt.Printf("\nNodeID:%s\nSecret:%s", infos[0].NodeID, infos[0].Secret)
		return nil
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

		info, err := schedulerAPI.GetNodeInfo(ctx, nodeID)
		if err != nil {
			return err
		}

		natType, err := schedulerAPI.GetNatType(ctx, nodeID)
		if err != nil {
			natType = "UnkonwNAT"
		}

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
		fmt.Printf("node NatType: %s \n", natType)

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

		info, err := schedulerAPI.ShowNodeLogFile(ctx, nodeID)
		if err != nil {
			return err
		}

		if info == nil {
			fmt.Printf("Log file not exist")
			return nil
		}

		data, err := schedulerAPI.DownloadNodeLogFile(ctx, nodeID)
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

		file, err := schedulerAPI.ShowNodeLogFile(ctx, nodeID)
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

		addr, err := schedulerAPI.GetEdgeExternalAddr(ctx, nodeID, schedulerURL)
		if err != nil {
			return err
		}

		fmt.Printf("edge external addr:%s\n", addr)
		return nil
	},
}
