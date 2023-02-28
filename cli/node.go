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

		nodes, err := schedulerAPI.GetOnlineDeviceIDs(ctx, api.NodeType(t))

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
		deviceIDFlag,
		secretFlag,
	},
	Action: func(cctx *cli.Context) error {
		secret := cctx.String("secret")
		deviceID := cctx.String("device-id")
		if deviceID == "" {
			return xerrors.New("device-id is nil")
		}

		ctx := ReqContext(cctx)

		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		perms := []auth.Permission{api.PermRead, api.PermWrite}
		info, err := schedulerAPI.AuthNodeNew(ctx, perms, deviceID, secret)
		if err != nil {
			return err
		}

		fmt.Println(string(info))
		return nil
	},
}

var registerNodeCmd = &cli.Command{
	Name:  "register",
	Usage: "Register deviceID and secret ",
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

		fmt.Printf("\nDeviceID:%s\nSecret:%s", infos[0].DeviceID, infos[0].Secret)
		return nil
	},
}

var showNodeInfoCmd = &cli.Command{
	Name:  "info",
	Usage: "Show node info",
	Flags: []cli.Flag{
		deviceIDFlag,
	},
	Action: func(cctx *cli.Context) error {
		deviceID := cctx.String("device-id")
		if deviceID == "" {
			return xerrors.New("device-id is nil")
		}

		ctx := ReqContext(cctx)

		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		info, err := schedulerAPI.GetNodeInfoByID(ctx, deviceID)
		if err != nil {
			return err
		}

		natType, err := schedulerAPI.GetNatType(ctx, deviceID)
		if err != nil {
			natType = "UnkonwNAT"
		}

		fmt.Printf("device id: %s \n", info.DeviceId)
		fmt.Printf("device name: %s \n", info.DeviceName)
		fmt.Printf("device external_ip: %s \n", info.ExternalIp)
		fmt.Printf("device internal_ip: %s \n", info.InternalIp)
		fmt.Printf("device system version: %s \n", info.SystemVersion)
		fmt.Printf("device disk usage: %.2f %s\n", info.DiskUsage, "%")
		fmt.Printf("device disk space: %.2f GB \n", info.DiskSpace/1024/1024/1024)
		fmt.Printf("device fstype: %s \n", info.IoSystem)
		fmt.Printf("device mac: %s \n", info.MacLocation)
		fmt.Printf("device download bandwidth: %.2f GB \n", info.BandwidthDown/1024/1024/1024)
		fmt.Printf("device upload bandwidth: %.2f GB \n", info.BandwidthUp/1024/1024/1024)
		fmt.Printf("device cpu percent: %.2f %s \n", info.CpuUsage, "%")
		//
		fmt.Printf("device DownloadCount: %d \n", info.DownloadCount)
		fmt.Printf("device NatType: %s \n", natType)

		return nil
	},
}

var downloadNodeLogFileCmd = &cli.Command{
	Name:  "download-log",
	Usage: "Download node log file",
	Flags: []cli.Flag{
		deviceIDFlag,
	},
	Action: func(cctx *cli.Context) error {
		deviceID := cctx.String("device-id")

		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		info, err := schedulerAPI.ShowNodeLogFile(ctx, deviceID)
		if err != nil {
			return err
		}

		if info == nil {
			fmt.Printf("Log file not exist")
			return nil
		}

		data, err := schedulerAPI.DownloadNodeLogFile(ctx, deviceID)
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
		deviceIDFlag,
	},
	Action: func(cctx *cli.Context) error {
		deviceID := cctx.String("device-id")

		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		file, err := schedulerAPI.ShowNodeLogFile(ctx, deviceID)
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
		deviceIDFlag,
	},
	Action: func(cctx *cli.Context) error {
		deviceID := cctx.String("device-id")

		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		return schedulerAPI.DeleteNodeLogFile(ctx, deviceID)
	},
}

var nodeQuitCmd = &cli.Command{
	Name:  "quit",
	Usage: "Node quit the titan",
	Flags: []cli.Flag{
		deviceIDFlag,
		secretFlag,
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		deviceID := cctx.String("device-id")
		if deviceID == "" {
			return xerrors.New("device-id is nil")
		}

		secret := cctx.String("secret")

		ctx := ReqContext(cctx)

		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		err = schedulerAPI.NodeQuit(ctx, deviceID, secret)
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
		deviceIDFlag,
		&cli.StringFlag{
			Name:  "scheduler-url",
			Usage: "scheduler url",
			Value: "http://localhost:3456/rpc/v0",
		},
	},
	Action: func(cctx *cli.Context) error {
		deviceID := cctx.String("device-id")
		schedulerURL := cctx.String("scheduler-url")

		ctx := ReqContext(cctx)
		schedulerAPI, closer, err := GetSchedulerAPI(cctx, "")
		if err != nil {
			return err
		}
		defer closer()

		addr, err := schedulerAPI.GetEdgeExternalAddr(ctx, deviceID, schedulerURL)
		if err != nil {
			return err
		}

		fmt.Printf("edge external addr:%s\n", addr)
		return nil
	},
}
