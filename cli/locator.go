package cli

import (
	"fmt"

	"github.com/urfave/cli/v2"
)

var LocationCmds = []*cli.Command{
	accesspointCmd,
}

var accesspointCmd = &cli.Command{
	Name:  "accesspoint",
	Usage: "access point",
	Subcommands: []*cli.Command{
		addCmd,
		removeCmd,
		listCmd,
		infoCmd,
		getCmd,
	},
}

var addCmd = &cli.Command{
	Name:  "add",
	Usage: "add access point",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "area-id",
			Usage: "area id",
			Value: "CN-GD-Shenzhen",
		},
		&cli.StringFlag{
			Name:  "url",
			Usage: "scheduler url",
			Value: "http://1277.0.0.1:3456",
		},
		&cli.Float64Flag{
			Name:  "weight",
			Usage: "range 0~1000",
			Value: 100,
		},
		&cli.StringFlag{
			Name:  "token",
			Usage: "scheduler access token",
			Value: "",
		},
	},

	Action: func(cctx *cli.Context) error {
		api, closer, err := GetLocatorAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		areaID := cctx.String("area-id")
		url := cctx.String("url")
		weight := cctx.Int("weight")
		token := cctx.String("token")
		ctx := ReqContext(cctx)
		// TODO: print more useful things

		err = api.AddAccessPoints(ctx, areaID, url, weight, token)
		if err != nil {
			return err
		}

		return nil
	},
}

var removeCmd = &cli.Command{
	Name:  "remove",
	Usage: "remove access point",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "area-id",
			Usage: "area id",
			Value: "CN-GD-Shenzhen",
		},
	},

	Action: func(cctx *cli.Context) error {
		api, closer, err := GetLocatorAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		areaID := cctx.String("area-id")
		ctx := ReqContext(cctx)

		err = api.RemoveAccessPoints(ctx, areaID)
		if err != nil {
			return err
		}

		return nil
	},
}

var listCmd = &cli.Command{
	Name:  "list",
	Usage: "list access point",
	Flags: []cli.Flag{},

	Action: func(cctx *cli.Context) error {
		api, closer, err := GetLocatorAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := ReqContext(cctx)

		areaIDs, err := api.ListAccessPoints(ctx)
		if err != nil {
			return err
		}

		fmt.Println("AreaID:")
		for _, areaID := range areaIDs {
			fmt.Println(areaID)
		}

		return nil
	},
}

var infoCmd = &cli.Command{
	Name:  "info",
	Usage: "get access point info",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "area-id",
			Usage: "area id",
			Value: "CN-GD-Shenzhen",
		},
	},

	Action: func(cctx *cli.Context) error {
		api, closer, err := GetLocatorAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		areaID := cctx.String("area-id")
		ctx := ReqContext(cctx)

		accesspoint, err := api.ShowAccessPoint(ctx, areaID)
		if err != nil {
			return err
		}

		fmt.Printf("AreaID:%s\n", accesspoint.AreaID)
		for _, info := range accesspoint.SchedulerInfos {
			fmt.Printf("URL:%s   Weight:%d\n", info.URL, info.Weight)
		}

		return nil
	},
}

var getCmd = &cli.Command{
	Name:  "get",
	Usage: "get scheduler url",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "device-id",
			Usage: "device id",
			Value: "2521c39087cecd74a853850dd56e9c859b786fbc",
		},
	},

	Action: func(cctx *cli.Context) error {
		api, closer, err := GetLocatorAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		deviceID := cctx.String("device-id")
		// securityKey := cctx.String("security-key")
		ctx := ReqContext(cctx)

		urls, err := api.GetAccessPoints(ctx, deviceID)
		if err != nil {
			return err
		}

		for _, url := range urls {
			fmt.Println(url)
		}

		return nil
	},
}
