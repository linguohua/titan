package cli

import (
	"fmt"

	"github.com/urfave/cli/v2"
)

var EdgeCmds = []*cli.Command{
	DeviceIDCmd,
}

var DeviceIDCmd = &cli.Command{
	Name:  "deviceid",
	Usage: "Print deviceid",
	Action: func(cctx *cli.Context) error {
		api, closer, err := GetEdgeAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := ReqContext(cctx)
		// TODO: print more useful things

		v, err := api.DeviceID(ctx)
		if err != nil {
			return err
		}
		fmt.Println("device id: ", v)
		return nil
	},
}
