package cli

import (
	"fmt"
	"syscall"

	"github.com/urfave/cli/v2"
)

var ValidatorCmds = []*cli.Command{
	VerfyDataCmd,
}

var VerfyDataCmd = &cli.Command{
	Name:  "verfydata",
	Usage: "verfy data",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "fid",
			Usage: "file id",
			Value: "",
		},
	},
	Action: func(cctx *cli.Context) error {
		api, closer, err := GetValidatorAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		fid := cctx.String("fid")
		fmt.Println("fid:", fid)
		ctx := ReqContext(cctx)
		// TODO: print more useful things

		cid, err := api.VerifyData(ctx, fid, "http://localhost:1234/rpc/v0")
		if err == syscall.ERROR_FILE_NOT_FOUND {
			fmt.Println("file not found")
			return err
		}

		if err != nil {
			fmt.Println("err", err)
			return err
		}

		fmt.Println("verify data cid:", cid)
		return nil
	},
}
