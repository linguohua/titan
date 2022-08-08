package cli

import (
	"fmt"

	API "github.com/linguohua/titan/api"
	"github.com/urfave/cli/v2"
)

var CandidateCmds = []*cli.Command{
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
		api, closer, err := GetCandidateAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		fid := cctx.String("fid")
		fmt.Println("fid:", fid)
		ctx := ReqContext(cctx)
		// TODO: print more useful things
		req := make([]API.ReqVarify, 0)
		varify := API.ReqVarify{}
		req = append(req, varify)

		cid, err := api.VerifyData(ctx, req)
		if err != nil {
			fmt.Println("err", err)
			return err
		}

		fmt.Println("verify data cid:", cid)
		return nil
	},
}
