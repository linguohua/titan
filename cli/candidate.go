package cli

import (
	"fmt"
	"time"

	API "github.com/linguohua/titan/api"
	"github.com/urfave/cli/v2"
)

var CandidateCmds = []*cli.Command{
	VerfyDataCmd,
}

var VerfyDataCmd = &cli.Command{
	Name:  "verifydata",
	Usage: "verify data",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "fid",
			Usage: "file id",
			Value: "",
		},
		&cli.StringFlag{
			Name:  "edge-url",
			Usage: "edge url",
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
		url := cctx.String("edge-url")
		fmt.Printf("fid:%s,url:%s", fid, url)
		ctx := ReqContext(cctx)
		// TODO: print more useful things
		req := make([]API.ReqVerify, 0)
		seed := time.Now().UnixNano()
		varify := API.ReqVerify{EdgeURL: url, Seed: seed, MaxRange: 1, Duration: 10}
		req = append(req, varify)

		err = api.VerifyData(ctx, req)
		if err != nil {
			fmt.Println("err", err)
			return err
		}

		// fmt.Println("verify data cid:", cid)
		return nil
	},
}
