package cli

import (
	"context"
	"fmt"
	"time"

	API "github.com/linguohua/titan/api"
	"github.com/urfave/cli/v2"
)

var EdgeCmds = []*cli.Command{
	DeviceInfoCmd,
	CacheDataCmd,
	VerfyDataCmd,
	DoVerifyCmd,
	DeleteBlockCmd,
	VerfyDataCmd,
	LimitRateCmd,
	UnlimitRateCmd,
}

var DeviceInfoCmd = &cli.Command{
	Name:  "deviceinfo",
	Usage: "Print device info",
	Action: func(cctx *cli.Context) error {
		api, closer, err := GetEdgeAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := ReqContext(cctx)
		// TODO: print more useful things

		v, err := api.DeviceInfo(ctx)
		if err != nil {
			return err
		}
		fmt.Printf("device id: %v \n", v.DeviceId)
		fmt.Printf("device name: %v \n", v.DeviceName)
		fmt.Printf("device external_ip: %v \n", v.ExternalIp)
		fmt.Printf("device internal_ip: %v \n", v.InternalIp)
		fmt.Printf("device systemVersion: %v \n", v.SystemVersion)
		fmt.Printf("device DiskUsage: %v \n", v.DiskUsage)
		fmt.Printf("device mac: %v \n", v.MacLocation)

		return nil
	},
}

var CacheDataCmd = &cli.Command{
	Name:  "cachedata",
	Usage: "cache block content",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "cid",
			Usage: "block cids",
			Value: "",
		},
		&cli.StringFlag{
			Name:  "fid",
			Usage: "block file id",
			Value: "",
		},
	},
	Action: func(cctx *cli.Context) error {
		fmt.Println("start cache data...")
		api, closer, err := GetEdgeAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		cid := cctx.String("cid")
		// fid := cctx.String("fid")
		fmt.Println("cid:", cid)
		ctx := ReqContext(cctx)
		// TODO: print more useful things

		ids := []string{
			// "QmeUqw4FY1wqnh2FMvuc2v8KAapE7fYwu2Up4qNwhZiRk7",
			// "QmbkdBycNb9xv69Q68ZWqFNE6ZRe5KFMtoHtA9D3MGZPhH",
			// "QmPDuq1nhSCYwJAaPYgby5hThSGdN1QkosPoFVKUDrP4Ro",
			// "bafkreic6vrp57vbzhzmxzetlqu5kqzb4nfcb7nrhnycn6djngp6ka4eziq",
			// "QmaP1ifdEt9K14sVVw7Vf2kcnuicxQmwQWDt5j7ShYXjsN",
			// "QmR7hNanvqaW9iG6jdszzoDKdDsfru5jPpu5VfQT2723kS",
			// "QmewmdJLBotQstj3tff4hAm9NjF1Jb9dbHDJo93PWqJ4Ka",
			// // "QmetZk77NyVmS1fDrhMNHBFaSZwtb7KXVPVqSDgXaZivpD",
			// "QmVqpiJqXTXEseJDbmsaCduFdFpnyzoSovKaHEzZmNoYD6",
			// // "QmWVCvoVHRMevzyDK4TuYr1ZcMzZa22kEq5RaxjnyPQb1y",

			"bafkreic2dalh6zxspsc5cgbtex3inrwjkk4cvuatd4e4jvtmzieeg6vmom",
			"bafkreicdh2dwooyqhm6oc7xil4vrrdawfevdwgvss3djqb5egvgla37x3a",
			"bafkreicnyd7hi3crw7luibjfxhqy65ewvjstwba4atrmfd7zakvg4qwjwy",
			"bafkreidrp46eb6onsmi6qqcywy3woig6fd5ogw77binktgujsbxrxctaw4",
			// "bafkreihvey6kt24rpxvub2mkzutgem4e3cgyi2xrdmzhdc77d477ifmuqm",
			// "bafkreigmp7zgjnkzm2aq6msb74fghpe75m4e2wgy62zt253y33ethjljje",
			// "bafkreih6ouktjwl5eiiojb2bwqmdbx4t2tx2lwtlk3fvvqwnfb532p5ywy",
			// "bafkreif4vamvmvss77oanijx34ytszxcyzhsscuvwj6xacszmrjyvtn54m",
			// "bafkreigq56ud4ohtmhijtusfsvdmwk56qs36vbkdb2tqfep25k7bsr5esq",
			// "bafkreibelhulvqgm5kuxvh7kcvbfp544xke3otuna2rfyt3ulcuyg73akm",
			// "bafkreigdg7pkvz5sob6crhaonrz7avi7qouem57sk3iaewqmiu2n5zuihy",
			// "bafkreibubp5fer4mgxyjvgr7t3u5pqwnjjcpqq3twm5idwr7yavsaqhzee",
			// "bafkreic6mtqflfaajzzbiknfr3ilnnsfugzztf2p3cqlpsa3s6xnfbdl34",
			// "bafkreig6avcaplojy4hzumpw2idin5lu57bycze7byvla73mlyqdx4zwnu",
			// "bafkreicazep6co2tblgfeo7xg6ob76gr56swe4jpuljnerdlzla2sgwrdu",
			// "bafkreigpwtgxhl7xflntbq7zpagbkqkdwgta6ltgfjwwnqbpxskxwx5fue",
			// "bafkreih5ccfsty3rzg6jgwlrp2vlgqpv2baezdbswzdpsjvpg2yqmjcq5m",
			// "bafkreicf5mezn5cy6ib2zocxwnqqoqvketwzsqr6k4pvgpeuq6gtjmuece",
			// "bafkreih5dktq7sop75myx53gsnabddx2cpd2g4ejgkdvifsxod36hx5nge",
			// "bafkreia2d6rymp23wt62yfao4a3arq6tcstwaxc5btbcexinvgrfgkcefq",
			// "bafkreigcsno3j2ravp7ntnig6zy5solyy6kzdt3pl57prlbzh5gslil3cy",
			// "bafkreicixxifhlk4dp4tvzs5w6bhvdo7e3qccuvbkdgohw56blmohy7sn4",
			// "bafkreiehzpfbeunyyfarn6h3goblullajuemfgthxbqmvjo72bqh4ds6oy",
			// "bafkreifyvp3t6ep6ftqjkq3h65zdmgr2breeqp4k7rmdmsibel4755vh4e",
			// "bafkreibpxgd3vlearz3rzm75zlw2wjq5bsv4suxtrj5mrxdcqq2itrqlii",
			// "bafkreidang5esgsroqj5dsrnj5flbxuciqki55zxir7waskre45oeefufa",
			// "bafkreibwg7p7fikhvdtdtj5u2hyjaojkz6aceg4vx66yrc2rcwqhpxldoy",
			// "bafkreibthn4ngz5yq3im6dtdlmeg3kwqldnqwp7bnljqiqufaduxqbp7ma",
			// "bafkreicvu5ym5fsffx2y2czmfh4lozt7bt6pkn3xpsezii2rvsztqgar2e",
			// "bafkreia5lywtofhazb2glmwem62jqrz67yetcq6oatucrywx4gltf7xhu4",
			// "bafkreic43vhpdroul77ajihs4jdpdzfdb6qepm2lq23ywq54inthal7pym",
			// "bafkreialkb4dcculkn2x3zr2snxxyh6laj7y43mrgh53s3kgrgvcqkwb3y",
			// "bafkreidhnpecmu7yqbdmzi463hnuauq7lqidwnex5k5hs3dhsysd5u7s4u",
			// "bafkreievimive25a2bgzerjwxchd6kme2cbm7hdhfnaxzgftqxo2bfeih4",
			// "bafkreig26o4roqcowqc25yg3lzfbqcd4i2ezenfbwjsi5m6s6xnycakrue",
			// "bafkreiewvhgytf4piiwqbyxv33byfaazsilps65avspfwb2s3bstwsdyza",
			// "bafkreibfwuh2qwo5kfvddisonvbbd72mlxa4t6vlecesqfzqgirtjdr5wq",
			// "bafkreibnq6h57f5pg7tly775t3ibpork2ftqv6s373v4vbxs2cwvtejj3e",
			// "bafkreihdnarq4lfzlt2p2ypnk5als7n65xiexoutn5w7m3aixwff74c4vu",
			// "bafkreiey6d3qspy2renhcumllqh5nrvclmepqwu5rpherjpum2lria26fm",
			// "bafkreif6ug4sngfl4s4izcwdqoccgdhzkjhmpjp3gmiwcp6ho5xwljjqba",
			// "bafkreiau64ltlfm6thzeitbfvda57ugiap5vogce6laphuutjewrlh2gvm",
			// "bafkreibzephikut47bv3a4yik3zfach75pxmteprkfpofixnkdvmispslq",
		}

		// req := make([]API.ReqCacheData, 0)
		// for i, id := range ids {
		// 	reqData := API.ReqCacheData{Cids: string{id}, ID: fmt.Sprintf("%d", i)}
		// 	req = append(req, reqData)
		// }
		reqData := API.ReqCacheData{Cids: ids, CandidateURL: ""}

		err = api.CacheData(ctx, reqData)
		if err != nil {
			return err
		}

		fmt.Println("cache data success")
		return nil
	},
}

var DoVerifyCmd = &cli.Command{
	Name:  "doverify",
	Usage: "do verify edge",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "fid",
			Usage: "block file id",
			Value: "",
		},
		&cli.StringFlag{
			Name:  "url",
			Usage: "candiate node url",
			Value: "",
		},
	},
	Action: func(cctx *cli.Context) error {
		api, closer, err := GetEdgeAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		fid := cctx.String("fid")
		url := cctx.String("url")
		log.Infof("fid:%s, url:%s", fid, url)

		seed := time.Now().UnixNano()
		req := API.ReqVerify{EdgeURL: "", Seed: seed, FIDs: []string{"0"}, Duration: 10}
		err = api.DoVerify(context.Background(), req, url)

		log.Infof("DoVerify success %v", err)
		return nil
	},
}

var DeleteBlockCmd = &cli.Command{
	Name:  "delete",
	Usage: "delete blocks",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "cid",
			Usage: "block cid",
			Value: "",
		},
	},
	Action: func(cctx *cli.Context) error {
		api, closer, err := GetEdgeAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		cid := cctx.String("cid")

		result, err := api.DeleteBlocks(context.Background(), []string{cid})
		if err != nil {
			return err
		}

		if len(result.List) > 0 {
			log.Infof("delete block %s failed %v", cid, result.List[0].ErrMsg)
			return nil
		}

		log.Infof("delete block %s success", cid)
		return nil
	},
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
		varify := API.ReqVerify{EdgeURL: url, Seed: seed, FIDs: []string{"0"}, Duration: 10}
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

var LimitRateCmd = &cli.Command{
	Name:  "limit",
	Usage: "limit rate",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "rate",
			Usage: "speed rate",
			Value: "",
		},
	},
	Action: func(cctx *cli.Context) error {
		api, closer, err := GetEdgeAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		speed := cctx.Int64("rate")

		ctx := ReqContext(cctx)

		err = api.SetDownloadSpeed(ctx, speed)
		if err != nil {
			fmt.Printf("Set Download speed failed:%v", err)
			return err
		}
		fmt.Printf("Set download speed %d success", speed)
		return nil
	},
}

var UnlimitRateCmd = &cli.Command{
	Name:  "unlimit",
	Usage: "unlimit rate",
	Flags: []cli.Flag{},
	Action: func(cctx *cli.Context) error {
		api, closer, err := GetEdgeAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := ReqContext(cctx)
		err = api.UnlimitDownloadSpeed(ctx)
		if err != nil {
			fmt.Printf("Unlimit speed failed:%v", err)
			return err
		}
		fmt.Printf("Unlimit speed success")
		return nil
	},
}
