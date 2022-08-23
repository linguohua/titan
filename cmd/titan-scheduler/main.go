package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/client"
	"github.com/linguohua/titan/build"
	lcli "github.com/linguohua/titan/cli"
	"github.com/linguohua/titan/lib/titanlog"
	"github.com/linguohua/titan/lib/ulimit"
	"github.com/linguohua/titan/metrics"
	"github.com/linguohua/titan/node/repo"
	"github.com/linguohua/titan/node/scheduler"
	"github.com/linguohua/titan/node/scheduler/db"
	"github.com/linguohua/titan/region"
	"golang.org/x/xerrors"

	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
	"go.opencensus.io/tag"
)

var log = logging.Logger("main")

const (
	FlagWorkerRepo = "scheduler-repo"

	// TODO remove after deprecation period
	FlagWorkerRepoDeprecation = "schedulerrepo"
)

func main() {
	api.RunningNodeType = api.NodeScheduler

	titanlog.SetupLogLevels()

	local := []*cli.Command{
		runCmd,
		cacheCmd,
		electionCmd,
		spotCheckCmd,
		cachesCmd,
		initDeviceIDsCmd,
	}

	app := &cli.App{
		Name:                 "titan-scheduler",
		Usage:                "Titan scheduler node",
		Version:              build.UserVersion(),
		EnableBashCompletion: true,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    FlagWorkerRepo,
				Aliases: []string{FlagWorkerRepoDeprecation},
				EnvVars: []string{"TITAN_SCHEDULER_PATH", "SCHEDULER_PATH"},
				Value:   "~/.titanscheduler", // TODO: Consider XDG_DATA_HOME
				Usage:   fmt.Sprintf("Specify worker repo path. flag %s and env TITAN_SCHEDULER_PATH are DEPRECATION, will REMOVE SOON", FlagWorkerRepoDeprecation),
			},
			&cli.StringFlag{
				Name:    "panic-reports",
				EnvVars: []string{"TITAN_PANIC_REPORT_PATH"},
				Hidden:  true,
				Value:   "~/.titanscheduler", // should follow --repo default
			},
		},

		After: func(c *cli.Context) error {
			if r := recover(); r != nil {
				// Generate report in LOTUS_PATH and re-raise panic
				build.GeneratePanicReport(c.String("panic-reports"), c.String(FlagWorkerRepo), c.App.Name)
				panic(r)
			}
			return nil
		},
		Commands: local,
	}
	app.Setup()
	app.Metadata["repoType"] = repo.Worker

	if err := app.Run(os.Args); err != nil {
		log.Warnf("%+v", err)
		return
	}
}

var runCmd = &cli.Command{
	Name:  "run",
	Usage: "Start titan scheduler node",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "listen",
			Usage: "host address and port the worker api will listen on",
			Value: "0.0.0.0:3456",
		},
		&cli.StringFlag{
			Name:  "cachedb-url",
			Usage: "cachedb url",
			Value: "127.0.0.1:6378",
		},
		&cli.StringFlag{
			Name:  "geodb-path",
			Usage: "geodb path",
			Value: "../../geoip/geolite2_city/city.mmdb",
		},
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		log.Info("Starting titan scheduler node")

		limit, _, err := ulimit.GetLimit()
		switch {
		case err == ulimit.ErrUnsupported:
			log.Errorw("checking file descriptor limit failed", "error", err)
		case err != nil:
			return xerrors.Errorf("checking fd limit: %w", err)
		default:
			if limit < build.EdgeFDLimit {
				return xerrors.Errorf("soft file descriptor limit (ulimit -n) too low, want %d, current %d", build.EdgeFDLimit, limit)
			}
		}

		// Connect to scheduler
		ctx := lcli.ReqContext(cctx)

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		// init mysql db
		// db.GMysqlDb = db.GormMysql()
		// TODO
		cURL := cctx.String("cachedb-url")
		db.NewCacheDB(cURL, db.TypeRedis())

		gPath := cctx.String("geodb-path")
		region.NewRegion(gPath, region.TypeGeoLite())

		schedulerAPI := scheduler.NewLocalScheduleNode()

		// log.Info("Setting up control endpoint at " + address)

		srv := &http.Server{
			Handler: schedulerHandler(schedulerAPI, true),
			BaseContext: func(listener net.Listener) context.Context {
				ctx, _ := tag.New(context.Background(), tag.Upsert(metrics.APIInterface, "titan-edge"))
				return ctx
			},
		}

		scheduler.InitKeepaliveTimewheel()
		// scheduler.InitVerifyTimewheel()

		go func() {
			<-ctx.Done()
			log.Warn("Shutting down...")
			if err := srv.Shutdown(context.TODO()); err != nil {
				log.Errorf("shutting down RPC server failed: %s", err)
			}
			log.Warn("Graceful shutdown successful")
		}()

		address := cctx.String("listen")

		nl, err := net.Listen("tcp", address)
		if err != nil {
			return err
		}

		log.Info("titan scheduler listen with:", address)

		return srv.Serve(nl)
	},
}

var spotCheckCmd = &cli.Command{
	Name:  "spotcheck",
	Usage: "spot check edge node",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "api-url",
			Usage: "host address and port the worker api will listen on",
			Value: "127.0.0.1:3456",
		},
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		url := cctx.String("api-url")
		log.Infof("scheduler url:%v", url)

		ctx := lcli.ReqContext(cctx)

		var schedulerAPI api.Scheduler
		var closer func()
		var err error
		for {
			schedulerAPI, closer, err = client.NewScheduler(ctx, url, nil)
			if err == nil {
				_, err = schedulerAPI.Version(ctx)
				if err == nil {
					break
				}
			}
			fmt.Printf("\r\x1b[0KConnecting to miner API... (%s)", err)
			time.Sleep(time.Second)
			continue
		}

		defer closer()

		err = schedulerAPI.SpotCheck(ctx)
		if err != nil {
			log.Infof("SpotCheck err:%v", err)
		}

		return err
	},
}

var electionCmd = &cli.Command{
	Name:  "election",
	Usage: "Start election validator",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "api-url",
			Usage: "host address and port the worker api will listen on",
			Value: "127.0.0.1:3456",
		},
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		url := cctx.String("api-url")
		log.Infof("scheduler url:%v", url)

		ctx := lcli.ReqContext(cctx)

		var schedulerAPI api.Scheduler
		var closer func()
		var err error
		for {
			schedulerAPI, closer, err = client.NewScheduler(ctx, url, nil)
			if err == nil {
				_, err = schedulerAPI.Version(ctx)
				if err == nil {
					break
				}
			}
			fmt.Printf("\r\x1b[0KConnecting to miner API... (%s)", err)
			time.Sleep(time.Second)
			continue
		}

		defer closer()

		err = schedulerAPI.ElectionValidators(ctx)
		if err != nil {
			log.Infof("ElectionValidators err:%v", err)
		}

		return err
	},
}

var cacheCmd = &cli.Command{
	Name:  "cache",
	Usage: "specify node cache data",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "api-url",
			Usage: "host address and port the worker api will listen on",
			Value: "127.0.0.1:3456",
		},
		&cli.StringFlag{
			Name:  "cids",
			Usage: "block cid",
			Value: "",
		},
		&cli.StringFlag{
			Name:  "deviceID",
			Usage: "cache node device id",
			Value: "",
		},
		&cli.StringFlag{
			Name:  "caches-path",
			Usage: "cache cids file path",
			Value: "",
		},
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		// arg0 := cctx.Args().Get(0)

		url := cctx.String("api-url")
		cids := cctx.String("cids")
		deviceID := cctx.String("deviceID")
		cachesPath := cctx.String("caches-path")

		log.Infof("cache cid:%v,url:%v,deviceID:%v,cachesPath:%v", cids, url, deviceID, cachesPath)

		ctx := lcli.ReqContext(cctx)

		var schedulerAPI api.Scheduler
		var closer func()
		var err error
		for {
			schedulerAPI, closer, err = client.NewScheduler(ctx, url, nil)
			if err == nil {
				_, err = schedulerAPI.Version(ctx)
				if err == nil {
					break
				}
			}
			fmt.Printf("\r\x1b[0KConnecting to miner API... (%s)", err)
			time.Sleep(time.Second)
			continue
		}

		defer closer()

		cidList := strings.Split(cids, ",")

		notFindNodeCids, err := schedulerAPI.CacheData(ctx, cidList, deviceID)
		if err != nil {
			log.Errorf("CacheData err:%v", err)
		}
		log.Infof("notFindNodeCids:%v", notFindNodeCids)

		return err
	},
}

var cachesCmd = &cli.Command{
	Name:  "caches",
	Usage: "specify node cache data",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "api-url",
			Usage: "host address and port the worker api will listen on",
			Value: "127.0.0.1:3456",
		},
		&cli.StringFlag{
			Name:  "caches-path",
			Usage: "cache cids file path",
			Value: "",
		},
	},

	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		// arg0 := cctx.Args().Get(0)

		url := cctx.String("api-url")
		cachesPath := cctx.String("caches-path")

		log.Infof("cache url:%v,cachesPath:%v", url, cachesPath)

		ctx := lcli.ReqContext(cctx)

		var schedulerAPI api.Scheduler
		var closer func()
		var err error
		for {
			schedulerAPI, closer, err = client.NewScheduler(ctx, url, nil)
			if err == nil {
				_, err = schedulerAPI.Version(ctx)
				if err == nil {
					break
				}
			}
			fmt.Printf("\r\x1b[0KConnecting to miner API... (%s)", err)
			time.Sleep(time.Second)
			continue
		}

		defer closer()

		nodes, _ := schedulerAPI.GetOnlineDeviceIDs(ctx, api.TypeNameAll)
		log.Infof("GetDeviceIDs nodes:%v", nodes)

		// cids := []string{"bafkreic2dalh6zxspsc5cgbtex3inrwjkk4cvuatd4e4jvtmzieeg6vmom", "bafkreicdh2dwooyqhm6oc7xil4vrrdawfevdwgvss3djqb5egvgla37x3a", "bafkreicnyd7hi3crw7luibjfxhqy65ewvjstwba4atrmfd7zakvg4qwjwy", "bafkreidrp46eb6onsmi6qqcywy3woig6fd5ogw77binktgujsbxrxctaw4", "bafkreihvey6kt24rpxvub2mkzutgem4e3cgyi2xrdmzhdc77d477ifmuqm", "bafkreigmp7zgjnkzm2aq6msb74fghpe75m4e2wgy62zt253y33ethjljje", "bafkreih6ouktjwl5eiiojb2bwqmdbx4t2tx2lwtlk3fvvqwnfb532p5ywy", "bafkreif4vamvmvss77oanijx34ytszxcyzhsscuvwj6xacszmrjyvtn54m", "bafkreigq56ud4ohtmhijtusfsvdmwk56qs36vbkdb2tqfep25k7bsr5esq", "bafkreibelhulvqgm5kuxvh7kcvbfp544xke3otuna2rfyt3ulcuyg73akm", "bafkreigdg7pkvz5sob6crhaonrz7avi7qouem57sk3iaewqmiu2n5zuihy", "bafkreibubp5fer4mgxyjvgr7t3u5pqwnjjcpqq3twm5idwr7yavsaqhzee", "bafkreic6mtqflfaajzzbiknfr3ilnnsfugzztf2p3cqlpsa3s6xnfbdl34", "bafkreig6avcaplojy4hzumpw2idin5lu57bycze7byvla73mlyqdx4zwnu", "bafkreicazep6co2tblgfeo7xg6ob76gr56swe4jpuljnerdlzla2sgwrdu", "bafkreigpwtgxhl7xflntbq7zpagbkqkdwgta6ltgfjwwnqbpxskxwx5fue", "bafkreih5ccfsty3rzg6jgwlrp2vlgqpv2baezdbswzdpsjvpg2yqmjcq5m", "bafkreicf5mezn5cy6ib2zocxwnqqoqvketwzsqr6k4pvgpeuq6gtjmuece", "bafkreih5dktq7sop75myx53gsnabddx2cpd2g4ejgkdvifsxod36hx5nge", "bafkreia2d6rymp23wt62yfao4a3arq6tcstwaxc5btbcexinvgrfgkcefq", "bafkreigcsno3j2ravp7ntnig6zy5solyy6kzdt3pl57prlbzh5gslil3cy", "bafkreicixxifhlk4dp4tvzs5w6bhvdo7e3qccuvbkdgohw56blmohy7sn4", "bafkreiehzpfbeunyyfarn6h3goblullajuemfgthxbqmvjo72bqh4ds6oy", "bafkreifyvp3t6ep6ftqjkq3h65zdmgr2breeqp4k7rmdmsibel4755vh4e", "bafkreibpxgd3vlearz3rzm75zlw2wjq5bsv4suxtrj5mrxdcqq2itrqlii", "bafkreidang5esgsroqj5dsrnj5flbxuciqki55zxir7waskre45oeefufa", "bafkreibwg7p7fikhvdtdtj5u2hyjaojkz6aceg4vx66yrc2rcwqhpxldoy", "bafkreibthn4ngz5yq3im6dtdlmeg3kwqldnqwp7bnljqiqufaduxqbp7ma", "bafkreicvu5ym5fsffx2y2czmfh4lozt7bt6pkn3xpsezii2rvsztqgar2e", "bafkreia5lywtofhazb2glmwem62jqrz67yetcq6oatucrywx4gltf7xhu4", "bafkreic43vhpdroul77ajihs4jdpdzfdb6qepm2lq23ywq54inthal7pym", "bafkreialkb4dcculkn2x3zr2snxxyh6laj7y43mrgh53s3kgrgvcqkwb3y", "bafkreidhnpecmu7yqbdmzi463hnuauq7lqidwnex5k5hs3dhsysd5u7s4u", "bafkreievimive25a2bgzerjwxchd6kme2cbm7hdhfnaxzgftqxo2bfeih4", "bafkreig26o4roqcowqc25yg3lzfbqcd4i2ezenfbwjsi5m6s6xnycakrue", "bafkreiewvhgytf4piiwqbyxv33byfaazsilps65avspfwb2s3bstwsdyza", "bafkreibfwuh2qwo5kfvddisonvbbd72mlxa4t6vlecesqfzqgirtjdr5wq", "bafkreibnq6h57f5pg7tly775t3ibpork2ftqv6s373v4vbxs2cwvtejj3e", "bafkreihdnarq4lfzlt2p2ypnk5als7n65xiexoutn5w7m3aixwff74c4vu", "bafkreiey6d3qspy2renhcumllqh5nrvclmepqwu5rpherjpum2lria26fm", "bafkreif6ug4sngfl4s4izcwdqoccgdhzkjhmpjp3gmiwcp6ho5xwljjqba", "bafkreiau64ltlfm6thzeitbfvda57ugiap5vogce6laphuutjewrlh2gvm", "bafkreibzephikut47bv3a4yik3zfach75pxmteprkfpofixnkdvmispslq"}
		// cids := []string{
		// 	"bafybeidp2bchkgkywd6vdmz3llcxaegih37u67xphd7qbf4tidxmmc5g3e",
		// 	"bafybeiedqh5ps3i2b5qk6e2u262wzgwbqwg3zqvbx2ysqwe2qx4ssytj4u",
		// 	"bafybeiexj2l3d6z6vl5fjqrhrtqefagixzctp52dqcchkaa2onqsqcew6e",
		// 	"bafybeiabw6ts7hyrno5tiq2jut4vfde6ctawfd5axdquq4anlms2ckd5uu",
		// 	"bafybeiev3435i57u5vu2a7x2v7j7t6w7fse5jdugs2tz7x2boyfpouh76q",
		// 	"bafybeifuswprp2e6l5ax6nu7bshx3hsf44bagyjap7kea4rag7lgi3u63y",
		// }
		// cids := []string{"bafkreidbf3hktvgog4lagl3ci7whr2bps5yd6llx5mm6a5zvpaf3k6swde", "bafkreih4yo6l3mrcruagdhrvejd5khiby7zs3sk6hts4y4asv3z5m2zppa", "bafkreide2hissxipyhkklzb4e7pveenxwf2gdimbrgf5wa4f3byxp6oxhy", "bafkreiepfnf5udgqcm2s4ithdd5fnxvimrguzanw7omh67nrf7htzd2gwi", "bafkreih7xvzo6u7fmqaf5iatpzmk3755b3m4kbr5dmia4giqnakyq7do74", "bafkreigseqextzsfvbqj56zdyddgebnmpw2fk4cypkl5jpudpmceu6vfhy", "bafkreigoawkncvlail6vfa347vafmnwmvm6ai7otrloyjsg2zgidi7giqu", "bafkreiblxx2lrjgxscic3wxhcckcczq5hvslf42dn4ymks7jjyvrbeeiju", "bafkreidmzn5tghb2enmysr3gctzsbzg4akwfflq3ego7ymnw3r26nkqw5e", "bafkreiehbn6f5qshw6p75hjoms7k6syuipelntmdjkqqubmtvwxxrjgbmy", "bafkreielvrmagujl2p2wiytetlko4u7zv3l7mzsfeptq6raiiisdfz3wmm", "bafkreifv5ax64liukjt6ev3zpvopdxqbsfzfm6lt4afohbe5tsxpkydvqa", "bafkreic4ifbeph5a52glwekorhysl7s6pyvkscwxfabbn657sykye4223u", "bafkreiay46pej6nlyxm6vtnvi44qxjy4i7zofhwca5yptbhtirv75se4ua", "bafkreicxkrjv6qfnydr5xnaexz2aqfez6is27cqpgahljvmcglqxqob6o4", "bafkreidzmqi32kq3rauzyieov2cyaaaeqdwdeqa7vf25dv7cuo26fohdnu", "bafkreiecjzypd4p37dpkdyhhhpwgmkaebbiw4aycn6qu4yarc5pngtryvu", "bafkreiabpinskwg5y5t3bxfsvvkvltlf44f4hoah4pzvvhhpz3conmdnpe", "bafkreicrmtbur5r4b3czqemvjphc7lbmhr6qmcqf5tnel7fvyooguep3eu", "bafkreifgbismouawnnbos5behwquvzslord2d3mfwxx7ysmyxw3xzrk76q", "bafkreicri3o57ajefpc2ln3x3sykmfv3utseyzuwvehdmge5xdqxwi4rbe", "bafkreigmcxqv7ycazskjrej472itjl7zeco6c43l5tndxzwxochk2qxyhq", "bafkreidxuvdl3zyws7nqf3jujph6wbgyfjwf4f4ofhyka7ohatg63umjia", "bafkreicfggry6zp2taupbda2pixe4o4wl3csru5omndu7t7u7bfuwij53y", "bafkreibc7oqpuprf6sft3zg4ci5xxerene45nrhhnipfqmvfyicmwicgmy", "bafkreictyy3desr7bntxl5ilfz7fcjcnbkocoovuqkoslw3ag6n7hxiivq", "bafkreibugeq6iqcktjnrwqjrzivieaqazkoopvttwd4lcwaolsevng7azq", "bafkreierh3vpgocxdj762hnkdunbmhv3u2ts4n3wkdbdji5ufjgswwgpzu", "bafkreia2osxiaucqifu6sddwq4dn5vtcfla36ovols7fhktakjvl3ghaim", "bafkreieyszomcefotlcgska6rnmvopvk2bowzmdhieu24lyfzohmr7rbnu", "bafkreidcejxlav5rfy4lx6vkmk6wpf6xibn6rqb42y2uknl2ncd7j74iym", "bafkreiekv7uxcq4hwud3baaiacagaxox5q32pvanr46hy7dszvpeeicxtu", "bafkreibxkhsllrmcpzlqye2coq5ydhiw6s626p2tybjpqtpxcnodiz7boa", "bafkreidr5drxs7rwafzh2bcgtxt5zkgtcdxsvm42k3ddmgrg3qmujz4v7m", "bafkreiaptpa44w7u6bd6bpduwrui2qka5rjzbj3mspk6zyt4jdcjx4qkim", "bafkreidpyzkxmfgdvlny7lusv73nbbwc242dl3pmkxw23zm2xws6nybbje", "bafkreicuzuoreye6qpyjrgt6nbvplxnywau37guda3o6qfz7cgg2pihoue", "bafkreibsb3ipr4e7bezbgsmm5uyktfvrwmgnw5engfi3aoibo7g2gohy4i", "bafkreiabh2htnfjtqhhmqmccgrmirvmcvvprc5wp6zhkwalawv74vfno2i"}
		// for _, deviceID := range nodes {
		// 	err = schedulerAPI.CacheData(ctx, cids, deviceID)
		// 	if err != nil {
		// 		log.Warnf("CacheData err:%v,deviceID:%v", err, deviceID)
		// 	}
		// }

		// "bafkreiblxx2lrjgxscic3wxhcckcczq5hvslf42dn4ymks7jjyvrbeeiju", "bafkreidmzn5tghb2enmysr3gctzsbzg4akwfflq3ego7ymnw3r26nkqw5e", "bafkreiehbn6f5qshw6p75hjoms7k6syuipelntmdjkqqubmtvwxxrjgbmy", "bafkreielvrmagujl2p2wiytetlko4u7zv3l7mzsfeptq6raiiisdfz3wmm", "bafkreifv5ax64liukjt6ev3zpvopdxqbsfzfm6lt4afohbe5tsxpkydvqa", "bafkreic4ifbeph5a52glwekorhysl7s6pyvkscwxfabbn657sykye4223u", "bafkreiay46pej6nlyxm6vtnvi44qxjy4i7zofhwca5yptbhtirv75se4ua", "bafkreicxkrjv6qfnydr5xnaexz2aqfez6is27cqpgahljvmcglqxqob6o4", "bafkreidzmqi32kq3rauzyieov2cyaaaeqdwdeqa7vf25dv7cuo26fohdnu", "bafkreiecjzypd4p37dpkdyhhhpwgmkaebbiw4aycn6qu4yarc5pngtryvu", "bafkreiabpinskwg5y5t3bxfsvvkvltlf44f4hoah4pzvvhhpz3conmdnpe", "bafkreicrmtbur5r4b3czqemvjphc7lbmhr6qmcqf5tnel7fvyooguep3eu", "bafkreifgbismouawnnbos5behwquvzslord2d3mfwxx7ysmyxw3xzrk76q", "bafkreicri3o57ajefpc2ln3x3sykmfv3utseyzuwvehdmge5xdqxwi4rbe", "bafkreigmcxqv7ycazskjrej472itjl7zeco6c43l5tndxzwxochk2qxyhq", "bafkreidxuvdl3zyws7nqf3jujph6wbgyfjwf4f4ofhyka7ohatg63umjia", "bafkreicfggry6zp2taupbda2pixe4o4wl3csru5omndu7t7u7bfuwij53y", "bafkreibc7oqpuprf6sft3zg4ci5xxerene45nrhhnipfqmvfyicmwicgmy", "bafkreictyy3desr7bntxl5ilfz7fcjcnbkocoovuqkoslw3ag6n7hxiivq", "bafkreibugeq6iqcktjnrwqjrzivieaqazkoopvttwd4lcwaolsevng7azq", "bafkreierh3vpgocxdj762hnkdunbmhv3u2ts4n3wkdbdji5ufjgswwgpzu", "bafkreia2osxiaucqifu6sddwq4dn5vtcfla36ovols7fhktakjvl3ghaim", "bafkreieyszomcefotlcgska6rnmvopvk2bowzmdhieu24lyfzohmr7rbnu", "bafkreidcejxlav5rfy4lx6vkmk6wpf6xibn6rqb42y2uknl2ncd7j74iym", "bafkreiekv7uxcq4hwud3baaiacagaxox5q32pvanr46hy7dszvpeeicxtu", "bafkreibxkhsllrmcpzlqye2coq5ydhiw6s626p2tybjpqtpxcnodiz7boa", "bafkreidr5drxs7rwafzh2bcgtxt5zkgtcdxsvm42k3ddmgrg3qmujz4v7m", "bafkreiaptpa44w7u6bd6bpduwrui2qka5rjzbj3mspk6zyt4jdcjx4qkim", bafkreidpyzkxmfgdvlny7lusv73nbbwc242dl3pmkxw23zm2xws6nybbje, bafkreicuzuoreye6qpyjrgt6nbvplxnywau37guda3o6qfz7cgg2pihoue, bafkreibsb3ipr4e7bezbgsmm5uyktfvrwmgnw5engfi3aoibo7g2gohy4i, bafkreiabh2htnfjtqhhmqmccgrmirvmcvvprc5wp6zhkwalawv74vfno2i

		return err
	},
}
