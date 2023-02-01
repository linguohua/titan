package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/filecoin-project/go-jsonrpc"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/client"
	"github.com/linguohua/titan/build"
	lcli "github.com/linguohua/titan/cli"
	"github.com/linguohua/titan/lib/titanlog"
	"github.com/shirou/gopsutil/v3/cpu"

	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
)

var log = logging.Logger("main")

// const (
// 	FlagWorkerRepo            = "edge-repo"
// 	FlagWorkerRepoDeprecation = "edgerepo"
// 	DefaultCarfileStoreDir    = "carfilestore"
// )

func main() {
	api.RunningNodeType = api.NodeEdge
	cpu.Percent(0, false)
	titanlog.SetupLogLevels()

	local := []*cli.Command{
		runCmd,
	}

	local = append(local, lcli.CommonCommands...)

	app := &cli.App{
		Name:                 "titan-update",
		Usage:                "Titan update",
		Version:              build.UserVersion(),
		EnableBashCompletion: true,
		// Flags: []cli.Flag{
		// 	&cli.StringFlag{
		// 		Name:    FlagWorkerRepo,
		// 		Aliases: []string{FlagWorkerRepoDeprecation},
		// 		EnvVars: []string{"TITAN_EDGE_PATH", "EDGE_PATH"},
		// 		Value:   "~/.titanedge", // TODO: Consider XDG_DATA_HOME
		// 		Usage:   fmt.Sprintf("Specify worker repo path. flag %s and env TITAN_EDGE_PATH are DEPRECATION, will REMOVE SOON", FlagWorkerRepoDeprecation),
		// 	},
		// 	&cli.StringFlag{
		// 		Name:    "panic-reports",
		// 		EnvVars: []string{"TITAN_PANIC_REPORT_PATH"},
		// 		Hidden:  true,
		// 		Value:   "~/.titanupdate", // should follow --repo default
		// 	},
		// },

		// After: func(c *cli.Context) error {
		// 	if r := recover(); r != nil {
		// 		// Generate report in LOTUS_PATH and re-raise panic
		// 		build.GeneratePanicReport(c.String("panic-reports"), c.String(FlagWorkerRepo), c.App.Name)
		// 		panic(r)
		// 	}
		// 	return nil
		// },
		Commands: append(local, lcli.EdgeCmds...),
	}
	app.Setup()
	// app.Metadata["repoType"] = repo.Worker

	if err := app.Run(os.Args); err != nil {
		log.Errorf("%+v", err)
		return
	}
}

var runCmd = &cli.Command{
	Name:  "run",
	Usage: "Start titan edge node",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "update-server",
			Usage: "update server url",
			Value: "http://192.168.0.132:3456/rpc/v0",
		},
		&cli.StringFlag{
			Name:  "install-path",
			Usage: "install path",
			Value: "/root/edge",
		},
	},
	Before: func(cctx *cli.Context) error {
		return nil
	},
	Action: func(cctx *cli.Context) error {
		log.Info("Starting titan edge node")
		schedulerURL := cctx.String("update-server")
		schedulerAPI, close, err := newSchedulerAPI(cctx, schedulerURL)
		if err != nil {
			return err
		}
		defer close()

		for {
			checkUpdate(cctx, schedulerAPI)

			time.Sleep(60 * time.Second)
		}

	},
}

func newSchedulerAPI(cctx *cli.Context, schedulerURL string) (api.Scheduler, jsonrpc.ClientCloser, error) {
	ctx, cancel := context.WithTimeout(context.TODO(), 15*time.Second)
	defer cancel()

	schedulerAPI, closer, err := client.NewScheduler(ctx, schedulerURL, nil)
	if err != nil {
		return nil, nil, err
	}
	return schedulerAPI, closer, nil
}

func checkUpdate(cctx *cli.Context, schedulerAPI api.Scheduler) {
	updateInfos, err := getNodeAppUpdateInfos(schedulerAPI)
	if err != nil {
		log.Errorf("getNodeAppUpdateInfo error:%s", err.Error())
	}

	mySelfUpdateInfo, ok := updateInfos[int(api.NodeUpdate)]
	if ok && isNeedToUpdateMySelf(cctx, mySelfUpdateInfo) {
		updateApp(cctx, mySelfUpdateInfo)
	}

	edgeUpdateInfo, ok := updateInfos[int(api.NodeEdge)]
	if ok && isNeedToUpdateEdge(cctx, edgeUpdateInfo) {
		updateApp(cctx, edgeUpdateInfo)
	}
}

func getNodeAppUpdateInfos(schedulerAPI api.Scheduler) (map[int]*api.NodeAppUpdateInfo, error) {
	ctx, cancel := context.WithTimeout(context.TODO(), 15*time.Second)
	defer cancel()

	return schedulerAPI.GetNodeAppUpdateInfos(ctx)
}

func getLocalEdgeVersion(appPath string) (api.Version, error) {
	cmd := exec.Command(appPath, "-v")
	stdout, err := cmd.Output()
	if err != nil {
		return api.Version(0), err
	}

	stringSplit := strings.Split(string(stdout), " ")
	if len(stringSplit) != 3 {
		return api.Version(0), fmt.Errorf("parse cmd output error")
	}

	return newVersion(stringSplit[3])
}

func isNeedToUpdateMySelf(cctx *cli.Context, updateInfo *api.NodeAppUpdateInfo) bool {
	version, err := newVersion(cctx.App.Version)
	if err != nil {
		log.Errorf("newVersion error:%s", err.Error())
		return false
	}

	if updateInfo.Version > version {
		return true
	}
	return false
}

func isNeedToUpdateEdge(cctx *cli.Context, updateInfo *api.NodeAppUpdateInfo) bool {
	installPath := cctx.String("install-path")
	version, err := getLocalEdgeVersion(installPath)
	if err != nil {
		log.Errorf("getLocalEdgeVersion error:%s", err)
		return false
	}

	if updateInfo.Version > version {
		return true
	}
	return false
}

func updateApp(cctx *cli.Context, updateInfo *api.NodeAppUpdateInfo) {
	log.Infof("updateApp, AppName:%s, Hash:%s, newVersion:%s, downloadURL:%s", updateInfo.AppName, updateInfo.Hash, updateInfo.Version.String(), updateInfo.DownloadURL)
	installPath := cctx.String("install-path")
	data, err := downloadApp(updateInfo.DownloadURL)
	if err != nil {
		log.Errorf("download app error:%s", err)
		return
	}

	// check file hash
	h := sha256.New()
	h.Write(data)
	hash := hex.EncodeToString(h.Sum(nil))

	if updateInfo.Hash != hash {
		log.Errorf("download file imcomplete, file hash %s != %s", hash, updateInfo.Hash)
		return
	}

	fileName := updateInfo.AppName + "_tmp"
	tmpFilePath := filepath.Join(installPath, fileName)

	err = os.WriteFile(tmpFilePath, data, 0644)
	if err != nil {
		log.Errorf("save app error:%s", err)
		return
	}

	filePath := filepath.Join(installPath, updateInfo.AppName)
	err = os.Remove(filePath)
	if err != nil {
		log.Errorf("remove old app error:%s", err)
		return
	}

	err = os.Rename(tmpFilePath, filePath)
	if err != nil {
		log.Errorf("remove old app error:%s", err)
		return
	}
}

func downloadApp(downloadURL string) ([]byte, error) {
	client := &http.Client{Timeout: 30 * time.Second}
	req, err := http.NewRequest("GET", downloadURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func newVersion(version string) (api.Version, error) {
	stringSplit := strings.Split(version, ".")
	if len(stringSplit) != 3 {
		return api.Version(0), fmt.Errorf("parse version error")
	}

	//major, minor, patch
	major, err := strconv.Atoi(stringSplit[0])
	if err != nil {
		return api.Version(0), fmt.Errorf("parse version major error:%s", err)
	}

	minor, err := strconv.Atoi(stringSplit[1])
	if err != nil {
		return api.Version(0), fmt.Errorf("parse version minor error:%s", err)
	}

	patch, err := strconv.Atoi(stringSplit[2])
	if err != nil {
		return api.Version(0), fmt.Errorf("parse version patch error:%s", err)
	}

	return api.Version(uint32(major)<<16 | uint32(minor)<<8 | uint32(patch)), nil

}
