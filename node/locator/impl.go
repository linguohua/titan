package locator

import (
	"context"
	"fmt"
	"time"

	"github.com/filecoin-project/go-jsonrpc/auth"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/common"
	"github.com/linguohua/titan/node/config"
	"github.com/linguohua/titan/node/handler"
	"github.com/linguohua/titan/node/repo"
	"github.com/linguohua/titan/node/secret"
	"github.com/linguohua/titan/region"

	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("locator")

const (
	miniWeight = 0
	maxWeight  = 1000
	// 3 seconds
	connectTimeout = 3
	defaultAreaID  = "CN-GD-Shenzhen"
)

func NewLocalLocator(ctx context.Context, lr repo.LockedRepo, dbAddr, uuid string, locatorPort int) api.Locator {
	locator := &Locator{}
	if len(dbAddr) > 0 {
		locator.db = newDB(dbAddr)
		locator.cfg = locator.db
	} else {
		locator.cfg = newLocalCfg(lr)
	}

	sec, err := secret.APISecret(lr)
	if err != nil {
		log.Panicf("NewLocalScheduleNode, new APISecret failed:%s", err.Error())
	}
	locator.APISecret = sec

	// auth new token to scheduler
	token, err := locator.AuthNew(ctx, []auth.Permission{api.PermRead, api.PermWrite})
	if err != nil {
		log.Panicf("NewLocalScheduleNode,new token to scheduler:%s", err.Error())
	}

	locator.apMgr = newAccessPointMgr(locatorPort, string(token), uuid)
	return locator

}

type lconfig interface {
	addAccessPoints(areaID string, schedulerURL string, weight int, accessToken string) error
	removeAccessPoints(areaID string) error
	listAccessPoints() (areaIDs []string, err error)
	getAccessPoint(areaID string) (*config.AccessPoint, error)
	isAccessPointExist(areaID, schedulerURL string) (bool, error)
}

type Locator struct {
	common.CommonAPI
	cfg   lconfig
	apMgr *accessPointMgr
	db    *db
}

func (locator *Locator) GetAccessPoints(ctx context.Context, deviceID string, securityKey string) ([]api.SchedulerAuth, error) {
	// TODO: verify securityKey
	ip := handler.GetRequestIP(ctx)
	areaID := ""
	geoInfo, err := region.GetRegion().GetGeoInfo(ip)
	if err != nil {
		log.Errorf("GetAccessPoints get geo from ip error %s", err.Error())
	} else {
		areaID = geoInfo.Geo
	}

	log.Infof("device %s ip %s get Area areaID %s", deviceID, ip, areaID)

	if areaID == "" || areaID == "unknown-unknown-unknown" {
		log.Warnf("GetAccessPoints device %s can not get areaID, use default areaID:%s", deviceID, defaultAreaID)
		areaID = defaultAreaID
	}

	device, err := locator.db.getDeviceInfo(deviceID)
	if err != nil {
		log.Errorf("GetAccessPoints, getDeviceInfo:%s", err.Error())
		return []api.SchedulerAuth{}, err
	}

	if device == nil || device.AreaID != areaID {
		log.Info("device == nil || device.AreaID != areaID")
		return locator.getAccessPointWithWeightCount(areaID, securityKey)
	}

	cfg := locator.getCfg(areaID, device.SchedulerURL)
	if cfg == nil {
		return locator.getAccessPointWithWeightCount(areaID, securityKey)
	}

	schedulerApi, ok := locator.apMgr.getSchedulerAPI(device.SchedulerURL, areaID, cfg.AccessToken)
	if ok {
		token, err := locator.newAuthTokenFromScheduler(schedulerApi, securityKey)
		if err == nil {
			auth := api.SchedulerAuth{URL: device.SchedulerURL, AccessToken: token}
			return []api.SchedulerAuth{auth}, nil
		}

		log.Errorf("GetAccessPoints authNewToken error:%s", err.Error())
	}

	log.Infof("area %s scheduler api %s not online", areaID, device.SchedulerURL)
	return locator.getAccessPointWithWeightCount(areaID, securityKey)
}

func (locator *Locator) AddAccessPoints(ctx context.Context, areaID string, schedulerURL string, weight int, schedulerAccessToken string) error {
	if weight < miniWeight || weight > maxWeight {
		return fmt.Errorf("weith is out of range, need to set %d ~ %d", miniWeight, maxWeight)
	}

	exist, err := locator.cfg.isAccessPointExist(areaID, schedulerURL)
	if err != nil {
		return err
	}

	if exist {
		return fmt.Errorf("access point aready exist")
	}

	_, err = locator.apMgr.newSchedulerAPI(schedulerURL, areaID, schedulerAccessToken)
	if err != nil {
		return err
	}

	return locator.cfg.addAccessPoints(areaID, schedulerURL, weight, schedulerAccessToken)
}

func (locator *Locator) RemoveAccessPoints(ctx context.Context, areaID string) error {
	locator.apMgr.removeAccessPoint(areaID)
	return locator.cfg.removeAccessPoints(areaID)
}

func (locator *Locator) ListAccessPoints(ctx context.Context) (areaIDs []string, err error) {
	return locator.cfg.listAccessPoints()
}

func (locator *Locator) ShowAccessPoint(ctx context.Context, areaID string) (api.AccessPoint, error) {
	ap, err := locator.cfg.getAccessPoint(areaID)
	if err != nil {
		return api.AccessPoint{}, err
	}
	infos := make([]api.SchedulerInfo, 0, len(ap.SchedulerCfgs))
	for _, cfg := range ap.SchedulerCfgs {
		schedulerInfo := api.SchedulerInfo{URL: cfg.URL, Weight: cfg.Weight, AccessToken: cfg.AccessToken}
		_, ok := locator.apMgr.getSchedulerAPI(cfg.URL, areaID, cfg.AccessToken)
		if ok {
			schedulerInfo.Online = true
		}

		infos = append(infos, schedulerInfo)
	}

	accessPoint := api.AccessPoint{AreaID: ap.AreaID, SchedulerInfos: infos}
	return accessPoint, nil
}

func (locator *Locator) DeviceOnline(ctx context.Context, deviceID string, areaID string, port int) error {
	log.Infof("areaID:%s device %s online", areaID, deviceID)
	ip := handler.GetRequestIP(ctx)
	schedulerURL := fmt.Sprintf("http://%s:%d/rpc/v0", ip, port)
	locator.db.db.setDeviceInfo(deviceID, schedulerURL, areaID, true)
	return nil
}

func (locator *Locator) DeviceOffline(ctx context.Context, deviceID string) error {
	log.Infof("device %s offline", deviceID)
	info, err := locator.db.getDeviceInfo(deviceID)
	if err != nil {
		log.Errorf("DeviceOffline, get device %s error:%s", deviceID, err.Error())
		return err
	}

	if info == nil {
		log.Errorf("DeviceOffline, device %s not in locator", deviceID)
		return fmt.Errorf("device %s not exist", deviceID)
	}

	locator.db.db.setDeviceInfo(deviceID, info.SchedulerURL, info.AreaID, false)
	return nil
}

func (locator *Locator) getAccessPointWithWeightCount(areaID, securityKey string) ([]api.SchedulerAuth, error) {
	log.Infof("getAccessPointWithWeightCount, areaID:%s", areaID)

	ap, err := locator.cfg.getAccessPoint(areaID)
	if err != nil {
		return []api.SchedulerAuth{}, err
	}

	if len(ap.SchedulerCfgs) == 0 {
		return []api.SchedulerAuth{}, nil
	}

	// filter online scheduler
	onlineSchedulerAPI := make(map[string]*schedulerAPI)
	onlineSchedulerCfgs := make(map[string]*config.SchedulerCfg)
	for _, cfg := range ap.SchedulerCfgs {
		api, ok := locator.apMgr.getSchedulerAPI(cfg.URL, areaID, cfg.AccessToken)
		if !ok {
			continue
		}

		var schedulerCfg = cfg
		onlineSchedulerCfgs[schedulerCfg.URL] = &schedulerCfg
		onlineSchedulerAPI[schedulerCfg.URL] = api
	}

	cfgWeights := countSchedulerWeightWithCfgs(onlineSchedulerCfgs)
	currentWeights := locator.countSchedulerWeightByDevice(onlineSchedulerCfgs)

	urls := make([]string, 0)
	for url, weight := range cfgWeights {
		currentWeight := currentWeights[url]
		if currentWeight <= weight {
			urls = append(urls, url)
		}
	}

	auths := make([]api.SchedulerAuth, 0, len(urls))
	for _, url := range urls {
		schedulerAPI, ok := onlineSchedulerAPI[url]
		if !ok {
			log.Errorf("onlineSchedulerAPI can not find scheduler %s", url)
			continue
		}
		accessToken, err := locator.newAuthTokenFromScheduler(schedulerAPI, securityKey)
		if err != nil {
			log.Errorf("onlineSchedulerAPI can not new authToken from scheduler %s, error: %s", url, err.Error())
			continue
		}
		auth := api.SchedulerAuth{URL: url, AccessToken: accessToken}
		auths = append(auths, auth)
	}

	log.Infof("area %s onlineSchedulers count:%d, access server %v", areaID, len(onlineSchedulerCfgs), urls)
	return auths, nil
}

func countSchedulerWeightWithCfgs(schedulerCfgs map[string]*config.SchedulerCfg) map[string]float32 {
	totalWeight := 0
	for _, cfg := range schedulerCfgs {
		totalWeight += cfg.Weight
	}

	result := make(map[string]float32)
	for _, cfg := range schedulerCfgs {
		result[cfg.URL] = float32(cfg.Weight) / float32(totalWeight)
	}

	return result
}

func (locator *Locator) countSchedulerWeightByDevice(schedulerCfgs map[string]*config.SchedulerCfg) map[string]float32 {
	totalWeight := 0

	weightMap := make(map[string]int)
	for _, cfg := range schedulerCfgs {
		count, err := locator.db.countDeviceOnScheduler(cfg.URL)
		if err != nil {
			log.Errorf("countSchedulerWeightByDevice, error:%s", err.Error())
			continue
		}

		weightMap[cfg.URL] = count
		totalWeight += count
	}

	result := make(map[string]float32)
	for url, weight := range weightMap {
		if totalWeight == 0 {
			result[url] = 0
		} else {
			result[url] = float32(weight) / float32(totalWeight)
		}
	}
	return result
}

func (locator *Locator) GetDownloadInfosWithBlocks(ctx context.Context, cids []string) (map[string][]api.DownloadInfo, error) {
	ip := handler.GetRequestIP(ctx)
	areaID := ""
	geoInfo, err := region.GetRegion().GetGeoInfo(ip)
	if err != nil {
		log.Errorf("GetAccessPoints get geo from ip error %s", err.Error())
	} else {
		areaID = geoInfo.Geo
	}

	log.Infof("user %s get Area areaID %s", ip, areaID)

	if areaID == "" || areaID == "unknown-unknown-unknown" {
		log.Errorf("user %s can not get areaID", ip)
		areaID = defaultAreaID
	}

	schedulerAPI, ok := locator.apMgr.randSchedulerAPI(areaID)
	if !ok {
		schedulerAPI, ok = locator.getFirstOnlineSchedulerAPIAt(areaID)
	}
	if ok {
		return schedulerAPI.GetDownloadInfosWithBlocks(ctx, cids)
	}

	return make(map[string][]api.DownloadInfo), nil

}
func (locator *Locator) GetDownloadInfoWithBlocks(ctx context.Context, cids []string) (map[string]api.DownloadInfo, error) {
	ip := handler.GetRequestIP(ctx)
	areaID := ""
	geoInfo, err := region.GetRegion().GetGeoInfo(ip)
	if err != nil {
		log.Errorf("GetAccessPoints get geo from ip error %s", err.Error())
	} else {
		areaID = geoInfo.Geo
	}

	log.Infof("user %s get Area areaID %s", ip, areaID)

	if areaID == "" || areaID == "unknown-unknown-unknown" {
		log.Errorf("user %s can not get areaID", ip)
		areaID = defaultAreaID
	}

	schedulerAPI, ok := locator.apMgr.randSchedulerAPI(areaID)
	if !ok {
		schedulerAPI, ok = locator.getFirstOnlineSchedulerAPIAt(areaID)
	}
	if ok {
		return schedulerAPI.GetDownloadInfoWithBlocks(ctx, cids)
	}
	return make(map[string]api.DownloadInfo), nil
}
func (locator *Locator) GetDownloadInfoWithBlock(ctx context.Context, cid string) (api.DownloadInfo, error) {
	ip := handler.GetRequestIP(ctx)
	areaID := ""
	geoInfo, err := region.GetRegion().GetGeoInfo(ip)
	if err != nil {
		log.Errorf("GetAccessPoints get geo from ip error %s", err.Error())
	} else {
		areaID = geoInfo.Geo
	}

	log.Infof("user %s get Area areaID %s", ip, areaID)

	if areaID == "" || areaID == "unknown-unknown-unknown" {
		log.Errorf("user %s can not get areaID", ip)
		areaID = defaultAreaID
	}

	schedulerAPI, ok := locator.apMgr.randSchedulerAPI(areaID)
	if !ok {
		schedulerAPI, ok = locator.getFirstOnlineSchedulerAPIAt(areaID)
	}
	if ok {
		return schedulerAPI.GetDownloadInfoWithBlock(ctx, cid)
	}
	return api.DownloadInfo{}, nil
}

func (locator *Locator) newAuthTokenFromScheduler(schedulerAPI *schedulerAPI, securityKey string) (string, error) {
	ctx, cancel := context.WithTimeout(context.TODO(), connectTimeout*time.Second)
	defer cancel()
	log.Infof("authNewTokenFromScheduler, securityKey:%s", securityKey)
	perms := []auth.Permission{api.PermRead, api.PermWrite}
	token, err := schedulerAPI.AuthNodeNew(ctx, perms, securityKey)
	if err != nil {
		return "", err
	}
	return string(token), err
}

func (locator *Locator) getCfg(areaID, schedulerURL string) *config.SchedulerCfg {
	accessPoint, err := locator.cfg.getAccessPoint(areaID)
	if err != nil {
		log.Errorf("getCfg, acccess point %s not exist", areaID)
		return nil
	}

	for _, cfg := range accessPoint.SchedulerCfgs {
		if cfg.URL == schedulerURL {
			return &cfg
		}
	}

	return nil
}

func (locator *Locator) getFirstOnlineSchedulerAPIAt(areaID string) (*schedulerAPI, bool) {
	accessPoint, err := locator.cfg.getAccessPoint(areaID)
	if err != nil {
		log.Errorf("getCfg, acccess point %s not exist", areaID)
		return nil, false
	}

	for _, cfg := range accessPoint.SchedulerCfgs {
		api, ok := locator.apMgr.getSchedulerAPI(cfg.URL, areaID, cfg.AccessToken)
		if ok {
			return api, true
		}
	}

	return nil, false
}
