package locator

import (
	"context"
	"fmt"

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
	unknownAreaID  = "unknown-unknown-unknown"
)

func NewLocalLocator(ctx context.Context, lr repo.LockedRepo, dbAddr, uuid string, locatorPort int) api.Locator {
	if len(dbAddr) == 0 {
		log.Panic("NewLocalLocator, db addr cannot empty")
	}

	locator := &Locator{}
	locator.db = newDB(dbAddr)

	sec, err := secret.APISecret(lr)
	if err != nil {
		log.Panicf("NewLocalLocator, new APISecret failed:%s", err.Error())
	}
	locator.APISecret = sec

	// auth new token to scheduler connect locator
	token, err := locator.AuthNew(ctx, []auth.Permission{api.PermRead, api.PermWrite})
	if err != nil {
		log.Panicf("NewLocalLocator,new token to scheduler:%s", err.Error())
	}

	locator.apMgr = newAccessPointMgr(locatorPort, string(token), uuid)
	return locator

}

type Locator struct {
	common.CommonAPI
	// cfg   lconfig
	apMgr *accessPointMgr
	db    *db
}

func (locator *Locator) GetAccessPoints(ctx context.Context, deviceID string) ([]string, error) {
	ip := handler.GetRequestIP(ctx)
	areaID := ""
	geoInfo, err := region.GetRegion().GetGeoInfo(ip)
	if err != nil {
		log.Errorf("GetAccessPoints get geo from ip error %s", err.Error())
	} else {
		areaID = geoInfo.Geo
	}

	log.Infof("device %s ip %s get Area areaID %s", deviceID, ip, areaID)

	if areaID == "" || areaID == unknownAreaID {
		log.Warnf("GetAccessPoints device %s can not get areaID, use default areaID:%s", deviceID, defaultAreaID)
		areaID = defaultAreaID
	}

	device, err := locator.db.getDeviceInfo(deviceID)
	if err != nil {
		log.Errorf("GetAccessPoints, getDeviceInfo error:%s", err.Error())
		return nil, err
	}

	if device == nil {
		log.Info("GetAccessPoints, device (%s) == nil", deviceID)
		return locator.getAccessPointWithWeightCount(areaID)
	}

	cfg := locator.getCfg(device.AreaID, device.SchedulerURL)
	if cfg == nil {
		return locator.getAccessPointWithWeightCount(device.AreaID)
	}

	_, ok := locator.apMgr.getSchedulerAPI(device.SchedulerURL, device.AreaID, cfg.AccessToken)
	if ok {
		return []string{device.SchedulerURL}, nil
	}

	log.Infof("area %s scheduler api %s not online", device.AreaID, device.SchedulerURL)
	return locator.getAccessPointWithWeightCount(areaID)
}

func (locator *Locator) AddAccessPoints(ctx context.Context, areaID string, schedulerURL string, weight int, schedulerAccessToken string) error {
	if weight < miniWeight || weight > maxWeight {
		return fmt.Errorf("weith is out of range, need to set %d ~ %d", miniWeight, maxWeight)
	}

	exist, err := locator.db.isAccessPointExist(areaID, schedulerURL)
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

	return locator.db.addAccessPoints(areaID, schedulerURL, weight, schedulerAccessToken)
}

func (locator *Locator) RemoveAccessPoints(ctx context.Context, areaID string) error {
	locator.apMgr.removeAccessPoint(areaID)
	return locator.db.removeAccessPoints(areaID)
}

func (locator *Locator) ListAccessPoints(ctx context.Context) (areaIDs []string, err error) {
	return locator.db.listAccessPoints()
}

func (locator *Locator) ShowAccessPoint(ctx context.Context, areaID string) (api.AccessPoint, error) {
	ap, err := locator.db.getAccessPoint(areaID)
	if err != nil {
		return api.AccessPoint{}, err
	}
	infos := make([]api.SchedulerInfo, 0, len(ap.SchedulerCfgs))
	for _, cfg := range ap.SchedulerCfgs {
		schedulerInfo := api.SchedulerInfo{URL: cfg.URL, Weight: cfg.Weight}
		// _, ok := locator.apMgr.getSchedulerAPI(cfg.URL, areaID, cfg.AccessToken)
		// if ok {
		// 	schedulerInfo.Online = true
		// }

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

func (locator *Locator) getAccessPointWithWeightCount(areaID string) ([]string, error) {
	log.Infof("getAccessPointWithWeightCount, areaID:%s", areaID)

	ap, err := locator.db.getAccessPoint(areaID)
	if err != nil {
		return nil, err
	}

	if len(ap.SchedulerCfgs) == 0 {
		return nil, nil
	}

	// filter online scheduler
	onlineSchedulerAPIs := make(map[string]*schedulerAPI)
	onlineSchedulerCfgs := make(map[string]*config.SchedulerCfg)
	for _, cfg := range ap.SchedulerCfgs {
		api, ok := locator.apMgr.getSchedulerAPI(cfg.URL, areaID, cfg.AccessToken)
		if !ok {
			continue
		}

		var schedulerCfg = cfg
		onlineSchedulerCfgs[schedulerCfg.URL] = &schedulerCfg
		onlineSchedulerAPIs[schedulerCfg.URL] = api
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

	schedulerURLs := make([]string, 0, len(urls))
	for _, url := range urls {
		_, ok := onlineSchedulerAPIs[url]
		if ok {
			schedulerURLs = append(schedulerURLs, url)
		}
	}

	log.Infof("area %s onlineSchedulers count:%d, access server %v", areaID, len(onlineSchedulerCfgs), schedulerURLs)
	return schedulerURLs, nil
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

func (locator *Locator) GetDownloadInfosWithBlocks(ctx context.Context, cids []string, publicKey string) (map[string][]api.DownloadInfoResult, error) {
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
		return schedulerAPI.GetDownloadInfosWithBlocks(ctx, cids, publicKey)
	}

	return make(map[string][]api.DownloadInfoResult), nil

}
func (locator *Locator) GetDownloadInfoWithBlocks(ctx context.Context, cids []string, publicKey string) (map[string]api.DownloadInfoResult, error) {
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
		return schedulerAPI.GetDownloadInfoWithBlocks(ctx, cids, publicKey)
	}
	return make(map[string]api.DownloadInfoResult), nil
}
func (locator *Locator) GetDownloadInfoWithBlock(ctx context.Context, cid string, publicKey string) (api.DownloadInfoResult, error) {
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
		return schedulerAPI.GetDownloadInfoWithBlock(ctx, cid, publicKey)
	}
	return api.DownloadInfoResult{}, nil
}

func (locator *Locator) UserDownloadBlockResults(ctx context.Context, results []api.UserBlockDownloadResult) error {
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
		return schedulerAPI.UserDownloadBlockResults(ctx, results)
	}
	return nil
}

func (locator *Locator) getCfg(areaID, schedulerURL string) *config.SchedulerCfg {
	accessPoint, err := locator.db.getAccessPoint(areaID)
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
	accessPoint, err := locator.db.getAccessPoint(areaID)
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

func (locator *Locator) getSchedulerAPIWithWeightCount(areaID string) ([]*schedulerAPI, error) {
	log.Infof("getSchedulerAPIWithWeightCount, areaID:%s", areaID)

	ap, err := locator.db.getAccessPoint(areaID)
	if err != nil {
		return nil, err
	}

	if len(ap.SchedulerCfgs) == 0 {
		return nil, nil
	}

	// filter online scheduler
	onlineSchedulerAPIs := make(map[string]*schedulerAPI)
	onlineSchedulerCfgs := make(map[string]*config.SchedulerCfg)
	for _, cfg := range ap.SchedulerCfgs {
		api, ok := locator.apMgr.getSchedulerAPI(cfg.URL, areaID, cfg.AccessToken)
		if !ok {
			continue
		}

		var schedulerCfg = cfg
		onlineSchedulerCfgs[schedulerCfg.URL] = &schedulerCfg
		onlineSchedulerAPIs[schedulerCfg.URL] = api
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

	schedulerAPIs := make([]*schedulerAPI, 0, len(urls))
	for _, url := range urls {
		onlineSchedulerAPI, ok := onlineSchedulerAPIs[url]
		if ok {
			schedulerAPIs = append(schedulerAPIs, onlineSchedulerAPI)
		}
	}
	return schedulerAPIs, nil
}

func (locator *Locator) RegisterNode(ctx context.Context, areaID string, schedulerURL string, nodeType api.NodeType, count int) ([]api.NodeRegisterInfo, error) {
	cfg := locator.getCfg(areaID, schedulerURL)
	if cfg == nil {
		return nil, fmt.Errorf("Scheduler %s not exist in area %s", schedulerURL, areaID)
	}

	schedulerAPI, ok := locator.apMgr.getSchedulerAPI(schedulerURL, areaID, cfg.AccessToken)
	if !ok {
		return nil, fmt.Errorf("Scheduler %s not online", schedulerURL)
	}

	return schedulerAPI.RegisterNode(ctx, nodeType, count)
}

func (locator *Locator) LoadAccessPointsForWeb(ctx context.Context) ([]api.AccessPoint, error) {
	allCfg, err := locator.db.db.getAllCfg()
	if err != nil {
		return make([]api.AccessPoint, 0), err
	}

	accessPointMap := make(map[string]*api.AccessPoint, 0)

	for _, cfg := range allCfg {
		ap, ok := accessPointMap[cfg.AreaID]
		if !ok {
			ap = &api.AccessPoint{AreaID: cfg.AreaID, SchedulerInfos: make([]api.SchedulerInfo, 0)}
			accessPointMap[cfg.AreaID] = ap
		}

		schedulerInfo := api.SchedulerInfo{URL: cfg.SchedulerURL, Weight: cfg.Weight}
		ap.SchedulerInfos = append(ap.SchedulerInfos, schedulerInfo)
	}

	accessPoints := make([]api.AccessPoint, 0, len(accessPointMap))
	for _, accessPoint := range accessPointMap {
		accessPoints = append(accessPoints, *accessPoint)
	}

	return accessPoints, nil
}

func (locator *Locator) LoadUserAccessPoint(ctx context.Context, userIP string) (api.AccessPoint, error) {
	areaID := defaultAreaID
	geoInfo, _ := region.GetRegion().GetGeoInfo(userIP)
	if geoInfo != nil && geoInfo.Geo != unknownAreaID {
		areaID = geoInfo.Geo
	}

	ap, err := locator.db.getAccessPoint(areaID)
	if err != nil {
		return api.AccessPoint{}, err
	}

	infos := make([]api.SchedulerInfo, 0, len(ap.SchedulerCfgs))
	for _, cfg := range ap.SchedulerCfgs {
		schedulerInfo := api.SchedulerInfo{URL: cfg.URL, Weight: cfg.Weight}
		infos = append(infos, schedulerInfo)
	}

	accessPoint := api.AccessPoint{AreaID: ap.AreaID, SchedulerInfos: infos}
	return accessPoint, nil
}
