package locator

import (
	"context"
	"database/sql"
	"fmt"
	"net"

	"go.uber.org/fx"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/common"
	"github.com/linguohua/titan/node/handler"
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

type Locator struct {
	fx.In

	*common.CommonAPI
	ApMgr *AccessPointMgr
	DB    *SqlDB
}

func isValid(geo string) bool {
	return len(geo) > 0 && geo != unknownAreaID
}

func (locator *Locator) GetAccessPoints(ctx context.Context, deviceID string) ([]string, error) {
	schedulerAPI, err := locator.getSchedulerAPIWith(deviceID)
	if err == nil { // device may be not exit if err != nil
		return []string{schedulerAPI.url}, nil
	}

	if err != sql.ErrNoRows {
		return nil, err
	}

	// if device not exist, get scheduler by ip location
	remoteAddr := handler.GetRemoteAddr(ctx)
	areaID, err := locator.getAreaIDWith(remoteAddr)
	if err != nil {
		return nil, err
	}

	return locator.getAccessPointsWithWeightCount(areaID)
}

func (locator *Locator) AddAccessPoint(ctx context.Context, areaID string, schedulerURL string, weight int, schedulerAccessToken string) error {
	if weight < miniWeight || weight > maxWeight {
		return fmt.Errorf("weith is out of range, need to set %d ~ %d", miniWeight, maxWeight)
	}

	exist, err := locator.DB.isAccessPointExist(schedulerURL)
	if err != nil {
		return err
	}

	if exist {
		return fmt.Errorf("access point aready exist")
	}

	_, err = locator.ApMgr.newSchedulerAPI(schedulerURL, areaID, schedulerAccessToken)
	if err != nil {
		return err
	}

	return locator.DB.addAccessPoint(areaID, schedulerURL, weight, schedulerAccessToken)
}

func (locator *Locator) RemoveAccessPoints(ctx context.Context, areaID string) error {
	locator.ApMgr.removeAccessPoint(areaID)
	return locator.DB.removeAccessPoints(areaID)
}

func (locator *Locator) ListAreaIDs(ctx context.Context) (areaIDs []string, err error) {
	return locator.DB.listAreaIDs()
}

func (locator *Locator) ShowAccessPoint(ctx context.Context, areaID string) (api.AccessPoint, error) {
	schedulerCfgs, err := locator.DB.getAccessPointCfgs(areaID)
	if err != nil {
		return api.AccessPoint{}, err
	}
	infos := make([]api.SchedulerInfo, 0, len(schedulerCfgs))
	for _, cfg := range schedulerCfgs {
		schedulerInfo := api.SchedulerInfo{URL: cfg.SchedulerURL, Weight: cfg.Weight}
		infos = append(infos, schedulerInfo)
	}

	accessPoint := api.AccessPoint{AreaID: areaID, SchedulerInfos: infos}
	return accessPoint, nil
}

func (locator *Locator) SetDeviceOnlineStatus(ctx context.Context, deviceID string, isOnline bool) error {
	log.Debugf("SetDeviceStatus device %s online status %t", deviceID, isOnline)

	info, err := locator.DB.getDeviceInfo(deviceID)
	if err != nil {
		log.Errorf("SetDeviceStatus, get device %s error:%s", deviceID, err.Error())
		return err
	}

	locator.DB.setDeviceInfo(deviceID, info.SchedulerURL, info.AreaID, isOnline)
	return nil
}

func (locator *Locator) getAccessPointsWithWeightCount(areaID string) ([]string, error) {
	log.Debugf("getAccessPointWithWeightCount, areaID:%s", areaID)

	schedulerCfgs, err := locator.DB.getAccessPointCfgs(areaID)
	if err != nil {
		return nil, err
	}

	if len(schedulerCfgs) == 0 {
		return make([]string, 0), nil
	}

	// filter online scheduler
	onlineSchedulerAPIs := make(map[string]*schedulerAPI)
	onlineSchedulerCfgs := make(map[string]*schedulerCfg)
	for _, cfg := range schedulerCfgs {
		api, err := locator.ApMgr.getSchedulerAPI(cfg.SchedulerURL, areaID, cfg.AccessToken)
		if err != nil {
			log.Warnf("getAccessPointsWithWeightCount, getSchedulerAPI error:%s", err.Error())
			continue
		}

		var schedulerCfg = cfg
		onlineSchedulerCfgs[schedulerCfg.SchedulerURL] = schedulerCfg
		onlineSchedulerAPIs[schedulerCfg.SchedulerURL] = api
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

	return schedulerURLs, nil
}

func countSchedulerWeightWithCfgs(schedulerCfgs map[string]*schedulerCfg) map[string]float32 {
	totalWeight := 0
	for _, cfg := range schedulerCfgs {
		totalWeight += cfg.Weight
	}

	result := make(map[string]float32)
	for _, cfg := range schedulerCfgs {
		result[cfg.SchedulerURL] = float32(cfg.Weight) / float32(totalWeight)
	}

	return result
}

func (locator *Locator) countSchedulerWeightByDevice(schedulerCfgs map[string]*schedulerCfg) map[string]float32 {
	totalWeight := 0

	weightMap := make(map[string]int)
	for _, cfg := range schedulerCfgs {
		count, err := locator.DB.countDeviceOnScheduler(cfg.SchedulerURL)
		if err != nil {
			log.Errorf("countSchedulerWeightByDevice, error:%s", err.Error())
			continue
		}

		weightMap[cfg.SchedulerURL] = count
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

// if device not exist return sql.ErrNoRows
func (locator *Locator) getSchedulerAPIWith(deviceID string) (*schedulerAPI, error) {
	device, err := locator.DB.getDeviceInfo(deviceID)
	if err != nil {
		log.Errorf("GetAccessPoints, getDeviceInfo error:%s", err.Error())
		return nil, err
	}

	cfg, err := locator.DB.getSchedulerCfg(device.SchedulerURL)
	if err != nil {
		return nil, err
	}

	return locator.ApMgr.getSchedulerAPI(device.SchedulerURL, device.AreaID, cfg.AccessToken)
}

func (locator *Locator) GetDownloadInfosWithCarfile(ctx context.Context, cid string, publicKey string) ([]*api.DownloadInfoResult, error) {
	remoteAddr := handler.GetRemoteAddr(ctx)
	areaID, err := locator.getAreaIDWith(remoteAddr)
	if err != nil {
		return nil, err
	}

	schedulerAPI := locator.ApMgr.randSchedulerAPI(areaID)
	if schedulerAPI == nil {
		schedulerAPI, err = locator.getFirstOnlineSchedulerAPIAt(areaID)
		if err != nil {
			return nil, err
		}
	}

	if schedulerAPI != nil {
		return schedulerAPI.GetDownloadInfosWithCarfile(ctx, cid, publicKey)
	}

	return make([]*api.DownloadInfoResult, 0), nil
}

func (locator *Locator) UserDownloadBlockResults(ctx context.Context, results []api.UserBlockDownloadResult) error {
	remoteAddr := handler.GetRemoteAddr(ctx)
	areaID, err := locator.getAreaIDWith(remoteAddr)
	if err != nil {
		return err
	}

	schedulerAPI := locator.ApMgr.randSchedulerAPI(areaID)
	if schedulerAPI == nil {
		schedulerAPI, err = locator.getFirstOnlineSchedulerAPIAt(areaID)
		if err != nil {
			return err
		}
	}

	if schedulerAPI != nil {
		return schedulerAPI.UserDownloadBlockResults(ctx, results)
	}
	return nil
}

// return defaultAreaID if can not get areaID
func (locator *Locator) getAreaIDWith(remoteAddr string) (string, error) {
	ip, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return "", err
	}

	geoInfo, err := region.GetRegion().GetGeoInfo(ip)
	if err != nil {
		log.Errorf("getAreaID error %s", err.Error())
		return "", err
	}

	if geoInfo != nil && isValid(geoInfo.Geo) {
		return geoInfo.Geo, nil
	}

	return defaultAreaID, nil
}

func (locator *Locator) getFirstOnlineSchedulerAPIAt(areaID string) (*schedulerAPI, error) {
	schedulerCfgs, err := locator.DB.getAccessPointCfgs(areaID)
	if err != nil {
		log.Errorf("getCfg, acccess point %s not exist", areaID)
		return nil, err
	}

	for _, cfg := range schedulerCfgs {
		api, err := locator.ApMgr.getSchedulerAPI(cfg.SchedulerURL, areaID, cfg.AccessToken)
		if err != nil {
			log.Warnf("getFirstOnlineSchedulerAPIAt, areaID:%s, scheduler %s offline", areaID, cfg.SchedulerURL)
			continue
		}

		return api, nil
	}

	return nil, fmt.Errorf("area %s no online scheduler exist", areaID)
}

func (locator *Locator) AllocateNodes(ctx context.Context, schedulerURL string, nodeType api.NodeType, count int) ([]api.NodeAllocateInfo, error) {
	cfg, err := locator.DB.getSchedulerCfg(schedulerURL)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("scheduler %s not exist", schedulerURL)
		}
		return nil, err
	}

	schedulerAPI, err := locator.ApMgr.getSchedulerAPI(schedulerURL, cfg.AreaID, cfg.AccessToken)
	if err != nil {
		return nil, err
	}

	registerInfos, err := schedulerAPI.AllocateNodes(ctx, nodeType, count)
	if err != nil {
		return nil, err
	}

	for _, registerInfo := range registerInfos {
		locator.DB.setDeviceInfo(registerInfo.DeviceID, schedulerURL, cfg.AreaID, false)
	}

	return registerInfos, nil
}

func (locator *Locator) LoadAccessPointsForWeb(ctx context.Context) ([]api.AccessPoint, error) {
	allCfg, err := locator.DB.getAllCfg()
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

// load user access point for special user ip
func (locator *Locator) LoadUserAccessPoint(ctx context.Context, userIP string) (api.AccessPoint, error) {
	areaID := defaultAreaID
	geoInfo, _ := region.GetRegion().GetGeoInfo(userIP)
	if geoInfo != nil && isValid(geoInfo.Geo) {
		areaID = geoInfo.Geo
	}

	schedulerCfgs, err := locator.DB.getAccessPointCfgs(areaID)
	if err != nil {
		return api.AccessPoint{}, err
	}

	infos := make([]api.SchedulerInfo, 0, len(schedulerCfgs))
	for _, cfg := range schedulerCfgs {
		schedulerInfo := api.SchedulerInfo{URL: cfg.SchedulerURL, Weight: cfg.Weight}
		infos = append(infos, schedulerInfo)
	}

	accessPoint := api.AccessPoint{AreaID: areaID, SchedulerInfos: infos}
	return accessPoint, nil
}
