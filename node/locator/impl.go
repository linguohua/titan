package locator

import (
	"context"
	"database/sql"
	"fmt"
	"net"

	"go.uber.org/fx"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/types"
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
	region.Region
}

func isValid(geo string) bool {
	return len(geo) > 0 && geo != unknownAreaID
}

func (locator *Locator) GetAccessPoints(ctx context.Context, nodeID string) ([]string, error) {
	schedulerAPI, err := locator.getSchedulerAPIWith(nodeID)
	if err == nil { // node may be not exit if err != nil
		return []string{schedulerAPI.url}, nil
	}

	if err != sql.ErrNoRows {
		return nil, err
	}

	// if node not exist, get scheduler by ip location
	remoteAddr := handler.GetRemoteAddr(ctx)
	areaID, err := locator.getAreaIDWith(remoteAddr)
	if err != nil {
		return nil, err
	}

	return locator.getAccessPointsWithWeightCount(areaID)
}

func (locator *Locator) AddAccessPoint(ctx context.Context, areaID string, schedulerURL string, weight int, schedulerAccessToken string) error {
	if weight < miniWeight || weight > maxWeight {
		return fmt.Errorf("weight is out of range, need to set %d ~ %d", miniWeight, maxWeight)
	}

	exist, err := locator.DB.isAccessPointExist(schedulerURL)
	if err != nil {
		return err
	}

	if exist {
		return fmt.Errorf("access point already exist")
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

func (locator *Locator) GetAccessPoint(ctx context.Context, areaID string) (api.AccessPoint, error) {
	schedulerConfigs, err := locator.DB.getAccessPointConfigs(areaID)
	if err != nil {
		return api.AccessPoint{}, err
	}
	infos := make([]api.SchedulerInfo, 0, len(schedulerConfigs))
	for _, cfg := range schedulerConfigs {
		schedulerInfo := api.SchedulerInfo{URL: cfg.SchedulerURL, Weight: cfg.Weight}
		infos = append(infos, schedulerInfo)
	}

	accessPoint := api.AccessPoint{AreaID: areaID, SchedulerInfos: infos}
	return accessPoint, nil
}

func (locator *Locator) UpdateNodeOnlineStatus(ctx context.Context, nodeID string, isOnline bool) error {
	log.Debugf("UpdateNodeOnlineStatus node %s online status %t", nodeID, isOnline)

	info, err := locator.DB.getNodeInfo(nodeID)
	if err != nil {
		log.Errorf("UpdateNodeOnlineStatus, get node %s error:%s", nodeID, err.Error())
		return err
	}

	return locator.DB.setNodeInfo(nodeID, info.SchedulerURL, info.AreaID, isOnline)
}

func (locator *Locator) getAccessPointsWithWeightCount(areaID string) ([]string, error) {
	log.Debugf("getAccessPointWithWeightCount, areaID:%s", areaID)

	schedulerConfigs, err := locator.DB.getAccessPointConfigs(areaID)
	if err != nil {
		return nil, err
	}

	if len(schedulerConfigs) == 0 {
		return make([]string, 0), nil
	}

	// filter online scheduler
	onlineSchedulerAPIs := make(map[string]*schedulerAPI)
	onlineSchedulerCfgs := make(map[string]*schedulerCfg)
	for _, cfg := range schedulerConfigs {
		api, err := locator.ApMgr.getSchedulerAPI(cfg.SchedulerURL, areaID, cfg.AccessToken)
		if err != nil {
			log.Warnf("getAccessPointsWithWeightCount, getSchedulerAPI error:%s", err.Error())
			continue
		}

		schedulerCfg := cfg
		onlineSchedulerCfgs[schedulerCfg.SchedulerURL] = schedulerCfg
		onlineSchedulerAPIs[schedulerCfg.SchedulerURL] = api
	}

	cfgWeights := countSchedulerWeightWithCfgs(onlineSchedulerCfgs)
	currentWeights := locator.countSchedulerWeightByNode(onlineSchedulerCfgs)

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

func (locator *Locator) countSchedulerWeightByNode(schedulerCfgs map[string]*schedulerCfg) map[string]float32 {
	totalWeight := 0

	weightMap := make(map[string]int)
	for _, cfg := range schedulerCfgs {
		count, err := locator.DB.countNodeOnScheduler(cfg.SchedulerURL)
		if err != nil {
			log.Errorf("countSchedulerWeightByNode, error:%s", err.Error())
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

// if node not exist return sql.ErrNoRows
func (locator *Locator) getSchedulerAPIWith(nodeID string) (*schedulerAPI, error) {
	node, err := locator.DB.getNodeInfo(nodeID)
	if err != nil {
		log.Errorf("GetAccessPoints, getNodeInfo error:%s", err.Error())
		return nil, err
	}

	cfg, err := locator.DB.getSchedulerCfg(node.SchedulerURL)
	if err != nil {
		return nil, err
	}

	return locator.ApMgr.getSchedulerAPI(node.SchedulerURL, node.AreaID, cfg.AccessToken)
}

func (locator *Locator) EdgeDownloadInfos(ctx context.Context, cid string) ([]*types.DownloadInfo, error) {
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
		return schedulerAPI.GetEdgeDownloadInfos(ctx, cid)
	}

	return make([]*types.DownloadInfo, 0), nil
}

func (locator *Locator) UserDownloadBlockResults(ctx context.Context, results []types.UserBlockDownloadResult) error {
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

	geoInfo, err := locator.GetGeoInfo(ip)
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
	schedulerCfgs, err := locator.DB.getAccessPointConfigs(areaID)
	if err != nil {
		log.Errorf("getCfg, access point %s not exist", areaID)
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

// AllocateNodes allocate nodes
func (locator *Locator) RegisterNewNode(ctx context.Context, schedulerURL, nodeID, publicKey string, nodeType types.NodeType) error {
	cfg, err := locator.DB.getSchedulerCfg(schedulerURL)
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("scheduler %s not exist", schedulerURL)
		}
		return err
	}

	schedulerAPI, err := locator.ApMgr.getSchedulerAPI(schedulerURL, cfg.AreaID, cfg.AccessToken)
	if err != nil {
		return err
	}

	err = schedulerAPI.RegisterNewNode(ctx, nodeID, publicKey, nodeType)
	if err != nil {
		return err
	}

	return locator.DB.setNodeInfo(nodeID, schedulerURL, cfg.AreaID, false)
}

func (locator *Locator) GetWebAccessPoints(ctx context.Context) ([]api.AccessPoint, error) {
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
func (locator *Locator) GetUserAccessPoint(ctx context.Context, userIP string) (api.AccessPoint, error) {
	areaID := defaultAreaID
	geoInfo, err := locator.GetGeoInfo(userIP)
	if err != nil {
		return api.AccessPoint{}, err
	}

	if geoInfo != nil && isValid(geoInfo.Geo) {
		areaID = geoInfo.Geo
	}

	schedulerCfgs, err := locator.DB.getAccessPointConfigs(areaID)
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
