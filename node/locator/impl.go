package locator

import (
	"context"
	"fmt"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/common"
	"github.com/linguohua/titan/node/handler"
	"github.com/linguohua/titan/node/repo"
	"github.com/linguohua/titan/region"

	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("locator")

const (
	miniWeight = 0
	maxWeight  = 1000
	// 3 seconds
	connectTimeout = 3
)

func NewLocalLocator(ctx context.Context, lr repo.LockedRepo, dbAddr, schedulerToken string, locatorPort int) api.Location {
	location := &Locator{apMgr: newAccessPointMgr(schedulerToken, locatorPort)}
	if len(dbAddr) > 0 {
		location.db = newDB(dbAddr)
		location.cfg = location.db
	} else {
		location.cfg = newLocalCfg(lr)
	}
	return location

}

type lconfig interface {
	addAccessPoints(areaID string, schedulerURL string, weight int) error
	removeAccessPoints(areaID string) error
	listAccessPoints() (areaIDs []string, err error)
	getAccessPoint(areaID string) (api.AccessPoint, error)
	isAccessPointExist(areaID, schedulerURL string) (bool, error)
}

type Locator struct {
	cfg lconfig
	*common.CommonAPI
	apMgr *accessPointMgr
	db    *db
	// connectMgr
}

func (locator *Locator) GetAccessPoints(ctx context.Context, deviceID string, securityKey string) ([]string, error) {
	// TODO: verify securityKey
	ip := handler.GetRequestIP(ctx)
	areaID := ""
	geoInfo, err := region.GetRegion().GetGeoInfo(ip)
	if err != nil {
		log.Errorf("GetAccessPoints get geo from ip error %s", err.Error())
	} else {
		areaID = geoInfo.Geo
	}

	if areaID == "" {
		return []string{}, fmt.Errorf("can not location device access point")
	}

	devices, err := locator.db.getDeviceInfos(deviceID)
	if err != nil {
		return []string{}, err
	}

	if len(devices) <= 0 {
		return locator.getAccessPointWithWeightCount(areaID)
	}

	device := devices[0]
	if device.AreaID != areaID {
		return locator.getAccessPointWithWeightCount(areaID)
	}

	if locator.apMgr.isSchedulerOnline(device.SchedulerURL, areaID) {
		return []string{device.SchedulerURL}, nil
	}
	return locator.getAccessPointWithWeightCount(areaID)
}

func (locator *Locator) AddAccessPoints(ctx context.Context, areaID string, schedulerURL string, weight int) error {
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

	_, err = locator.apMgr.newSchedulerAPI(schedulerURL, areaID)
	if err != nil {
		return err
	}

	return locator.cfg.addAccessPoints(areaID, schedulerURL, weight)
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
	infos := make([]api.SchedulerInfo, 0, len(ap.SchedulerInfos))
	for _, cfg := range ap.SchedulerInfos {
		if locator.apMgr.isSchedulerOnline(cfg.URL, areaID) {
			cfg.Online = true
		}

		infos = append(infos, cfg)
	}

	ap.SchedulerInfos = infos
	return ap, nil
}

func (locator *Locator) DeviceOnline(ctx context.Context, deviceID string, areaID string, port int) error {
	ip := handler.GetRequestIP(ctx)
	schedulerURL := fmt.Sprintf("http://%s:%d/rpc/v0", ip, port)
	locator.db.db.setDeviceInfo(deviceID, schedulerURL, areaID, true)
	return nil
}

func (locator *Locator) DeviceOffline(ctx context.Context, deviceID string) error {
	infos, err := locator.db.getDeviceInfos(deviceID)
	if err != nil {
		return err
	}

	if len(infos) <= 0 {
		return fmt.Errorf("Device %s not exist in location", deviceID)
	}

	info := infos[0]
	locator.db.db.setDeviceInfo(deviceID, info.SchedulerURL, info.AreaID, false)
	return nil
}

func (locator *Locator) getAccessPointWithWeightCount(areaID string) ([]string, error) {
	ap, err := locator.cfg.getAccessPoint(areaID)
	if err != nil {
		return []string{}, nil
	}

	if len(ap.SchedulerInfos) == 0 {
		return []string{}, fmt.Errorf("Area %s no config exist", areaID)
	}

	// filter online scheduler cfg
	infos := make([]api.SchedulerInfo, 0, len(ap.SchedulerInfos))
	for _, info := range ap.SchedulerInfos {
		if locator.apMgr.isSchedulerOnline(info.URL, areaID) {
			infos = append(infos, info)
		}
	}

	cfgWeights := countSchedulerWeightWithInfo(infos)
	currentWeights := locator.countSchedulerWeightByDevice(infos)

	result := make([]string, 0)
	for url, weight := range cfgWeights {
		currentWeight := currentWeights[url]
		if currentWeight < weight {
			result = append(result, url)
		}
	}

	return result, nil
}

func countSchedulerWeightWithInfo(schedulerCfgs []api.SchedulerInfo) map[string]float32 {
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

func (locator *Locator) countSchedulerWeightByDevice(schedulerCfgs []api.SchedulerInfo) map[string]float32 {
	totalWeight := 0

	weightMap := make(map[string]int)
	for _, cfg := range schedulerCfgs {
		count, err := locator.db.countDeviceOnScheduler(cfg.URL)
		if err != nil {
			continue
		}

		weightMap[cfg.URL] = count
		totalWeight += count
	}

	result := make(map[string]float32)
	for url, weight := range weightMap {
		result[url] = float32(weight) / float32(totalWeight)
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

	schedulerAPI, ok := locator.apMgr.randSchedulerAPI(areaID)
	if ok {
		return schedulerAPI.GetDownloadInfosWithBlocks(ctx, cids)
	}

	// TODO: new scheduler
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

	schedulerAPI, ok := locator.apMgr.randSchedulerAPI(areaID)
	if ok {
		return schedulerAPI.GetDownloadInfoWithBlocks(ctx, cids)
	}
	// TODO: new scheduler
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

	schedulerAPI, ok := locator.apMgr.randSchedulerAPI(areaID)
	if ok {
		return schedulerAPI.GetDownloadInfoWithBlock(ctx, cid)
	}
	// TODO: new scheduler
	return api.DownloadInfo{}, nil
}
