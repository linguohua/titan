package locator

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"go.uber.org/fx"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/types"
	"github.com/linguohua/titan/node/common"
	"github.com/linguohua/titan/region"

	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("locator")

const (
	// miniWeight = 0
	// maxWeight  = 1000
	// 3 seconds
	connectTimeout = 3
	defaultAreaID  = "CN-GD-Shenzhen"
	unknownAreaID  = "unknown-unknown-unknown"
)

type Locator struct {
	fx.In

	*common.CommonAPI
	sMgr *SchedulerMgr
	region.Region
	storage storage
}

func isValid(geo string) bool {
	return len(geo) > 0 && geo != unknownAreaID
}

func (l *Locator) GetAccessPoints(ctx context.Context, nodeID, areaID string) ([]string, error) {
	if len(nodeID) == 0 || len(areaID) == 0 {
		return nil, fmt.Errorf("params nodeID or areaID can not empty")
	}

	schedulers := l.sMgr.getSchedulers(areaID)
	ss, err := l.selectValidSchedulers(schedulers, nodeID)
	if err != nil {
		return nil, err
	}

	if len(ss) > 0 {
		return ss, nil
	}

	schedulerURLs, err := l.storage.GetSchedulerURLs(areaID)
	if err != nil {
		return nil, err
	}

	schedulers, err = l.newSchedulers(schedulerURLs)
	if err != nil {
		return nil, err
	}

	return l.selectValidSchedulers(schedulers, nodeID)
}

func (locator *Locator) selectValidSchedulers(ss []*scheduler, nodeID string) ([]string, error) {
	if len(ss) == 0 {
		return nil, nil
	}

	lock := &sync.Mutex{}
	schedulers := make([]string, 0)

	var wg sync.WaitGroup
	for _, s := range ss {
		wg.Add(1)

		go func(s *scheduler) {
			ctx, cancle := context.WithTimeout(context.Background(), connectTimeout*time.Second)
			defer cancle()

			if nodeInfo, err := s.GetNodeInfo(ctx, nodeID); err != nil {
				log.Errorf("get node info error:%s, nodeID:%s", err, nodeID)
			} else {
				if nodeInfo.NodeID == nodeID {

					lock.Lock()
					schedulers = append(schedulers, s.url)
					lock.Unlock()
				}
			}

			wg.Done()
		}(s)
	}

	wg.Wait()

	return schedulers, nil
}

func (locator *Locator) newSchedulers(urls []string) ([]*scheduler, error) {
	return nil, nil
}

func (locator *Locator) EdgeDownloadInfos(ctx context.Context, cid string) (*types.EdgeDownloadInfoList, error) {
	// remoteAddr := handler.GetRemoteAddr(ctx)
	// areaID, err := locator.getAreaIDWith(remoteAddr)
	// if err != nil {
	// 	return nil, err
	// }

	// schedulerAPI := locator.ApMgr.randSchedulerAPI(areaID)
	// if schedulerAPI == nil {
	// 	schedulerAPI, err = locator.getFirstOnlineSchedulerAPIAt(areaID)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// }

	// if schedulerAPI != nil {
	// 	return schedulerAPI.GetEdgeDownloadInfos(ctx, cid)
	// }

	return nil, nil
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

// load user access point for special user ip
func (locator *Locator) GetUserAccessPoint(ctx context.Context, userIP string) (api.AccessPoint, error) {
	// areaID := defaultAreaID
	// geoInfo, err := locator.GetGeoInfo(userIP)
	// if err != nil {
	// 	return api.AccessPoint{}, err
	// }

	// if geoInfo != nil && isValid(geoInfo.Geo) {
	// 	areaID = geoInfo.Geo
	// }

	// schedulerCfgs, err := locator.DB.getAccessPointConfigs(areaID)
	// if err != nil {
	// 	return api.AccessPoint{}, err
	// }

	// infos := make([]api.SchedulerInfo, 0, len(schedulerCfgs))
	// for _, cfg := range schedulerCfgs {
	// 	schedulerInfo := api.SchedulerInfo{URL: cfg.SchedulerURL, Weight: cfg.Weight}
	// 	infos = append(infos, schedulerInfo)
	// }

	// accessPoint := api.AccessPoint{AreaID: areaID, SchedulerInfos: infos}
	// return accessPoint, nil
	return api.AccessPoint{}, nil
}
