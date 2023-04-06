package locator

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/filecoin-project/go-jsonrpc"
	"github.com/google/uuid"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/client"
)

type schedulerAPI struct {
	uuid uuid.UUID
	api.Scheduler
	close jsonrpc.ClientCloser
	url   string
}

type accessPoint struct {
	apis []*schedulerAPI
}

type AccessPointMgr struct {
	// key is areaID
	accessPoints sync.Map
	random       *rand.Rand
	uuid         string
	locatorToken string
}

func NewAccessPointMgr(locatorToken, uuid string) *AccessPointMgr {
	s := rand.NewSource(time.Now().UnixNano())
	mgr := &AccessPointMgr{random: rand.New(s), uuid: uuid, locatorToken: locatorToken}
	return mgr
}

func (mgr *AccessPointMgr) loadAccessPointFromMap(key string) *accessPoint {
	vb, ok := mgr.accessPoints.Load(key)
	if ok {
		return vb.(*accessPoint)
	}
	return nil
}

func (mgr *AccessPointMgr) deleteAccessPointFromMap(key string) {
	mgr.accessPoints.Delete(key)
}

func (mgr *AccessPointMgr) addAccessPointToMap(areaID string, ap *accessPoint) {
	mgr.accessPoints.Store(areaID, ap)
}

func (mgr *AccessPointMgr) newSchedulerAPI(url string, areaID string, schedulerAccessToken string) (*schedulerAPI, error) {
	ap := mgr.loadAccessPointFromMap(areaID)
	if ap == nil {
		ap = &accessPoint{apis: make([]*schedulerAPI, 0)}
	}

	for _, api := range ap.apis {
		if api.url == url {
			return nil, fmt.Errorf("newSchedulerAPI error, scheduler %s already exist", url)
		}
	}

	ctx, cancel := context.WithTimeout(context.TODO(), connectTimeout*time.Second)
	defer cancel()

	log.Infof("newSchedulerAPI, url:%s, areaID:%s, accessToken:%s", url, areaID, schedulerAccessToken)
	headers := http.Header{}
	headers.Add("Authorization", "Bearer "+schedulerAccessToken)
	api, close, err := client.NewScheduler(ctx, url, headers)
	if err != nil {
		log.Errorf("newSchedulerAPI err:%s,url:%s", err.Error(), url)
		return nil, err
	}

	err = api.ConnectLocator(ctx, mgr.uuid, mgr.locatorToken)
	if err != nil {
		log.Errorf("newSchedulerAPI connect to scheduler err:%s", err.Error())
		return nil, err
	}

	uuid, err := api.Session(ctx)
	if err != nil {
		log.Errorf("newSchedulerAPI Session err:%s", err.Error())
		return nil, err
	}

	log.Infof("newSchedulerAPI connect to scheduler %s, locator token:%s", url, mgr.locatorToken)

	newAPI := &schedulerAPI{uuid, api, close, url}
	ap.apis = append(ap.apis, newAPI)
	mgr.addAccessPointToMap(areaID, ap)

	return newAPI, nil
}

func (mgr *AccessPointMgr) removeSchedulerAPI(url, areaID string) {
	ap := mgr.loadAccessPointFromMap(areaID)
	if ap == nil {
		return
	}

	if len(ap.apis) == 0 {
		return
	}

	var removeAPI *schedulerAPI
	for i, api := range ap.apis {
		if api.url == url {
			removeAPI = api
			ap.apis = append(ap.apis[:i], ap.apis[i+1:]...)
			break
		}
	}

	if removeAPI != nil {
		removeAPI.close()
		mgr.addAccessPointToMap(areaID, ap)
	}
}

func (mgr *AccessPointMgr) removeAccessPoint(areaID string) {
	ap := mgr.loadAccessPointFromMap(areaID)
	if ap == nil {
		return
	}

	for _, api := range ap.apis {
		api.close()
	}

	mgr.deleteAccessPointFromMap(areaID)
}

func (mgr *AccessPointMgr) getSchedulerAPI(url, areaID, accessToken string) (*schedulerAPI, error) {
	ctx, cancel := context.WithTimeout(context.TODO(), connectTimeout*time.Second)
	defer cancel()

	ap := mgr.loadAccessPointFromMap(areaID)
	if ap != nil {
		for _, api := range ap.apis {
			if api.url == url {
				// check scheduler if online
				uuid, err := api.Session(ctx)
				if err != nil {
					log.Warnf("scheduler already %s offline", url)
					mgr.removeSchedulerAPI(url, areaID)
					return nil, err
				}

				if api.uuid != uuid {
					// remove offline scheduler api
					log.Warnf("scheduler %s have been restart, will renew again", url)
					mgr.removeSchedulerAPI(url, areaID)
					break
				}
				return api, nil
			}
		}
	}

	// reconnect scheduler
	api, err := mgr.newSchedulerAPI(url, areaID, accessToken)
	if err != nil {
		return nil, err
	}

	return api, nil
}

func (mgr *AccessPointMgr) randSchedulerAPI(areaID string) *schedulerAPI {
	ap := mgr.loadAccessPointFromMap(areaID)
	if ap == nil {
		return nil
	}

	if len(ap.apis) > 0 {
		index := mgr.random.Intn(len(ap.apis))
		return ap.apis[index]
	}

	return nil
}
