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

type accessPointMgr struct {
	// key is areaID
	accessPoints sync.Map
	random       *rand.Rand
	uuid         string
	locatorToken string
}

func newAccessPointMgr(locatorToken, uuid string) *accessPointMgr {
	s := rand.NewSource(time.Now().UnixNano())
	mgr := &accessPointMgr{random: rand.New(s), uuid: uuid, locatorToken: locatorToken}
	return mgr
}

func (mgr *accessPointMgr) loadAccessPointFromMap(key string) (*accessPoint, bool) {
	vb, exist := mgr.accessPoints.Load(key)
	if exist {
		return vb.(*accessPoint), true
	}
	return nil, false
}

func (mgr *accessPointMgr) deleteAccessPointFromMap(key string) {
	mgr.accessPoints.Delete(key)
}

func (mgr *accessPointMgr) addAccessPointToMap(areaID string, ap *accessPoint) {
	mgr.accessPoints.Store(areaID, ap)
}

func (mgr *accessPointMgr) newSchedulerAPI(url string, areaID string, schedulerAccessToken string) (*schedulerAPI, error) {
	ap, exist := mgr.loadAccessPointFromMap(areaID)
	if !exist {
		ap = &accessPoint{apis: make([]*schedulerAPI, 0)}
	}

	for _, api := range ap.apis {
		if api.url == url {
			return nil, fmt.Errorf("newSchedulerAPI error, scheduler %s aready exist", url)
		}
	}

	ctx, cancel := context.WithTimeout(context.TODO(), connectTimeout*time.Second)
	defer cancel()

	log.Infof("newSchedulerAPI, url:%s, areaID:%s, accessToken:%s", url, areaID, schedulerAccessToken)
	headers := http.Header{}
	headers.Add("Authorization", "Bearer "+string(schedulerAccessToken))
	api, close, err := client.NewScheduler(ctx, url, headers)
	if err != nil {
		log.Errorf("newSchedulerAPI err:%s,url:%s", err.Error(), url)
		return nil, err
	}

	err = api.LocatorConnect(ctx, mgr.uuid, mgr.locatorToken)
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

func (mgr *accessPointMgr) removeSchedulerAPI(url, areaID string) {
	ap, exist := mgr.loadAccessPointFromMap(areaID)
	if !exist {
		return
	}

	if len(ap.apis) == 0 {
		return
	}

	index := 0
	for i, api := range ap.apis {
		if api.url == url {
			index = i
			break
		}
	}

	api := ap.apis[index]
	ap.apis = append(ap.apis[0:index], ap.apis[index+1:]...)
	api.close()

	mgr.addAccessPointToMap(areaID, ap)
}

func (mgr *accessPointMgr) removeAccessPoint(areaID string) {
	ap, exist := mgr.loadAccessPointFromMap(areaID)
	if !exist {
		return
	}

	for _, api := range ap.apis {
		api.close()
	}

	mgr.deleteAccessPointFromMap(areaID)
}

func (mgr *accessPointMgr) getSchedulerAPI(url, areaID, accessToken string) (*schedulerAPI, bool) {
	ctx, cancel := context.WithTimeout(context.TODO(), connectTimeout*time.Second)
	defer cancel()

	ap, exist := mgr.loadAccessPointFromMap(areaID)
	if exist {
		for _, api := range ap.apis {
			if api.url == url {
				// check scheduler if online
				uuid, err := api.Session(ctx)
				if err != nil {
					log.Warnf("scheduler aready %s offline", url)
					mgr.removeSchedulerAPI(url, areaID)
					return nil, false
				}

				if api.uuid != uuid {
					// remove offline scheduler api
					log.Warnf("scheduler %s have been restart, will renew again", url)
					mgr.removeSchedulerAPI(url, areaID)
					break
				}
				return api, true
			}
		}
	}

	// reconnect scheduler
	api, err := mgr.newSchedulerAPI(url, areaID, accessToken)
	if err == nil {
		return api, true
	}

	return nil, false
}

func (mgr *accessPointMgr) randSchedulerAPI(areaID string) (*schedulerAPI, bool) {
	ap, exist := mgr.loadAccessPointFromMap(areaID)
	if !exist {
		return nil, false
	}

	if len(ap.apis) > 0 {
		index := mgr.random.Intn(len(ap.apis))
		return ap.apis[index], true
	}

	return nil, false
}
