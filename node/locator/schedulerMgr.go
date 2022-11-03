package locator

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/filecoin-project/go-jsonrpc"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/client"
)

type schedulerAPI struct {
	api.Scheduler
	close jsonrpc.ClientCloser
	url   string
}

type accessPoint struct {
	apis []*schedulerAPI
}

type accessPointMgr struct {
	// key is areaID
	// TODO change to async
	accessPoints sync.Map
	token        string
	locatorPort  int
	random       *rand.Rand
}

func newAccessPointMgr(token string, locatorPort int) *accessPointMgr {
	s := rand.NewSource(time.Now().UnixNano())
	mgr := &accessPointMgr{token: token, locatorPort: locatorPort, random: rand.New(s)}
	return mgr
}

func (mgr *accessPointMgr) loadAccessPointFromMap(key string) (*accessPoint, bool) {
	vb, ok := mgr.accessPoints.Load(key)
	if ok {
		return vb.(*accessPoint), ok
	}
	return nil, ok
}

func (mgr *accessPointMgr) deleteAccessPointFromMap(key string) {
	mgr.accessPoints.Delete(key)
}

func (mgr *accessPointMgr) addAccessPointToMap(areaID string, ap *accessPoint) {
	mgr.accessPoints.Store(areaID, ap)
}

func (mgr *accessPointMgr) newSchedulerAPI(url string, areaID string) (*schedulerAPI, error) {
	ap, ok := mgr.loadAccessPointFromMap(areaID)
	if !ok {
		ap = &accessPoint{apis: make([]*schedulerAPI, 0)}
	}

	for _, api := range ap.apis {
		if api.url == url {
			return nil, fmt.Errorf("newSchedulerAPI error, scheduler %s aready exist", url)
		}
	}

	ctx, cancel := context.WithTimeout(context.TODO(), connectTimeout*time.Second)
	defer cancel()

	headers := http.Header{}
	headers.Add("Authorization", "Bearer "+string(mgr.token))
	api, close, err := client.NewScheduler(ctx, url, headers)
	if err != nil {
		log.Errorf("newSchedulerAPI err:%s,url:%s", err.Error(), url)
		return nil, err
	}

	err = api.LocatorConnect(ctx, mgr.locatorPort, areaID)
	if err != nil {
		log.Errorf("newSchedulerAPI get version err:%s", err.Error())
		return nil, err
	}

	log.Infof("newSchedulerAPI connect to scheduler %s", url)

	newAPI := &schedulerAPI{api, close, url}
	ap.apis = append(ap.apis, newAPI)
	mgr.addAccessPointToMap(areaID, ap)

	return newAPI, nil

}

func (mgr *accessPointMgr) removeSchedulerAPI(url, areaID string) {
	ap, ok := mgr.loadAccessPointFromMap(areaID)
	if !ok {
		return
	}

	var index = 0
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
	ap, ok := mgr.loadAccessPointFromMap(areaID)
	if !ok {
		return
	}

	for _, api := range ap.apis {
		api.close()
	}

	mgr.deleteAccessPointFromMap(areaID)
}

func (mgr *accessPointMgr) getSchedulerAPI(url, areaID string) (*schedulerAPI, bool) {
	ap, ok := mgr.loadAccessPointFromMap(areaID)
	if !ok {
		return nil, false
	}

	for _, api := range ap.apis {
		if api.url == url {
			return api, true
		}
	}

	return nil, false

}

func (mgr *accessPointMgr) isSchedulerOnline(url, areaID string) bool {
	ctx, cancel := context.WithTimeout(context.TODO(), connectTimeout*time.Second)
	defer cancel()

	api, ok := mgr.getSchedulerAPI(url, areaID)
	if ok {
		_, err := api.Version(ctx)
		if err != nil {
			mgr.removeSchedulerAPI(url, areaID)
			return false
		}

		return true
	}

	// if offline, reconnect to scheduler
	_, err := mgr.newSchedulerAPI(url, areaID)
	if err != nil {
		return false
	}
	return true
}

func (mgr *accessPointMgr) randSchedulerAPI(areaID string) (*schedulerAPI, bool) {
	ap, ok := mgr.loadAccessPointFromMap(areaID)
	if !ok {
		return nil, false
	}

	if len(ap.apis) > 0 {
		index := mgr.random.Intn(len(ap.apis))
		return ap.apis[index], true
	}

	return nil, false

}
