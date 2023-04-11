package locator

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/filecoin-project/go-jsonrpc"
	"github.com/google/uuid"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/client"
)

type scheduler struct {
	uuid uuid.UUID
	api.Scheduler
	close jsonrpc.ClientCloser
	url   string
}

type SchedulerMgr struct {
	// key is areaID
	schedulers sync.Map
}

func NewSchedulerMgr() *SchedulerMgr {
	sMgr := &SchedulerMgr{}
	return sMgr
}

func (sMgr *SchedulerMgr) loadSchedulersFromMap(areaID string) []*scheduler {
	vb, ok := sMgr.schedulers.Load(areaID)
	if ok {
		return vb.([]*scheduler)
	}
	return nil
}

func (sMgr *SchedulerMgr) removeSchedulersFromMap(areaID string) {
	sMgr.schedulers.Delete(areaID)
}

func (sMgr *SchedulerMgr) addSchedulersToMap(areaID string, schedulers []*scheduler) {
	sMgr.schedulers.Store(areaID, schedulers)
}

func (sMgr *SchedulerMgr) newScheduler(url string, areaID string, accessToken string) (*scheduler, error) {
	schedulers := sMgr.loadSchedulersFromMap(areaID)
	if schedulers == nil {
		schedulers = make([]*scheduler, 0)
	}

	for _, scheduler := range schedulers {
		if scheduler.url == url {
			return nil, fmt.Errorf("newScheduler error, scheduler %s already exist", url)
		}
	}

	ctx, cancel := context.WithTimeout(context.TODO(), connectTimeout*time.Second)
	defer cancel()

	log.Debugf("newSchedulerAPI, url:%s, areaID:%s, accessToken:%s", url, areaID, accessToken)

	headers := http.Header{}
	headers.Add("Authorization", "Bearer "+accessToken)
	api, close, err := client.NewScheduler(ctx, url, headers)
	if err != nil {
		log.Errorf("newSchedulerAPI err:%s,url:%s", err.Error(), url)
		return nil, err
	}

	uuid, err := api.Session(ctx)
	if err != nil {
		log.Errorf("newSchedulerAPI Session err:%s", err.Error())
		return nil, err
	}

	scheduler := &scheduler{uuid, api, close, url}
	schedulers = append(schedulers, scheduler)
	sMgr.addSchedulersToMap(areaID, schedulers)

	return scheduler, nil
}

func (sMgr *SchedulerMgr) removeScheduler(url, areaID string) {
	schedulers := sMgr.loadSchedulersFromMap(areaID)
	if len(schedulers) == 0 {
		return
	}

	var removeScheduler *scheduler
	for i, scheduler := range schedulers {
		if scheduler.url == url {
			removeScheduler = scheduler
			schedulers = append(schedulers[:i], schedulers[i+1:]...)
			break
		}
	}

	if removeScheduler != nil {
		removeScheduler.close()
		sMgr.addSchedulersToMap(areaID, schedulers)
	}
}

func (sMgr *SchedulerMgr) getSchedulers(areaID string) []*scheduler {
	schedulers := sMgr.loadSchedulersFromMap(areaID)
	if len(schedulers) == 0 {
		return nil
	}

	return schedulers
}
