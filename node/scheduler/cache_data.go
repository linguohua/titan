package scheduler

import (
	"context"
	"time"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/handler"
	"github.com/linguohua/titan/node/helper"
	"github.com/linguohua/titan/node/scheduler/data"
	"github.com/linguohua/titan/node/scheduler/db/cache"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"golang.org/x/xerrors"
)

// CacheContinue Cache Continue
func (s *Scheduler) CacheContinue(ctx context.Context, cid, cacheID string) error {
	if cid == "" || cacheID == "" {
		return xerrors.New("parameter is nil")
	}

	return s.dataManager.CacheContinue(cid, cacheID)
}

// CacheResult Cache Data Result
func (s *Scheduler) CacheResult(ctx context.Context, deviceID string, info api.CacheResultInfo) (string, error) {
	deviceID = handler.GetDeviceID(ctx)

	if !isDeviceExist(deviceID, 0) {
		return "", xerrors.Errorf("node not Exist: %s", deviceID)
	}

	info.DeviceID = deviceID

	// log.Warnf("CacheResult ,CacheID:%s Cid:%s", info.CacheID, info.Cid)
	// err := s.dataManager.PushCacheResultToQueue(&info)

	err := s.dataManager.CacheCarfileResult(&info)
	if err != nil {
		log.Errorf("doResultTask cacheCarfileResult err:%s", err.Error())
		// return
	}

	return "", err
}

// ResetCacheExpiredTime reset expired time with data cache
func (s *Scheduler) ResetCacheExpiredTime(ctx context.Context, carfileCid, cacheID string, expiredTime time.Time) error {
	if time.Now().After(expiredTime) {
		return xerrors.Errorf("now is after the expiredTime:%s", expiredTime.String())
	}

	return s.dataManager.ResetExpiredTime(carfileCid, cacheID, expiredTime)
}

// ReplenishCacheExpiredTime replenish expired time with data cache
func (s *Scheduler) ReplenishCacheExpiredTime(ctx context.Context, carfileCid, cacheID string, hour int) error {
	if hour <= 0 {
		return xerrors.Errorf("hour is :%d", hour)
	}

	return s.dataManager.ReplenishExpiredTimeToData(carfileCid, cacheID, hour)
}

// StopCacheTask stop cache
func (s *Scheduler) StopCacheTask(ctx context.Context, carfileCid string) error {
	return s.dataManager.StopCacheTask(carfileCid)
}

// ShowDataTasks Show Data Tasks
func (s *Scheduler) ShowDataTasks(ctx context.Context) ([]api.DataInfo, error) {
	infos := make([]api.DataInfo, 0)

	list := s.dataManager.GetRunningTasks()

	for _, info := range list {
		data := s.dataManager.GetData(info.CarfileHash)
		if data != nil {
			cInfo := dataToCacheDataInfo(data)

			t, err := cache.GetDB().GetRunningDataTaskExpiredTime(info.CarfileHash)
			if err == nil {
				cInfo.DataTimeout = t
			}

			infos = append(infos, cInfo)
		}
	}

	// s.dataManager.taskMap.Range(func(key, value interface{}) bool {
	// 	data := value.(*Data)

	// 	infos = append(infos, dataToCacheDataInfo(data))

	// 	return true
	// })

	// log.Infof("ShowDataTasks:%v", infos)
	return infos, nil
}

func dataToCacheDataInfo(d *data.Data) api.DataInfo {
	info := api.DataInfo{}
	if d != nil {
		info.CarfileCid = d.GetCarfileCid()
		info.CarfileHash = d.GetCarfileHash()
		info.TotalSize = d.GetTotalSize()
		info.NeedReliability = d.GetNeedReliability()
		info.Reliability = d.GetReliability()
		info.TotalBlocks = d.GetTotalBlocks()
		info.Nodes = d.GetTotalNodes()

		caches := make([]api.CacheInfo, 0)

		cMap := d.GetCacheMap()

		cMap.Range(func(key, value interface{}) bool {
			c := value.(*data.Cache)

			cache := api.CacheInfo{
				CacheID:    c.GetCacheID(),
				Status:     c.GetStatus(),
				DoneSize:   c.GetDoneSize(),
				DoneBlocks: c.GetDoneBlocks(),
				Nodes:      c.GetNodes(),
			}

			caches = append(caches, cache)
			return true
		})

		info.CacheInfos = caches
	}

	return info
}

// GetCacheData Show Data Task
func (s *Scheduler) GetCacheData(ctx context.Context, cid string) (api.DataInfo, error) {
	info := api.DataInfo{}

	if cid == "" {
		return info, xerrors.Errorf("Not Found Cid:%s", cid)
	}

	hash, err := helper.CIDString2HashString(cid)
	if err != nil {
		return info, err
	}

	d := s.dataManager.GetData(hash)
	if d != nil {
		cInfo := dataToCacheDataInfo(d)
		t, err := cache.GetDB().GetRunningDataTaskExpiredTime(hash)
		if err == nil {
			cInfo.DataTimeout = t
		}

		return cInfo, nil
	}

	return info, xerrors.Errorf("Not Found Cid:%s", cid)
}

// ListEvents get data events
func (s *Scheduler) ListEvents(ctx context.Context, page int) (api.EventListInfo, error) {
	count, totalPage, list, err := persistent.GetDB().GetEventInfos(page)
	if err != nil {
		return api.EventListInfo{}, err
	}

	return api.EventListInfo{Page: page, TotalPage: totalPage, Count: count, EventList: list}, nil
}

// ListCacheDatas List Datas
func (s *Scheduler) ListCacheDatas(ctx context.Context, page int) (api.DataListInfo, error) {
	count, totalPage, list, err := persistent.GetDB().GetDataCidWithPage(page)
	if err != nil {
		return api.DataListInfo{}, err
	}

	out := make([]*api.DataInfo, 0)
	for _, info := range list {
		dInfo := &api.DataInfo{
			CarfileCid:      info.CarfileCid,
			CarfileHash:     info.CarfileHash,
			NeedReliability: info.NeedReliability,
			Reliability:     info.Reliability,
			TotalSize:       info.TotalSize,
			TotalBlocks:     info.TotalBlocks,
			Nodes:           info.Nodes,
		}

		out = append(out, dInfo)
	}

	return api.DataListInfo{Page: page, TotalPage: totalPage, Cids: count, CacheInfos: out}, nil
}

// RemoveCarfile remove all caches with carfile
func (s *Scheduler) RemoveCarfile(ctx context.Context, carfileID string) error {
	if carfileID == "" {
		return xerrors.Errorf("Cid Is Nil")
	}

	return s.dataManager.RemoveCarfile(carfileID)
}

// RemoveCache remove a caches with carfile
func (s *Scheduler) RemoveCache(ctx context.Context, carfileID, cacheID string) error {
	if carfileID == "" {
		return xerrors.Errorf("Cid Is Nil")
	}

	if cacheID == "" {
		return xerrors.Errorf("CacheID Is Nil")
	}

	return s.dataManager.RemoveCache(carfileID, cacheID)
}

// CacheCarfile Cache Carfile
func (s *Scheduler) CacheCarfile(ctx context.Context, cid string, reliability int, hour int) error {
	if cid == "" {
		return xerrors.New("Cid is Nil")
	}

	expiredTime := time.Now().Add(time.Duration(hour) * time.Hour)

	return s.dataManager.CacheData(cid, reliability, expiredTime)
}

// DeleteBlockRecords  Delete Block Record
func (s *Scheduler) DeleteBlockRecords(ctx context.Context, deviceID string, cids []string) (map[string]string, error) {
	if len(cids) <= 0 {
		return nil, xerrors.New("Cid is Nil")
	}

	// edge := s.nodeManager.getEdgeNode(deviceID)
	// if edge != nil {
	// 	return edge.deleteBlockRecords(cids)
	// }

	// candidate := s.nodeManager.getCandidateNode(deviceID)
	// if candidate != nil {
	// 	return candidate.deleteBlockRecords(cids)
	// }

	return nil, xerrors.Errorf("Not Found Node:%s", deviceID)
}

func (s *Scheduler) deviceBlockCacheCount(deviceID string, blockSize int) error {
	// save block count to redis
	err := cache.GetDB().IncrByDeviceInfo(deviceID, "BlockCount", 1)
	if err != nil {
		log.Errorf("IncrByDeviceInfo err:%s ", err.Error())
		return err
	}

	err = cache.GetDB().IncrByDeviceInfo(deviceID, "TotalDownload", int64(blockSize))
	if err != nil {
		log.Errorf("IncrByDeviceInfo err:%s ", err.Error())
		return err
	}

	return nil
}
