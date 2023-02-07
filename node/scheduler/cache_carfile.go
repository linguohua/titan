package scheduler

import (
	"context"
	"time"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/handler"
	"github.com/linguohua/titan/node/helper"
	"github.com/linguohua/titan/node/scheduler/carfile"
	"github.com/linguohua/titan/node/scheduler/db/cache"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"golang.org/x/xerrors"
)

// CacheResult Cache Data Result
func (s *Scheduler) CacheResult(ctx context.Context, info api.CacheResultInfo) error {
	deviceID := handler.GetDeviceID(ctx)

	if !deviceExists(deviceID, 0) {
		return xerrors.Errorf("node not Exist: %s", deviceID)
	}

	//update node info
	node := s.nodeManager.GetNode(deviceID)
	if node != nil {
		node.DiskUsage = info.DiskUsage
	}
	//update redis
	err := cache.GetDB().UpdateNodeCacheInfo(deviceID, &cache.NodeCacheInfo{
		BlockCount: info.TotalBlockCount,
		DiskUsage:  info.DiskUsage,
	})
	if err != nil {
		log.Errorf("UpdateNodeCacheInfo err:%s", err.Error())
	}

	return s.dataManager.CacheCarfileResult(deviceID, &info)
}

// RemoveCarfileResult remove carfile result
func (s *Scheduler) RemoveCarfileResult(ctx context.Context, resultInfo api.RemoveCarfileResultInfo) error {
	deviceID := handler.GetDeviceID(ctx)

	if !deviceExists(deviceID, 0) {
		return xerrors.Errorf("node not Exist: %s", deviceID)
	}

	//update node info
	node := s.nodeManager.GetNode(deviceID)
	if node != nil {
		node.DiskUsage = resultInfo.DiskUsage
	}

	//update redis
	return cache.GetDB().UpdateNodeCacheInfo(deviceID, &cache.NodeCacheInfo{
		BlockCount: resultInfo.BlockCount,
		DiskUsage:  resultInfo.DiskUsage,
	})
}

//GetUndoneCarfileRecords get all undone carfile
func (s *Scheduler) GetUndoneCarfileRecords(ctx context.Context, page int) (*api.DataListInfo, error) {
	return persistent.GetDB().GetUndoneCarfiles(page)
}

//ExecuteUndoneCarfilesTask Execute Undone Carfiles Task
func (s *Scheduler) ExecuteUndoneCarfilesTask(ctx context.Context) error {
	info, err := persistent.GetDB().GetUndoneCarfiles(-1)
	if err != nil {
		return err
	}

	if info.CarfileRecords != nil {
		for _, carfile := range info.CarfileRecords {
			err = s.CacheCarfile(ctx, carfile.CarfileCid, carfile.NeedReliability, carfile.ExpiredTime)
			if err != nil {
				log.Errorf("ExecuteUndoneCarfilesTask CacheCarfile err:%s", err.Error())
			}
		}
	}

	return nil
}

// ResetCacheExpiredTime reset expired time with data cache
func (s *Scheduler) ResetCacheExpiredTime(ctx context.Context, carfileCid string, expiredTime time.Time) error {
	if time.Now().After(expiredTime) {
		return xerrors.Errorf("expiredTime:%s has passed", expiredTime.String())
	}

	return s.dataManager.ResetCacheExpiredTime(carfileCid, expiredTime)
}

// ReplenishCacheExpiredTime replenish expired time with data cache
func (s *Scheduler) ReplenishCacheExpiredTime(ctx context.Context, carfileCid string, hour int) error {
	if hour <= 0 {
		return xerrors.Errorf("hour is :%d <= 0", hour)
	}

	return s.dataManager.ReplenishCacheExpiredTime(carfileCid, hour)
}

// StopCacheTask stop cache
func (s *Scheduler) StopCacheTask(ctx context.Context, carfileCid string) error {
	return s.dataManager.StopCacheTask(carfileCid, "")
}

// GetRunningCarfileRecords Show Data Tasks
func (s *Scheduler) GetRunningCarfileRecords(ctx context.Context) ([]*api.CarfileRecordInfo, error) {
	infos := make([]*api.CarfileRecordInfo, 0)

	log.Debug("running count:", s.dataManager.RunningTaskCount)

	s.dataManager.CarfileRecordMap.Range(func(key, value interface{}) bool {
		if value != nil {
			data := value.(*carfile.CarfileRecord)
			if data != nil {
				cInfo := carfileRecord2Info(data)
				infos = append(infos, cInfo)
			}
		}

		return true
	})

	return infos, nil
}

func carfileRecord2Info(d *carfile.CarfileRecord) *api.CarfileRecordInfo {
	info := &api.CarfileRecordInfo{}
	if d != nil {
		info.CarfileCid = d.GetCarfileCid()
		info.CarfileHash = d.GetCarfileHash()
		info.TotalSize = d.GetTotalSize()
		info.NeedReliability = d.GetNeedReliability()
		info.Reliability = d.GetReliability()
		info.TotalBlocks = d.GetTotalBlocks()
		info.ExpiredTime = d.GetExpiredTime()

		caches := make([]api.CacheTaskInfo, 0)

		d.CacheTaskMap.Range(func(key, value interface{}) bool {
			c := value.(*carfile.CacheTask)

			cc := api.CacheTaskInfo{
				Status:         c.GetStatus(),
				DoneSize:       c.GetDoneSize(),
				DoneBlocks:     c.GetDoneBlocks(),
				CandidateCache: c.IsRootCache(),
				DeviceID:       c.GetDeviceID(),
			}

			caches = append(caches, cc)
			return true
		})

		info.CacheInfos = caches
	}

	return info
}

// GetCarfileRecord Show Data Task
func (s *Scheduler) GetCarfileRecord(ctx context.Context, cid string) (api.CarfileRecordInfo, error) {
	info := api.CarfileRecordInfo{}

	if cid == "" {
		return info, xerrors.Errorf("not found cid:%s", cid)
	}

	hash, err := helper.CIDString2HashString(cid)
	if err != nil {
		return info, err
	}

	d, _ := s.dataManager.GetCarfileRecord(hash)
	if d != nil {
		cInfo := carfileRecord2Info(d)

		return *cInfo, nil
	}

	return info, xerrors.Errorf("not found cid:%s", cid)
}

// ResetBackupCacheCount reset backupCacheCount
func (s *Scheduler) ResetBackupCacheCount(ctx context.Context, backupCacheCount int) error {
	s.dataManager.ResetBackupCacheCount(backupCacheCount)
	return nil
}

// ListCacheEvents get data events
func (s *Scheduler) ListCacheEvents(ctx context.Context, page int, cid string) (*api.EventListInfo, error) {
	return persistent.GetDB().ListCacheEventInfos(page, cid)
}

// ListCarfileRecords List Datas
func (s *Scheduler) ListCarfileRecords(ctx context.Context, page int) (*api.DataListInfo, error) {
	return persistent.GetDB().GetCarfileCidWithPage(page)
	// if err != nil {
	// 	return api.DataListInfo{}, err
	// }

	// out := make([]*api.CarfileRecordInfo, 0)
	// for _, info := range info.CarfileRecords {
	// 	dInfo := &api.CarfileRecordInfo{
	// 		CarfileCid:      info.CarfileCid,
	// 		CarfileHash:     info.CarfileHash,
	// 		NeedReliability: info.NeedReliability,
	// 		Reliability:     info.Reliability,
	// 		TotalSize:       info.TotalSize,
	// 		TotalBlocks:     info.TotalBlocks,
	// 	}

	// 	out = append(out, dInfo)
	// }

	// return api.DataListInfo{Page: page, TotalPage: totalPage, Cids: count, CacheInfos: out}, nil
}

// RemoveCarfile remove all caches with carfile
func (s *Scheduler) RemoveCarfile(ctx context.Context, carfileCid string) error {
	if carfileCid == "" {
		return xerrors.Errorf("Cid Is Nil")
	}

	hash, err := helper.CIDString2HashString(carfileCid)
	if err != nil {
		return err
	}

	return s.dataManager.RemoveCarfileRecord(carfileCid, hash)
}

// RemoveCache remove a caches with carfile
func (s *Scheduler) RemoveCache(ctx context.Context, carfileID, deviceID string) error {
	if carfileID == "" {
		return xerrors.Errorf("Cid Is Nil")
	}

	if deviceID == "" {
		return xerrors.Errorf("DeviceID Is Nil")
	}

	return s.dataManager.RemoveCache(carfileID, deviceID)
}

// CacheCarfile Cache Carfile
func (s *Scheduler) CacheCarfile(ctx context.Context, cid string, reliability int, expiredTime time.Time) error {
	if cid == "" {
		return xerrors.New("Cid is Nil")
	}

	if reliability < 1 {
		return xerrors.Errorf("reliability is %d < 1", reliability)
	}

	if time.Now().After(expiredTime) {
		return xerrors.Errorf("now after expiredTime:%s", expiredTime.String())
	}

	// expiredTime := time.Now().Add(time.Duration(hour) * time.Hour)

	return s.dataManager.CacheCarfile(cid, reliability, expiredTime)
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

	return nil, xerrors.Errorf("not found node:%s", deviceID)
}
