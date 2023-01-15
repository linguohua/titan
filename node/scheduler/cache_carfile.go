package scheduler

import (
	"context"
	"time"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/handler"
	"github.com/linguohua/titan/node/helper"
	"github.com/linguohua/titan/node/scheduler/carfile"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"golang.org/x/xerrors"
)

// CacheContinue Cache Continue
func (s *Scheduler) CacheContinue(ctx context.Context, cid, deviceID string) error {
	if cid == "" || deviceID == "" {
		return xerrors.New("parameter is nil")
	}

	return s.dataManager.CacheContinue(cid, deviceID)
}

// CacheResult Cache Data Result
func (s *Scheduler) CacheResult(ctx context.Context, info api.CacheResultInfo) error {
	deviceID := handler.GetDeviceID(ctx)

	if !deviceExists(deviceID, 0) {
		return xerrors.Errorf("node not Exist: %s", deviceID)
	}

	return s.dataManager.CacheCarfileResult(deviceID, &info)
}

// ResetCacheExpiredTime reset expired time with data cache
func (s *Scheduler) ResetCacheExpiredTime(ctx context.Context, carfileCid, deviceID string, expiredTime time.Time) error {
	if time.Now().After(expiredTime) {
		return xerrors.Errorf("now is after the expiredTime:%s", expiredTime.String())
	}

	return s.dataManager.ResetCacheExpiredTime(carfileCid, deviceID, expiredTime)
}

// ReplenishCacheExpiredTime replenish expired time with data cache
func (s *Scheduler) ReplenishCacheExpiredTime(ctx context.Context, carfileCid, deviceID string, hour int) error {
	if hour <= 0 {
		return xerrors.Errorf("hour is :%d", hour)
	}

	return s.dataManager.ReplenishCacheExpiredTime(carfileCid, deviceID, hour)
}

// StopCacheTask stop cache
func (s *Scheduler) StopCacheTask(ctx context.Context, carfileCid string) error {
	return s.dataManager.StopCacheTask(carfileCid, "")
}

// ShowRunningCarfileRecords Show Data Tasks
func (s *Scheduler) ShowRunningCarfileRecords(ctx context.Context) ([]api.CarfileRecordInfo, error) {
	infos := make([]api.CarfileRecordInfo, 0)

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

func carfileRecord2Info(d *carfile.CarfileRecord) api.CarfileRecordInfo {
	info := api.CarfileRecordInfo{}
	if d != nil {
		info.CarfileCid = d.GetCarfileCid()
		info.CarfileHash = d.GetCarfileHash()
		info.TotalSize = d.GetTotalSize()
		info.NeedReliability = d.GetNeedReliability()
		info.Reliability = d.GetReliability()
		info.TotalBlocks = d.GetTotalBlocks()

		caches := make([]api.CacheTaskInfo, 0)

		d.CacheTaskMap.Range(func(key, value interface{}) bool {
			c := value.(*carfile.CacheTask)

			cc := api.CacheTaskInfo{
				Status:     c.GetStatus(),
				DoneSize:   c.GetDoneSize(),
				DoneBlocks: c.GetDoneBlocks(),
				RootCache:  c.IsRootCache(),
				DeviceID:   c.GetDeviceID(),
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

		return cInfo, nil
	}

	return info, xerrors.Errorf("not found cid:%s", cid)
}

// // ListEvents get data events
// func (s *Scheduler) ListEvents(ctx context.Context, page int) (api.EventListInfo, error) {
// 	count, totalPage, list, err := persistent.GetDB().GetEventInfos(page)
// 	if err != nil {
// 		return api.EventListInfo{}, err
// 	}

// 	return api.EventListInfo{Page: page, TotalPage: totalPage, Count: count, EventList: list}, nil
// }

// ListCarfileRecords List Datas
func (s *Scheduler) ListCarfileRecords(ctx context.Context, page int) (api.DataListInfo, error) {
	count, totalPage, list, err := persistent.GetDB().GetCarfileCidWithPage(page)
	if err != nil {
		return api.DataListInfo{}, err
	}

	out := make([]*api.CarfileRecordInfo, 0)
	for _, info := range list {
		dInfo := &api.CarfileRecordInfo{
			CarfileCid:      info.CarfileCid,
			CarfileHash:     info.CarfileHash,
			NeedReliability: info.NeedReliability,
			Reliability:     info.Reliability,
			TotalSize:       info.TotalSize,
			TotalBlocks:     info.TotalBlocks,
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

	return s.dataManager.RemoveCarfileRecord(carfileID)
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
