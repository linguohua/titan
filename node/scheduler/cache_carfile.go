package scheduler

import (
	"context"
	"time"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/cidutil"
	"github.com/linguohua/titan/node/handler"
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

	// update node info
	node := s.nodeManager.GetNode(deviceID)
	if node != nil {
		node.DiskUsage = info.DiskUsage
	}
	// update redis
	err := cache.UpdateNodeCacheInfo(deviceID, &cache.NodeCacheInfo{
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

	// update node info
	node := s.nodeManager.GetNode(deviceID)
	if node != nil {
		node.DiskUsage = resultInfo.DiskUsage
	}

	// update redis
	return cache.UpdateNodeCacheInfo(deviceID, &cache.NodeCacheInfo{
		BlockCount: resultInfo.BlockCount,
		DiskUsage:  resultInfo.DiskUsage,
	})
}

// ExecuteUndoneCarfilesTask Execute Undone Carfiles Task
func (s *Scheduler) ExecuteUndoneCarfilesTask(ctx context.Context, hashs []string) error {
	list, err := persistent.GetCarfileInfos(hashs)
	if err != nil {
		return err
	}

	if list != nil {
		for _, carfile := range list {
			info := &api.CacheCarfileInfo{
				CarfileCid:  carfile.CarfileCid,
				Replica:     carfile.Replica,
				ExpiredTime: carfile.ExpiredTime,
			}
			err = s.CacheCarfile(ctx, info)
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

// StopCacheTask stop cache
func (s *Scheduler) StopCacheTask(ctx context.Context, carfileCid string) error {
	return s.dataManager.StopCacheTask(carfileCid, "")
}

// GetRunningCarfileRecords Show Data Tasks
func (s *Scheduler) GetRunningCarfileRecords(ctx context.Context) ([]*api.CarfileRecordInfo, error) {
	return s.dataManager.GetRunningCarfileInfos(), nil
}

// GetCarfileRecordInfo Show Data Task
func (s *Scheduler) GetCarfileRecordInfo(ctx context.Context, cid string) (api.CarfileRecordInfo, error) {
	info, err := s.dataManager.GetCarfileRecordInfo(cid)
	if err != nil {
		return api.CarfileRecordInfo{}, err
	}

	return *info, nil
}

// ResetReplicaCacheCount Reset candidate replica count
func (s *Scheduler) ResetReplicaCacheCount(ctx context.Context, count int) error {
	s.dataManager.ResetReplicaCount(count)
	return nil
}

// ListCarfileRecords List Datas
func (s *Scheduler) ListCarfileRecords(ctx context.Context, page int) (*api.DataListInfo, error) {
	return persistent.GetCarfileCidWithPage(page)
}

// RemoveCarfile remove all caches with carfile
func (s *Scheduler) RemoveCarfile(ctx context.Context, carfileCid string) error {
	if carfileCid == "" {
		return xerrors.Errorf("Cid Is Nil")
	}

	hash, err := cidutil.CIDString2HashString(carfileCid)
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
func (s *Scheduler) CacheCarfile(ctx context.Context, info *api.CacheCarfileInfo) error {
	if info.CarfileCid == "" {
		return xerrors.New("Cid is Nil")
	}

	hash, err := cidutil.CIDString2HashString(info.CarfileCid)
	if err != nil {
		return xerrors.Errorf("%s cid to hash err:%s", info.CarfileCid, err.Error())
	}

	info.CarfileHash = hash

	if info.DeviceID == "" {
		if info.Replica < 1 {
			return xerrors.Errorf("replica is %d < 1", info.Replica)
		}

		if time.Now().After(info.ExpiredTime) {
			return xerrors.Errorf("now after expiredTime:%s", info.ExpiredTime.String())
		}
	}

	return s.dataManager.CacheCarfile(info)
}
