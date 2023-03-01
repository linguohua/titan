package scheduler

import (
	"context"
	"time"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/cidutil"
	"github.com/linguohua/titan/node/handler"
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
		node.BlockCount = info.TotalBlockCount
	}
	// update redis
	err := persistent.UpdateNodeCacheInfo(deviceID, info.DiskUsage, info.TotalBlockCount)
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
		node.BlockCount = resultInfo.BlockCount
	}

	// update redis
	return persistent.UpdateNodeCacheInfo(deviceID, resultInfo.DiskUsage, resultInfo.BlockCount)
}

// ExecuteUndoneCarfilesTask Execute Undone Carfiles Task
func (s *Scheduler) ExecuteUndoneCarfilesTask(ctx context.Context, hashs []string) error {
	list, err := persistent.LoadCarfileInfos(hashs)
	if err != nil {
		return err
	}

	if list != nil {
		for _, carfile := range list {
			info := &api.CacheCarfileInfo{
				CarfileCid:     carfile.CarfileCid,
				Replica:        carfile.Replica,
				ExpirationTime: carfile.ExpirationTime,
			}
			err = s.CacheCarfile(ctx, info)
			if err != nil {
				log.Errorf("ExecuteUndoneCarfilesTask CacheCarfile err:%s", err.Error())
			}
		}
	}

	return nil
}

// ResetCacheExpirationTime reset expiration time with data cache
func (s *Scheduler) ResetCacheExpirationTime(ctx context.Context, carfileCid string, t time.Time) error {
	if time.Now().After(t) {
		return xerrors.Errorf("expirationTime:%s has passed", t.String())
	}

	return s.dataManager.ResetCacheExpirationTime(carfileCid, t)
}

// GetDownloadingCarfileRecords Show downloading carfiles
func (s *Scheduler) GetDownloadingCarfileRecords(ctx context.Context) ([]*api.CarfileRecordInfo, error) {
	return s.dataManager.GetDownloadingCarfileInfos(), nil
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
func (s *Scheduler) ListCarfileRecords(ctx context.Context, page int) (*api.CarfileRecordsInfo, error) {
	return persistent.CarfileRecordInfos(page)
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

		if time.Now().After(info.ExpirationTime) {
			return xerrors.Errorf("now after expirationTime:%s", info.ExpirationTime.String())
		}
	}

	return s.dataManager.CacheCarfile(info)
}
