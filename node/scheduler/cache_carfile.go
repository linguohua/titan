package scheduler

import (
	"context"
	"time"

	"github.com/linguohua/titan/node/scheduler/storage"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/cidutil"
	"github.com/linguohua/titan/node/handler"
	"golang.org/x/xerrors"
)

// CacheResult nodeMgrCache Data Result
func (s *Scheduler) CacheResult(ctx context.Context, info api.CacheResult) error {
	nodeID := handler.GetNodeID(ctx)

	if !s.nodeExists(nodeID, 0) {
		return xerrors.Errorf("node not Exist: %s", nodeID)
	}

	// update node info
	node := s.NodeManager.GetNode(nodeID)
	if node != nil {
		node.DiskUsage = info.DiskUsage
		node.Blocks = info.TotalBlockCount
	}
	return s.DataManager.CacheCarfileResult(nodeID, &info)
}

// RemoveCarfileResult remove storage result
func (s *Scheduler) RemoveCarfileResult(ctx context.Context, resultInfo api.RemoveCarfileResult) error {
	nodeID := handler.GetNodeID(ctx)

	if !s.nodeExists(nodeID, 0) {
		return xerrors.Errorf("node not Exist: %s", nodeID)
	}

	// update node info
	node := s.NodeManager.GetNode(nodeID)
	if node != nil {
		node.DiskUsage = resultInfo.DiskUsage
		node.Blocks = resultInfo.BlockCount
	}

	return nil
}

// ExecuteUndoneCarfilesTask Execute Undone Carfiles Task
func (s *Scheduler) ExecuteUndoneCarfilesTask(ctx context.Context, hashs []string) error {
	list, err := s.NodeManager.CarfileDB.LoadCarfileInfos(hashs)
	if err != nil {
		return err
	}

	if list != nil {
		for _, carfile := range list {
			info := &api.CacheCarfileInfo{
				CarfileCid:     carfile.CarfileCid,
				Replicas:       carfile.Replica,
				ExpirationTime: carfile.Expiration,
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

	return s.DataManager.ResetCacheExpirationTime(carfileCid, t)
}

// GetDownloadingCarfileRecords Show downloading carfiles
func (s *Scheduler) GetDownloadingCarfileRecords(ctx context.Context) ([]*api.CarfileRecordInfo, error) {
	return s.DataManager.GetDownloadingCarfileInfos(), nil
}

// GetCarfileRecordInfo Show Data Task
func (s *Scheduler) GetCarfileRecordInfo(ctx context.Context, cid string) (api.CarfileRecordInfo, error) {
	info, err := s.DataManager.GetCarfileRecordInfo(cid)
	if err != nil {
		return api.CarfileRecordInfo{}, err
	}

	return *info, nil
}

// ResetReplicaCacheCount Reset candidate replica count
func (s *Scheduler) ResetReplicaCacheCount(ctx context.Context, count int) error {
	s.DataManager.ResetReplicaCount(count)
	return nil
}

// ListCarfileRecords List Datas
func (s *Scheduler) ListCarfileRecords(ctx context.Context, page int) (*api.CarfileRecordsInfo, error) {
	return s.NodeManager.CarfileDB.CarfileRecordInfos(page)
}

// RemoveCarfile remove all caches with storage
func (s *Scheduler) RemoveCarfile(ctx context.Context, carfileCid string) error {
	if carfileCid == "" {
		return xerrors.Errorf("Cid Is Nil")
	}

	hash, err := cidutil.CIDString2HashString(carfileCid)
	if err != nil {
		return err
	}

	return s.DataManager.RemoveCarfileRecord(carfileCid, hash)
}

// RemoveCache remove a caches with carfile
func (s *Scheduler) RemoveCache(ctx context.Context, carfileID, nodeID string) error {
	if carfileID == "" {
		return xerrors.Errorf("Cid Is Nil")
	}

	if nodeID == "" {
		return xerrors.Errorf("NodeID Is Nil")
	}

	return s.DataManager.RemoveCache(carfileID, nodeID)
}

// CacheCarfile nodeMgrCache Carfile
func (s *Scheduler) CacheCarfile(ctx context.Context, info *api.CacheCarfileInfo) error {
	if info.CarfileCid == "" {
		return xerrors.New("Cid is Nil")
	}

	hash, err := cidutil.CIDString2HashString(info.CarfileCid)
	if err != nil {
		return xerrors.Errorf("%s cid to hash err:%s", info.CarfileCid, err.Error())
	}

	info.CarfileHash = hash

	if info.NodeID == "" {
		if info.Replicas < 1 {
			return xerrors.Errorf("replica is %d < 1", info.Replicas)
		}

		if time.Now().After(info.ExpirationTime) {
			return xerrors.Errorf("now after expirationTime:%s", info.ExpirationTime.String())
		}
	}

	return s.DataManager.CacheCarfile(info)
}

// CarfilesStatus return the carfile caches state
func (s *Scheduler) CarfilesStatus(ctx context.Context, cid storage.CarfileID) (storage.CarfileInfo, error) {
	return s.CarfilesStatus(ctx, cid)
}
