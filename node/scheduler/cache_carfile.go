package scheduler

import (
	"context"
	"time"

	"github.com/linguohua/titan/api/types"
	"github.com/linguohua/titan/node/cidutil"
	"github.com/linguohua/titan/node/handler"
	"golang.org/x/xerrors"
)

// CacheResult nodeMgrCache Data Result
func (s *Scheduler) CacheResult(ctx context.Context, info types.CacheResult) error {
	nodeID := handler.GetNodeID(ctx)

	// if !s.nodeExists(nodeID, 0) {
	// 	return xerrors.Errorf("node not Exist: %s", nodeID)
	// }

	// update node info
	node := s.NodeManager.GetNode(nodeID)
	if node != nil {
		node.DiskUsage = info.DiskUsage
		node.Blocks = info.TotalBlocksCount
	}
	return s.DataManager.CacheCarfileResult(nodeID, &info)
}

// RemoveCarfileResult remove storage result
func (s *Scheduler) RemoveCarfileResult(ctx context.Context, resultInfo types.RemoveCarfileResult) error {
	nodeID := handler.GetNodeID(ctx)

	// if !s.nodeExists(nodeID, 0) {
	// 	return xerrors.Errorf("node not Exist: %s", nodeID)
	// }

	// update node info
	node := s.NodeManager.GetNode(nodeID)
	if node != nil {
		node.DiskUsage = resultInfo.DiskUsage
		node.Blocks = resultInfo.BlocksCount
	}

	return nil
}

// RecacheCarfiles Execute Undone Carfiles Task
func (s *Scheduler) RecacheCarfiles(ctx context.Context, hashs []string) error {
	list, err := s.NodeManager.CarfileDB.CarfileInfos(hashs)
	if err != nil {
		return err
	}

	if list != nil {
		for _, carfile := range list {
			info := &types.CacheCarfileInfo{
				CarfileCid: carfile.CarfileCID,
				Replicas:   carfile.NeedEdgeReplica,
				Expiration: carfile.Expiration,
			}
			err = s.CacheCarfiles(ctx, info)
			if err != nil {
				log.Errorf("RecacheCarfiles CacheCarfiles err:%s", err.Error())
			}
		}
	}

	return nil
}

// ResetCarfileExpiration reset expiration time with data cache
func (s *Scheduler) ResetCarfileExpiration(ctx context.Context, carfileCid string, t time.Time) error {
	if time.Now().After(t) {
		return xerrors.Errorf("expiration:%s has passed", t.String())
	}

	return s.DataManager.ResetCarfileExpiration(carfileCid, t)
}

// DownloadingCarfileRecords Show downloading carfiles
func (s *Scheduler) DownloadingCarfileRecords(ctx context.Context) ([]*types.CarfileRecordInfo, error) {
	return s.DataManager.GetDownloadingCarfileInfos(), nil
}

// CarfileRecord Show Data Task
func (s *Scheduler) CarfileRecord(ctx context.Context, cid string) (*types.CarfileRecordInfo, error) {
	info, err := s.DataManager.GetCarfileRecordInfo(cid)
	if err != nil {
		return nil, err
	}

	return info, nil
}

// ResetCandidateReplicaCount Reset candidate replica count
func (s *Scheduler) ResetCandidateReplicaCount(ctx context.Context, count int) error {
	s.DataManager.ResetReplicaCount(count)
	return nil
}

// CarfileRecords List Datas
func (s *Scheduler) CarfileRecords(ctx context.Context, page int) (*types.ListCarfileRecordRsp, error) {
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

// RemoveReplica remove a caches with carfile
func (s *Scheduler) RemoveReplica(ctx context.Context, carfileID, nodeID string) error {
	if carfileID == "" {
		return xerrors.Errorf("Cid Is Nil")
	}

	if nodeID == "" {
		return xerrors.Errorf("NodeID Is Nil")
	}

	return s.DataManager.RemoveCache(carfileID, nodeID)
}

// CacheCarfiles nodeMgrCache Carfile
func (s *Scheduler) CacheCarfiles(ctx context.Context, info *types.CacheCarfileInfo) error {
	if info.CarfileCid == "" {
		return xerrors.New("Cid is Nil")
	}

	hash, err := cidutil.CIDString2HashString(info.CarfileCid)
	if err != nil {
		return xerrors.Errorf("%s cid to hash err:%s", info.CarfileCid, err.Error())
	}

	info.CarfileHash = hash

	if info.Replicas < 1 {
		return xerrors.Errorf("replica is %d < 1", info.Replicas)
	}

	if time.Now().After(info.Expiration) {
		return xerrors.Errorf("now after expiration:%s", info.Expiration.String())
	}

	return s.DataManager.CacheCarfile(info)
}

// CarfileStatus return the carfile caches state
func (s *Scheduler) CarfileStatus(ctx context.Context, cid types.CarfileHash) (types.CarfileRecordInfo, error) {
	return s.DataManager.CarfileStatus(ctx, cid)
}
