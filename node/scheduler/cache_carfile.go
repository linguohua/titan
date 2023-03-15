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

// RestartFailedCarfiles restart failed carfiles
func (s *Scheduler) RestartFailedCarfiles(ctx context.Context, hashes []types.CarfileHash) error {
	return s.DataManager.FailedCarfilesRestart(hashes)
}

// ResetCarfileExpiration reset expiration time with data cache
func (s *Scheduler) ResetCarfileExpiration(ctx context.Context, carfileCid string, t time.Time) error {
	if time.Now().After(t) {
		return xerrors.Errorf("expiration:%s has passed", t.String())
	}

	return s.DataManager.ResetCarfileExpiration(carfileCid, t)
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
func (s *Scheduler) CarfileRecords(ctx context.Context, page int, status types.CacheStatus) (*types.ListCarfileRecordRsp, error) {
	info, err := s.NodeManager.CarfileDB.CarfileRecordInfos(page, status)
	if err != nil {
		return nil, err
	}

	for _, c := range info.CarfileRecords {
		rs, err := s.NodeManager.CarfileDB.ReplicaInfosByCarfile(c.CarfileHash, false)
		if err != nil {
			continue
		}

		c.ReplicaInfos = rs
	}

	return info, nil
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
