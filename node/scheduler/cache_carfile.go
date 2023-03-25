package scheduler

import (
	"context"
	"time"

	"github.com/linguohua/titan/api/types"
	"github.com/linguohua/titan/node/cidutil"
	"github.com/linguohua/titan/node/handler"
	"golang.org/x/xerrors"
)

// RemoveCarfileResult remove carfile result
func (s *Scheduler) RemoveCarfileResult(ctx context.Context, resultInfo types.RemoveCarfileResult) error {
	nodeID := handler.GetNodeID(ctx)

	// update node info
	node := s.NodeManager.GetNode(nodeID)
	if node != nil {
		node.DiskUsage = resultInfo.DiskUsage
		node.Blocks = resultInfo.BlocksCount
	}

	return nil
}

// RestartFailedCarfiles restart failed carfiles
func (s *Scheduler) RestartFailedCarfiles(ctx context.Context, hashes []types.AssetHash) error {
	return s.DataManager.RestartCacheAssets(hashes)
}

// ResetCarfileExpiration reset expiration time with data cache
func (s *Scheduler) ResetCarfileExpiration(ctx context.Context, carfileCid string, t time.Time) error {
	if time.Now().After(t) {
		return xerrors.Errorf("expiration:%s has passed", t.String())
	}

	return s.DataManager.ResetAssetRecordExpiration(carfileCid, t)
}

// CarfileRecord Show Data Task
func (s *Scheduler) CarfileRecord(ctx context.Context, cid string) (*types.AssetRecord, error) {
	info, err := s.DataManager.GetAssetRecordInfo(cid)
	if err != nil {
		return nil, err
	}

	return info, nil
}

// CarfileRecords List carfiles
func (s *Scheduler) CarfileRecords(ctx context.Context, limit, offset int, states []string) ([]*types.AssetRecord, error) {
	rows, err := s.NodeManager.LoadAssetRecords(states, limit, offset, s.ServerID)
	if err != nil {
		return nil, err
	}

	list := make([]*types.AssetRecord, 0)

	// loading carfiles to local
	for rows.Next() {
		cInfo := &types.AssetRecord{}
		err = rows.StructScan(cInfo)
		if err != nil {
			log.Errorf("carfile StructScan err: %s", err.Error())
			continue
		}

		cInfo.ReplicaInfos, err = s.NodeManager.LoadAssetReplicaInfos(cInfo.Hash)
		if err != nil {
			log.Errorf("carfile %s load replicas err: %s", cInfo.CID, err.Error())
			continue
		}

		list = append(list, cInfo)
	}

	return list, nil
}

// RemoveCarfile remove all caches with carfile cid
func (s *Scheduler) RemoveCarfile(ctx context.Context, cid string) error {
	if cid == "" {
		return xerrors.Errorf("Cid Is Nil")
	}

	hash, err := cidutil.CIDString2HashString(cid)
	if err != nil {
		return err
	}

	return s.DataManager.RemoveAsset(cid, hash)
}

// CacheCarfiles nodeMgrCache Carfile
func (s *Scheduler) CacheCarfiles(ctx context.Context, info *types.CacheAssetReq) error {
	if info.CID == "" {
		return xerrors.New("Cid is Nil")
	}

	hash, err := cidutil.CIDString2HashString(info.CID)
	if err != nil {
		return xerrors.Errorf("%s cid to hash err:%s", info.CID, err.Error())
	}

	info.Hash = hash

	if info.Replicas < 1 {
		return xerrors.Errorf("replicas %d must greater than 1", info.Replicas)
	}

	if time.Now().After(info.Expiration) {
		return xerrors.Errorf("expiration %s less than now(%v)", info.Expiration.String(), time.Now())
	}

	return s.DataManager.CacheAsset(info)
}
