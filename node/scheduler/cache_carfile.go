package scheduler

import (
	"context"
	"time"

	"github.com/linguohua/titan/api/types"
	"github.com/linguohua/titan/node/cidutil"
	"github.com/linguohua/titan/node/handler"
	"golang.org/x/xerrors"
)

// RemoveAssetResult remove asset result
func (s *Scheduler) RemoveAssetResult(ctx context.Context, resultInfo types.RemoveAssetResult) error {
	nodeID := handler.GetNodeID(ctx)

	// update node info
	node := s.NodeManager.GetNode(nodeID)
	if node != nil {
		node.DiskUsage = resultInfo.DiskUsage
		node.Blocks = resultInfo.BlocksCount
	}

	return nil
}

// RestartFailedAssets restart failed assets
func (s *Scheduler) RestartFailedAssets(ctx context.Context, hashes []types.AssetHash) error {
	return s.DataManager.RestartCacheAssets(hashes)
}

// ResetAssetExpiration reset expiration time with data cache
func (s *Scheduler) ResetAssetExpiration(ctx context.Context, cid string, t time.Time) error {
	if time.Now().After(t) {
		return xerrors.Errorf("expiration:%s has passed", t.String())
	}

	return s.DataManager.ResetAssetRecordExpiration(cid, t)
}

// AssetRecord Show Data Task
func (s *Scheduler) AssetRecord(ctx context.Context, cid string) (*types.AssetRecord, error) {
	info, err := s.DataManager.GetAssetRecordInfo(cid)
	if err != nil {
		return nil, err
	}

	return info, nil
}

// AssetRecords List assets
func (s *Scheduler) AssetRecords(ctx context.Context, limit, offset int, states []string) ([]*types.AssetRecord, error) {
	rows, err := s.NodeManager.LoadAssetRecords(states, limit, offset, s.ServerID)
	if err != nil {
		return nil, err
	}

	list := make([]*types.AssetRecord, 0)

	// loading assets to local
	for rows.Next() {
		cInfo := &types.AssetRecord{}
		err = rows.StructScan(cInfo)
		if err != nil {
			log.Errorf("asset StructScan err: %s", err.Error())
			continue
		}

		cInfo.ReplicaInfos, err = s.NodeManager.LoadAssetReplicaInfos(cInfo.Hash)
		if err != nil {
			log.Errorf("asset %s load replicas err: %s", cInfo.CID, err.Error())
			continue
		}

		list = append(list, cInfo)
	}

	return list, nil
}

// RemoveAsset remove all caches with asset cid
func (s *Scheduler) RemoveAsset(ctx context.Context, cid string) error {
	if cid == "" {
		return xerrors.Errorf("Cid Is Nil")
	}

	hash, err := cidutil.CIDString2HashString(cid)
	if err != nil {
		return err
	}

	return s.DataManager.RemoveAsset(cid, hash)
}

// CacheAsset cache asset
func (s *Scheduler) CacheAsset(ctx context.Context, info *types.CacheAssetReq) error {
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
