package scheduler

import (
	"context"
	"time"

	"github.com/linguohua/titan/api/types"
	"github.com/linguohua/titan/node/cidutil"
	"github.com/linguohua/titan/node/handler"
	"golang.org/x/xerrors"
)

// RemoveAssetResult updates a node's disk usage and block count based on the resultInfo.
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

// RestartFailedAssets restarts of pulling assets for a given list of  asset hashes.
func (s *Scheduler) RestartFailedAssets(ctx context.Context, hashes []types.AssetHash) error {
	return s.AssetManager.RestartPullAssets(hashes)
}

// ResetAssetExpiration resets the expiration time of an asset record based on the provided CID and new expiration time.
func (s *Scheduler) ResetAssetExpiration(ctx context.Context, cid string, t time.Time) error {
	if time.Now().After(t) {
		return xerrors.Errorf("expiration:%s has passed", t.String())
	}

	return s.AssetManager.ResetAssetRecordExpiration(cid, t)
}

// AssetRecord retrieves an asset record by its CID.
func (s *Scheduler) AssetRecord(ctx context.Context, cid string) (*types.AssetRecord, error) {
	info, err := s.AssetManager.GetAssetRecordInfo(cid)
	if err != nil {
		return nil, err
	}

	return info, nil
}

// AssetRecords lists asset records with optional filtering by status, limit, and offset.
func (s *Scheduler) AssetRecords(ctx context.Context, limit, offset int, statuses []string) ([]*types.AssetRecord, error) {
	rows, err := s.NodeManager.FetchAssetRecords(statuses, limit, offset, s.ServerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	list := make([]*types.AssetRecord, 0)

	// loading assets to local
	for rows.Next() {
		cInfo := &types.AssetRecord{}
		err = rows.StructScan(cInfo)
		if err != nil {
			log.Errorf("asset StructScan err: %s", err.Error())
			continue
		}

		cInfo.ReplicaInfos, err = s.NodeManager.FetchAssetReplicas(cInfo.Hash)
		if err != nil {
			log.Errorf("asset %s load replicas err: %s", cInfo.CID, err.Error())
			continue
		}

		list = append(list, cInfo)
	}

	return list, nil
}

// RemoveAsset removes an asset record from the system by its CID.
func (s *Scheduler) RemoveAsset(ctx context.Context, cid string) error {
	if cid == "" {
		return xerrors.Errorf("Cid Is Nil")
	}

	hash, err := cidutil.CIDString2HashString(cid)
	if err != nil {
		return err
	}

	return s.AssetManager.RemoveAsset(cid, hash)
}

// CacheAsset caches an asset based on the provided PullAssetReq structure.
func (s *Scheduler) CacheAsset(ctx context.Context, info *types.PullAssetReq) error {
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

	return s.AssetManager.PullAssets(info)
}

// AssetReplicaList lists asset replicas based on a given request with startTime, endTime, cursor, and count parameters.
func (s *Scheduler) AssetReplicaList(ctx context.Context, req types.ListReplicaInfosReq) (*types.ListReplicaInfosRsp, error) {
	startTime := time.Unix(req.StartTime, 0)
	endTime := time.Unix(req.EndTime, 0)

	info, err := s.NodeManager.FetchReplicas(startTime, endTime, req.Cursor, req.Count)
	if err != nil {
		return nil, err
	}

	return info, nil
}
