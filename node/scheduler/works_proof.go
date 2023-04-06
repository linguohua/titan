package scheduler

import (
	"context"
	"crypto"

	"github.com/linguohua/titan/api/types"
	"github.com/linguohua/titan/node/cidutil"
	"github.com/linguohua/titan/node/handler"
	titanrsa "github.com/linguohua/titan/node/rsa"
	"golang.org/x/xerrors"
)

// UserDownloadResult result for user download
func (s *Scheduler) UserDownloadResult(ctx context.Context, result types.UserDownloadResult) error {
	nodeID := handler.GetNodeID(ctx)
	if result.Succeed {
		blockHash, err := cidutil.CIDString2HashString(result.BlockCID)
		if err != nil {
			return err
		}

		blockDownloadInfo := &types.DownloadRecordInfo{NodeID: nodeID, BlockCID: result.BlockCID, BlockSize: result.BlockSize}

		record, _ := s.NodeManager.FetchAssetRecord(blockHash)
		if record != nil && record.CID != "" {
			blockDownloadInfo.CarfileCID = result.BlockCID
		}
	}

	return nil
}

func (s *Scheduler) handleUserDownloadBlockResult(ctx context.Context, result types.UserBlockDownloadResult) error {
	// TODO: implement user download count
	return nil
}

// UserDownloadBlockResults node result for user download block
func (s *Scheduler) UserDownloadBlockResults(ctx context.Context, results []types.UserBlockDownloadResult) error {
	for _, result := range results {
		if err := s.handleUserDownloadBlockResult(ctx, result); err != nil {
			log.Errorf("handleUserDownloadBlockResult error: %s", err.Error())
		}
	}
	return nil
}

// EdgeDownloadInfos find node
func (s *Scheduler) EdgeDownloadInfos(ctx context.Context, cid string) ([]*types.DownloadInfo, error) {
	if cid == "" {
		return nil, xerrors.New("cids is nil")
	}

	hash, err := cidutil.CIDString2HashString(cid)
	if err != nil {
		return nil, xerrors.Errorf("%s cid to hash err:%s", cid, err.Error())
	}

	rows, err := s.NodeManager.FetchReplicasByHash(hash, []types.ReplicaStatus{types.ReplicaStatusSucceeded})
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	titanRsa := titanrsa.New(crypto.SHA256, crypto.SHA256.New())
	infos := make([]*types.DownloadInfo, 0)

	for rows.Next() {
		rInfo := &types.ReplicaInfo{}
		err = rows.StructScan(rInfo)
		if err != nil {
			log.Errorf("replica StructScan err: %s", err.Error())
			continue
		}

		if rInfo.IsCandidate {
			continue
		}

		nodeID := rInfo.NodeID
		eNode := s.NodeManager.GetEdgeNode(nodeID)
		if eNode == nil {
			continue
		}

		credentials, err := eNode.Credentials(cid, titanRsa, s.NodeManager.PrivateKey)
		if err != nil {
			continue
		}

		info := &types.DownloadInfo{
			URL:          eNode.DownloadAddr(),
			NodeID:       nodeID,
			Credentials:  credentials,
			NatType:      eNode.NatType,
			SchedulerURL: s.SchedulerCfg.RPCURL,
		}
		infos = append(infos, info)
	}

	return infos, nil
}
