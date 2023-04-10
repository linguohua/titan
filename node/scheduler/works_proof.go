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

// UserDownloadResult handles the result of a user download
func (s *Scheduler) UserDownloadResult(ctx context.Context, result types.UserDownloadResult) error {
	nodeID := handler.GetNodeID(ctx)
	if result.Succeed {
		blockHash, err := cidutil.CIDString2HashString(result.BlockCID)
		if err != nil {
			return err
		}

		blockDownloadInfo := &types.DownloadHistory{NodeID: nodeID, BlockCID: result.BlockCID, BlockSize: result.BlockSize}

		record, _ := s.NodeManager.LoadAssetRecord(blockHash)
		if record != nil && record.CID != "" {
			blockDownloadInfo.AssetCID = result.BlockCID
		}
	}

	return nil
}

func (s *Scheduler) UserProofsOfWork(ctx context.Context, proofs []*types.UserProofOfWork) error {
	return nil
}

// GetEdgeDownloadInfos finds edge download information for a given CID
func (s *Scheduler) GetEdgeDownloadInfos(ctx context.Context, cid string) ([]*types.EdgeDownloadInfo, error) {
	if cid == "" {
		return nil, xerrors.New("cids is nil")
	}

	hash, err := cidutil.CIDString2HashString(cid)
	if err != nil {
		return nil, xerrors.Errorf("%s cid to hash err:%s", cid, err.Error())
	}

	rows, err := s.NodeManager.LoadReplicasByHash(hash, []types.ReplicaStatus{types.ReplicaStatusSucceeded})
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	titanRsa := titanrsa.New(crypto.SHA256, crypto.SHA256.New())
	infos := make([]*types.EdgeDownloadInfo, 0)

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

		info := &types.EdgeDownloadInfo{
			URL:          eNode.DownloadAddr(),
			NodeID:       nodeID,
			Credentials:  credentials,
			NatType:      eNode.NATType,
			SchedulerURL: s.SchedulerCfg.RPCURL,
		}
		infos = append(infos, info)
	}

	return infos, nil
}
