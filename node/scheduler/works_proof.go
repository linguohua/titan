package scheduler

import (
	"context"
	"crypto"

	"github.com/linguohua/titan/api/types"
	"github.com/linguohua/titan/node/cidutil"
	titanrsa "github.com/linguohua/titan/node/rsa"
	"golang.org/x/xerrors"
)

func (s *Scheduler) SubmitUserProofsOfWork(ctx context.Context, proofs []*types.UserProofOfWork) error {
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
