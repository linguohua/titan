package scheduler

import (
	"context"

	"github.com/linguohua/titan/api/types"
	"github.com/linguohua/titan/node/cidutil"
	"github.com/linguohua/titan/node/handler"
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

		record, _ := s.NodeManager.LoadAssetRecord(blockHash)
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

	userURL := handler.GetRemoteAddr(ctx)

	log.Infof("EdgeDownloadInfos url:%s", userURL)

	infos, err := s.NodeManager.FindEdgeDownloadInfos(cid, userURL)
	if err != nil {
		return nil, err
	}

	return infos, nil
}
