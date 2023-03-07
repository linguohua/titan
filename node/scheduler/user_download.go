package scheduler

import (
	"context"
	"crypto/rsa"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/cidutil"
	"github.com/linguohua/titan/node/handler"
	titanRsa "github.com/linguohua/titan/node/rsa"
	"golang.org/x/xerrors"
)

// NodeDownloadBlockResult node result for user download block
func (s *Scheduler) UserDownloadResult(ctx context.Context, result api.UserDownloadResult) error {
	nodeID := handler.GetNodeID(ctx)
	if result.Succeed {
		blockHash, err := cidutil.CIDString2HashString(result.BlockCID)
		if err != nil {
			return err
		}

		blockDwnloadInfo := &api.DownloadRecordInfo{NodeID: nodeID, BlockCID: result.BlockCID, BlockSize: result.BlockSize}

		carfileInfo, _ := s.NodeManager.CarfileDB.LoadCarfileInfo(blockHash)
		if carfileInfo != nil && carfileInfo.CarfileCid != "" {
			blockDwnloadInfo.CarfileCID = result.BlockCID
		}

		// err = cache.NodeDownloadCount(nodeID, blockDwnloadInfo)
		// if err != nil {
		// 	return err
		// }
	}

	return nil
}

func (s *Scheduler) handleUserDownloadBlockResult(ctx context.Context, result api.UserBlockDownloadResult) error {
	// TODO: implement user download count
	return nil
}

// UserDownloadBlockResults node result for user download block
func (s *Scheduler) UserDownloadBlockResults(ctx context.Context, results []api.UserBlockDownloadResult) error {
	for _, result := range results {
		s.handleUserDownloadBlockResult(ctx, result)
	}
	return nil
}

// GetDownloadInfosWithCarfile find node
func (s *Scheduler) GetDownloadInfosWithCarfile(ctx context.Context, cid string) ([]*api.DownloadInfoResult, error) {
	if cid == "" {
		return nil, xerrors.New("cids is nil")
	}

	userURL := handler.GetRemoteAddr(ctx)

	log.Infof("GetDownloadInfosWithCarfile url:%s", userURL)

	infos, err := s.NodeManager.FindNodeDownloadInfos(cid, userURL)
	if err != nil {
		return nil, err
	}

	err = s.signDownloadInfos(cid, infos, make(map[string]*rsa.PrivateKey))
	if err != nil {
		return nil, err
	}

	return infos, nil
}

func (s *Scheduler) signDownloadInfos(cid string, results []*api.DownloadInfoResult, privateKeys map[string]*rsa.PrivateKey) error {
	sn := int64(0)

	signTime := time.Now().Unix()

	for index := range results {
		nodeID := results[index].NodeID

		privateKey, exist := privateKeys[nodeID]
		if !exist {
			var err error
			privateKey, err = s.getNodePrivateKey(nodeID)
			if err != nil {
				log.Errorf("signDownloadInfos get private key error:%s", err.Error())
				return err
			}
			privateKeys[nodeID] = privateKey
		}

		sign, err := titanRsa.RsaSign(privateKey, fmt.Sprintf("%s%d%d%d", nodeID, sn, signTime, blockDonwloadTimeout))
		if err != nil {
			log.Errorf("signDownloadInfos rsa sign error:%s", err.Error())
			return err
		}
		results[index].Sign = hex.EncodeToString(sign)
		results[index].SN = sn
		results[index].SignTime = signTime
		results[index].TimeOut = blockDonwloadTimeout
	}
	return nil
}

func (s *Scheduler) getNodePrivateKey(nodeID string) (*rsa.PrivateKey, error) {
	edge := s.NodeManager.GetEdgeNode(nodeID)
	if edge != nil {
		return edge.PrivateKey(), nil
	}

	candidate := s.NodeManager.GetCandidateNode(nodeID)
	if candidate != nil {
		return candidate.PrivateKey(), nil
	}

	privateKeyStr, err := s.NodeManager.NodeMgrDB.NodePrivateKey(nodeID)
	if err != nil {
		return nil, err
	}

	privateKey, err := titanRsa.Pem2PrivateKey(privateKeyStr)
	if err != nil {
		return nil, err
	}

	return privateKey, nil
}

func (s *Scheduler) getNodesUnValidate(minute int) ([]string, error) {
	return s.NodeManager.CarfileDB.GetNodesByUserDownloadBlockIn(minute)
}
