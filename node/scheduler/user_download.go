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
	"github.com/linguohua/titan/node/scheduler/db/cache"
	"golang.org/x/xerrors"
)

// NodeDownloadBlockResult node result for user download block
func (s *Scheduler) NodeResultForUserDownloadBlock(ctx context.Context, result api.NodeBlockDownloadResult) error {
	deviceID := handler.GetDeviceID(ctx)
	if result.Succeed {
		// err := incrDeviceReward(deviceID, 1)
		// if err != nil {
		// 	return err
		// }

		blockHash, err := cidutil.CIDString2HashString(result.BlockCID)
		if err != nil {
			return err
		}

		blockDwnloadInfo := &api.BlockDownloadInfo{DeviceID: deviceID, BlockCID: result.BlockCID, BlockSize: result.BlockSize}

		carfileInfo, _ := s.NodeManager.CarfileDB.LoadCarfileInfo(blockHash)
		if carfileInfo != nil && carfileInfo.CarfileCid != "" {
			blockDwnloadInfo.CarfileCID = result.BlockCID
		}

		// err = cache.NodeDownloadCount(deviceID, blockDwnloadInfo)
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
func (s *Scheduler) GetDownloadInfosWithCarfile(ctx context.Context, cid string, publicKey string) ([]*api.DownloadInfoResult, error) {
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

func (s *Scheduler) verifyNodeResultForUserDownloadBlock(deviceID string, record *cache.DownloadBlockRecord, sign []byte) error {
	verifyContent := fmt.Sprintf("%s%d%d%d", record.Cid, record.SN, record.SignTime, record.Timeout)
	edgeNode := s.NodeManager.GetEdgeNode(deviceID)
	if edgeNode != nil {
		return titanRsa.VerifyRsaSign(&edgeNode.PrivateKey().PublicKey, sign, verifyContent)
	}

	candidate := s.NodeManager.GetCandidateNode(deviceID)
	if candidate != nil {
		return titanRsa.VerifyRsaSign(&candidate.PrivateKey().PublicKey, sign, verifyContent)
	}

	privateKeyStr, err := s.NodeManager.NodeMgrDB.NodePrivateKey(deviceID)
	if err != nil {
		return err
	}

	privateKey, err := titanRsa.Pem2PrivateKey(privateKeyStr)
	if err != nil {
		return err
	}
	return titanRsa.VerifyRsaSign(&privateKey.PublicKey, sign, verifyContent)
}

func (s *Scheduler) verifyUserDownloadBlockSign(publicPem, cid string, sign []byte) error {
	publicKey, err := titanRsa.Pem2PublicKey(publicPem)
	if err != nil {
		return err
	}
	return titanRsa.VerifyRsaSign(publicKey, sign, cid)
}

func (s *Scheduler) signDownloadInfos(cid string, results []*api.DownloadInfoResult, devicePrivateKeys map[string]*rsa.PrivateKey) error {
	sn, err := s.NodeManager.NodeMgrCache.IncrBlockDownloadSN()
	if err != nil {
		log.Errorf("signDownloadInfos incr block download sn error:%s", err.Error())
		return err
	}

	signTime := time.Now().Unix()

	for index := range results {
		deviceID := results[index].DeviceID

		privateKey, exist := devicePrivateKeys[deviceID]
		if !exist {
			var err error
			privateKey, err = s.getDevicePrivateKey(deviceID)
			if err != nil {
				log.Errorf("signDownloadInfos get private key error:%s", err.Error())
				return err
			}
			devicePrivateKeys[deviceID] = privateKey
		}

		sign, err := titanRsa.RsaSign(privateKey, fmt.Sprintf("%s%d%d%d", deviceID, sn, signTime, blockDonwloadTimeout))
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

func (s *Scheduler) getDevicePrivateKey(deviceID string) (*rsa.PrivateKey, error) {
	edge := s.NodeManager.GetEdgeNode(deviceID)
	if edge != nil {
		return edge.PrivateKey(), nil
	}

	candidate := s.NodeManager.GetCandidateNode(deviceID)
	if candidate != nil {
		return candidate.PrivateKey(), nil
	}

	privateKeyStr, err := s.NodeManager.NodeMgrDB.NodePrivateKey(deviceID)
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
