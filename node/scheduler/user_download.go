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
	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"golang.org/x/xerrors"
)

// NodeDownloadBlockResult node result for user download block
func (s *Scheduler) NodeResultForUserDownloadBlock(ctx context.Context, result api.NodeBlockDownloadResult) error {
	deviceID := handler.GetDeviceID(ctx)
	if result.Succeed {
		err := incrDeviceReward(deviceID, 1)
		if err != nil {
			return err
		}

		blockHash, err := cidutil.CIDString2HashString(result.BlockCID)
		if err != nil {
			return err
		}

		blockDwnloadInfo := &api.BlockDownloadInfo{DeviceID: deviceID, BlockCID: result.BlockCID, BlockSize: result.BlockSize}

		carfileInfo, _ := persistent.LoadCarfileInfo(blockHash)
		if carfileInfo != nil && carfileInfo.CarfileCid != "" {
			blockDwnloadInfo.CarfileCID = result.BlockCID
		}

		err = cache.NodeDownloadCount(deviceID, blockDwnloadInfo)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Scheduler) handleUserDownloadBlockResult(ctx context.Context, result api.UserBlockDownloadResult) error {
	record, err := cache.GetDownloadBlockRecord(result.SN)
	if err != nil {
		log.Errorf("handleUserDownloadBlockResult, GetBlockDownloadRecord error:%s", err.Error())
		return err
	}

	err = s.verifyUserDownloadBlockSign(record.UserPublicKey, record.Cid, result.Sign)
	if err != nil {
		log.Errorf("handleUserDownloadBlockResult, verifyNodeDownloadBlockSign error:%s", err.Error())
		return err
	}

	record.UserStatus = int(blockDownloadStatusFailed)
	if result.Succeed {
		record.UserStatus = int(blockDownloadStatusSucceeded)
	}

	s.recordDownloadBlock(record, nil, "", "")
	return nil
}

// UserDownloadBlockResults node result for user download block
func (s *Scheduler) UserDownloadBlockResults(ctx context.Context, results []api.UserBlockDownloadResult) error {
	return nil
}

// GetDownloadInfosWithCarfile find node
func (s *Scheduler) GetDownloadInfosWithCarfile(ctx context.Context, cid string, publicKey string) ([]*api.DownloadInfoResult, error) {
	if cid == "" {
		return nil, xerrors.New("cids is nil")
	}

	userURL := handler.GetRemoteAddr(ctx)

	log.Infof("GetDownloadInfosWithCarfile url:%s", userURL)

	infos, err := s.nodeManager.FindNodeDownloadInfos(cid, userURL)
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
	edgeNode := s.nodeManager.GetEdgeNode(deviceID)
	if edgeNode != nil {
		return titanRsa.VerifyRsaSign(&edgeNode.PrivateKey().PublicKey, sign, verifyContent)
	}

	candidate := s.nodeManager.GetCandidateNode(deviceID)
	if candidate != nil {
		return titanRsa.VerifyRsaSign(&candidate.PrivateKey().PublicKey, sign, verifyContent)
	}

	privateKeyStr, err := persistent.NodePrivateKey(deviceID)
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
	sn, err := cache.IncrBlockDownloadSN()
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
	edge := s.nodeManager.GetEdgeNode(deviceID)
	if edge != nil {
		return edge.PrivateKey(), nil
	}

	candidate := s.nodeManager.GetCandidateNode(deviceID)
	if candidate != nil {
		return candidate.PrivateKey(), nil
	}

	privateKeyStr, err := persistent.NodePrivateKey(deviceID)
	if err != nil {
		return nil, err
	}

	privateKey, err := titanRsa.Pem2PrivateKey(privateKeyStr)
	if err != nil {
		return nil, err
	}

	return privateKey, nil
}

func (s *Scheduler) recordDownloadBlock(record *cache.DownloadBlockRecord, nodeResult *api.NodeBlockDownloadResult, deviceID string, clientIP string) error {
	info, err := persistent.GetBlockDownloadInfoByID(record.ID)
	if err != nil {
		return err
	}

	if info == nil {
		info = &api.BlockDownloadInfo{ID: record.ID, CreatedTime: time.Unix(record.SignTime, 0)}
		if info.BlockCID == info.CarfileCID {
			cache.AddLatestDownloadCarfile(info.CarfileCID, clientIP)
		}
	}

	if nodeResult != nil {
		info.Speed = int64(nodeResult.DownloadSpeed)
		info.FailedReason = nodeResult.FailedReason
	}

	if len(deviceID) > 0 {
		info.DeviceID = deviceID
	}

	if record.NodeStatus == int(blockDownloadStatusFailed) || record.UserStatus == int(blockDownloadStatusFailed) {
		info.Status = int(blockDownloadStatusFailed)
	}

	if record.NodeStatus == int(blockDownloadStatusSucceeded) && record.UserStatus == int(blockDownloadStatusSucceeded) {
		info.CompleteTime = time.Now()
		info.Reward = 1
		info.Status = int(blockDownloadStatusSucceeded)
		err = cache.RemoveDownloadBlockRecord(record.SN)
		if err != nil {
			return err
		}
		// add reward
		err = incrDeviceReward(info.DeviceID, info.Reward)
		if err != nil {
			return err
		}
		err = cache.NodeDownloadCount(deviceID, info)
		if err != nil {
			return err
		}
	} else {
		err = cache.SetDownloadBlockRecord(record)
		if err != nil {
			return err
		}
	}

	return persistent.SetBlockDownloadInfo(info)
}

func (s *Scheduler) getNodesUnValidate(minute int) ([]string, error) {
	return persistent.GetNodesByUserDownloadBlockIn(minute)
}
