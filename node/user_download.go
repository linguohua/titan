package scheduler

import (
	"context"
	"crypto/rsa"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/handler"
	"github.com/linguohua/titan/node/scheduler/db/cache"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"golang.org/x/xerrors"
)

// NodeDownloadBlockResult node result for user download block
func (s *Scheduler) NodeResultForUserDownloadBlock(ctx context.Context, result api.NodeBlockDownloadResult) error {
	deviceID := handler.GetDeviceID(ctx)

	if !s.nodeManager.isDeviceExist(deviceID, 0) {
		return xerrors.Errorf("node not Exist: %s", deviceID)
	}

	record, err := cache.GetDB().GetDownloadBlockRecord(result.SN)
	if err != nil {
		log.Errorf("NodeDownloadBlockResult, GetBlockDownloadRecord error:%s", err.Error())
		return err
	}

	err = s.verifyNodeResultForUserDownloadBlock(deviceID, record, result.Sign)
	if err != nil {
		log.Errorf("NodeDownloadBlockResult, verifyNodeDownloadBlockSign error:%s", err.Error())
		return err
	}

	record.NodeStatus = int(blockDownloadStatusFailed)
	if result.Result {
		record.NodeStatus = int(blockDownloadStatusSuccess)
	}

	reward := int64(0)
	if record.NodeStatus == int(blockDownloadStatusSuccess) && record.UserStatus == int(blockDownloadStatusSuccess) {
		reward = 1
		// add reward
		if err := cache.GetDB().IncrDeviceReward(deviceID, reward); err != nil {
			return err
		}
	}

	s.recordDownloadBlock(record, &result, int(reward), deviceID)
	return nil
}

func (s *Scheduler) handleUserDownloadBlockResult(ctx context.Context, result api.UserBlockDownloadResult) error {
	record, err := cache.GetDB().GetDownloadBlockRecord(result.SN)
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
	if result.Result {
		record.UserStatus = int(blockDownloadStatusSuccess)
	}

	var deviceID string

	reward := int64(0)
	if record.NodeStatus == int(blockDownloadStatusSuccess) && record.UserStatus == int(blockDownloadStatusSuccess) {
		reward = 1
		// add reward
		if err := cache.GetDB().IncrDeviceReward(deviceID, reward); err != nil {
			return err
		}
	}

	s.recordDownloadBlock(record, nil, 0, "")
	return nil
}

// UserDownloadBlockResults node result for user download block
func (s *Scheduler) UserDownloadBlockResults(ctx context.Context, results []api.UserBlockDownloadResult) error {
	for _, result := range results {
		err := s.handleUserDownloadBlockResult(ctx, result)
		if err != nil {
			return err
		}
	}
	return nil
}

// GetDownloadInfosWithBlocks find node
func (s *Scheduler) GetDownloadInfosWithBlocks(ctx context.Context, cids []string, publicKey string) (map[string][]api.DownloadInfoResult, error) {
	if len(cids) < 1 {
		return nil, xerrors.New("cids is nil")
	}

	devicePrivateKey := make(map[string]*rsa.PrivateKey)
	infoMap := make(map[string][]api.DownloadInfoResult)

	for _, cid := range cids {
		infos, err := s.nodeManager.findNodeDownloadInfos(cid)
		if err != nil {
			continue
		}

		sn, err := cache.GetDB().IncrBlockDownloadSN()
		if err != nil {
			continue
		}

		signTime := time.Now().Unix()
		infos, err = s.signDownloadInfos(cid, sn, signTime, infos, devicePrivateKey)
		if err != nil {
			continue
		}
		infoMap[cid] = infos

		record := cache.DownloadBlockRecord{
			SN:            sn,
			ID:            uuid.New().String(),
			Cid:           cid,
			SignTime:      signTime,
			Timeout:       blockDonwloadTimeout,
			UserPublicKey: publicKey,
			NodeStatus:    int(blockDownloadStatusUnknow),
			UserStatus:    int(blockDownloadStatusUnknow),
		}
		err = s.recordDownloadBlock(record, nil, 0, "")
		if err != nil {
			log.Errorf("GetDownloadInfosWithBlocks,recordDownloadBlock error %s", err.Error())
		}
	}

	return infoMap, nil
}

// GetDownloadInfoWithBlocks find node
func (s *Scheduler) GetDownloadInfoWithBlocks(ctx context.Context, cids []string, publicKey string) (map[string]api.DownloadInfoResult, error) {
	if len(cids) < 1 {
		return nil, xerrors.New("cids is nil")
	}

	devicePrivateKey := make(map[string]*rsa.PrivateKey)
	infoMap := make(map[string]api.DownloadInfoResult)

	for _, cid := range cids {
		infos, err := s.nodeManager.findNodeDownloadInfos(cid)
		if err != nil {
			continue
		}

		info := infos[randomNum(0, len(infos))]

		sn, err := cache.GetDB().IncrBlockDownloadSN()
		if err != nil {
			continue
		}

		signTime := time.Now().Unix()
		infos, err = s.signDownloadInfos(cid, sn, signTime, []api.DownloadInfoResult{info}, devicePrivateKey)
		if err != nil {
			continue
		}

		infoMap[cid] = infos[0]

		record := cache.DownloadBlockRecord{
			SN:            sn,
			ID:            uuid.New().String(),
			Cid:           cid,
			SignTime:      signTime,
			Timeout:       blockDonwloadTimeout,
			UserPublicKey: publicKey,
			NodeStatus:    int(blockDownloadStatusUnknow),
			UserStatus:    int(blockDownloadStatusUnknow),
		}
		err = s.recordDownloadBlock(record, nil, 0, "")
		if err != nil {
			log.Errorf("GetDownloadInfoWithBlocks,recordDownloadBlock error %s", err.Error())
		}
	}

	return infoMap, nil
}

// GetDownloadInfoWithBlock find node
func (s *Scheduler) GetDownloadInfoWithBlock(ctx context.Context, cid string, publicKey string) (api.DownloadInfoResult, error) {
	if cid == "" {
		return api.DownloadInfoResult{}, xerrors.New("cids is nil")
	}

	infos, err := s.nodeManager.findNodeDownloadInfos(cid)
	if err != nil {
		return api.DownloadInfoResult{}, err
	}

	info := infos[randomNum(0, len(infos))]

	sn, err := cache.GetDB().IncrBlockDownloadSN()
	if err != nil {
		return api.DownloadInfoResult{}, err
	}
	signTime := time.Now().Unix()
	infos, err = s.signDownloadInfos(cid, sn, signTime, []api.DownloadInfoResult{info}, make(map[string]*rsa.PrivateKey))
	if err != nil {
		return api.DownloadInfoResult{}, err
	}

	record := cache.DownloadBlockRecord{
		SN:            sn,
		ID:            uuid.New().String(),
		Cid:           cid,
		SignTime:      signTime,
		Timeout:       blockDonwloadTimeout,
		UserPublicKey: publicKey,
		NodeStatus:    int(blockDownloadStatusUnknow),
		UserStatus:    int(blockDownloadStatusUnknow),
	}

	err = s.recordDownloadBlock(record, nil, 0, "")
	if err != nil {
		log.Errorf("GetDownloadInfoWithBlock,recordDownloadBlock error %s", err.Error())
	}

	return infos[0], nil
}

func (s *Scheduler) verifyNodeResultForUserDownloadBlock(deviceID string, record cache.DownloadBlockRecord, sign []byte) error {
	verifyContent := fmt.Sprintf("%s%d%d%d", record.Cid, record.SN, record.SignTime, record.Timeout)
	edgeNode := s.nodeManager.getEdgeNode(deviceID)
	if edgeNode != nil {
		return verifyRsaSign(&edgeNode.privateKey.PublicKey, sign, verifyContent)
	}

	candidate := s.nodeManager.getCandidateNode(deviceID)
	if candidate != nil {
		return verifyRsaSign(&candidate.privateKey.PublicKey, sign, verifyContent)
	}

	authInfo, err := persistent.GetDB().GetNodeAuthInfo(deviceID)
	if err != nil {
		return err
	}

	privateKey, err := pem2PrivateKey(authInfo.PrivateKey)
	if err != nil {
		return err
	}
	return verifyRsaSign(&privateKey.PublicKey, sign, verifyContent)
}

func (s *Scheduler) verifyUserDownloadBlockSign(publicPem, cid string, sign []byte) error {
	publicKey, err := pem2PublicKey(publicPem)
	if err != nil {
		return err
	}
	return verifyRsaSign(publicKey, sign, cid)
}

func (s *Scheduler) signDownloadInfos(cid string, sn int64, signTime int64, results []api.DownloadInfoResult, devicePrivateKeys map[string]*rsa.PrivateKey) ([]api.DownloadInfoResult, error) {
	downloadInfoResults := make([]api.DownloadInfoResult, 0, len(results))
	for _, result := range results {
		privateKey, ok := devicePrivateKeys[result.DeviceID]
		if !ok {
			var err error
			privateKey, err = s.getDeviccePrivateKey(result.DeviceID)
			if err != nil {
				return nil, err
			}
			devicePrivateKeys[result.DeviceID] = privateKey
		}

		sign, err := rsaSign(privateKey, fmt.Sprintf("%s%d%d%d", cid, sn, signTime, blockDonwloadTimeout))
		if err != nil {
			return nil, err
		}

		downloadInfoResult := api.DownloadInfoResult{URL: result.URL, Sign: hex.EncodeToString(sign), SN: sn, SignTime: signTime, TimeOut: blockDonwloadTimeout}
		downloadInfoResults = append(downloadInfoResults, downloadInfoResult)
	}

	return downloadInfoResults, nil
}

func (s *Scheduler) getDeviccePrivateKey(deviceID string) (*rsa.PrivateKey, error) {
	edge := s.nodeManager.getEdgeNode(deviceID)
	if edge != nil {
		return edge.privateKey, nil
	}

	candidate := s.nodeManager.getCandidateNode(deviceID)
	if candidate != nil {
		return candidate.privateKey, nil
	}

	authInfo, err := persistent.GetDB().GetNodeAuthInfo(deviceID)
	if err != nil {
		return nil, err
	}

	privateKey, err := pem2PrivateKey(authInfo.PrivateKey)
	if err != nil {
		return nil, err
	}

	return privateKey, nil
}

func (s *Scheduler) recordDownloadBlock(record cache.DownloadBlockRecord, nodeResult *api.NodeBlockDownloadResult, reward int, deviceID string) error {
	info, err := persistent.GetDB().GetBlockDownloadInfoByID(record.ID)
	if err != nil {
		return err
	}

	if info == nil {
		info = &api.BlockDownloadInfo{ID: record.ID, BlockCID: record.Cid, CreatedTime: time.Unix(record.SignTime, 0)}
	}

	if nodeResult != nil {
		info.Speed = int64(nodeResult.DownloadSpeed)
		info.BlockSize = nodeResult.BlockSize
		info.ClientIP = nodeResult.ClientIP
		info.FailedReason = nodeResult.FailedReason
	}

	if len(deviceID) > 0 {
		info.DeviceID = deviceID
	}

	if reward > 0 {
		info.Reward = int64(reward)
	}

	if record.NodeStatus == int(blockDownloadStatusFailed) || record.UserStatus == int(blockDownloadStatusFailed) {
		info.Status = int(blockDownloadStatusFailed)
	}

	if record.NodeStatus == int(blockDownloadStatusSuccess) && record.UserStatus == int(blockDownloadStatusSuccess) {
		info.CompleteTime = time.Now()
		info.Status = int(blockDownloadStatusSuccess)
		err = cache.GetDB().RemoveDownloadBlockRecord(record.SN)
	} else {
		err = cache.GetDB().SetDownloadBlockRecord(record)
	}

	if err != nil {
		return err
	}

	return persistent.GetDB().SetBlockDownloadInfo(info)
}
