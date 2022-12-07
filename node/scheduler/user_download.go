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
	"github.com/linguohua/titan/node/helper"
	titanRsa "github.com/linguohua/titan/node/rsa"
	"github.com/linguohua/titan/node/scheduler/db/cache"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"golang.org/x/xerrors"
)

// NodeDownloadBlockResult node result for user download block
func (s *Scheduler) NodeResultForUserDownloadBlock(ctx context.Context, result api.NodeBlockDownloadResult) error {
	deviceID := handler.GetDeviceID(ctx)

	if !s.nodeManager.IsDeviceExist(deviceID, 0) {
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
		if err := incrDeviceReward(deviceID, reward); err != nil {
			return err
		}
	}

	s.recordDownloadBlock(record, &result, deviceID, "")
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
		if err := incrDeviceReward(deviceID, reward); err != nil {
			return err
		}
	}

	s.recordDownloadBlock(record, nil, "", "")
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
	clientIP := handler.GetRequestIP(ctx)
	devicePrivateKey := make(map[string]*rsa.PrivateKey)
	infoMap := make(map[string][]api.DownloadInfoResult)

	for _, cid := range cids {
		infos, err := s.nodeManager.FindNodeDownloadInfos(cid)
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
		err = s.recordDownloadBlock(record, nil, "", clientIP)
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

	clientIP := handler.GetRequestIP(ctx)
	devicePrivateKey := make(map[string]*rsa.PrivateKey)
	infoMap := make(map[string]api.DownloadInfoResult)

	for _, cid := range cids {
		infos, err := s.nodeManager.FindNodeDownloadInfos(cid)
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
		err = s.recordDownloadBlock(record, nil, "", clientIP)
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

	infos, err := s.nodeManager.FindNodeDownloadInfos(cid)
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

	err = s.recordDownloadBlock(record, nil, "", handler.GetRequestIP(ctx))
	if err != nil {
		log.Errorf("GetDownloadInfoWithBlock,recordDownloadBlock error %s", err.Error())
	}

	return infos[0], nil
}

func (s *Scheduler) verifyNodeResultForUserDownloadBlock(deviceID string, record cache.DownloadBlockRecord, sign []byte) error {
	verifyContent := fmt.Sprintf("%s%d%d%d", record.Cid, record.SN, record.SignTime, record.Timeout)
	edgeNode := s.nodeManager.GetEdgeNode(deviceID)
	if edgeNode != nil {
		return titanRsa.VerifyRsaSign(&edgeNode.PrivateKey.PublicKey, sign, verifyContent)
	}

	candidate := s.nodeManager.GetCandidateNode(deviceID)
	if candidate != nil {
		return titanRsa.VerifyRsaSign(&candidate.PrivateKey.PublicKey, sign, verifyContent)
	}

	authInfo, err := persistent.GetDB().GetNodeAuthInfo(deviceID)
	if err != nil {
		return err
	}

	privateKey, err := titanRsa.Pem2PrivateKey(authInfo.PrivateKey)
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

		sign, err := titanRsa.RsaSign(privateKey, fmt.Sprintf("%s%d%d%d", cid, sn, signTime, blockDonwloadTimeout))
		if err != nil {
			return nil, err
		}

		downloadInfoResult := api.DownloadInfoResult{URL: result.URL, Sign: hex.EncodeToString(sign), SN: sn, SignTime: signTime, TimeOut: blockDonwloadTimeout}
		downloadInfoResults = append(downloadInfoResults, downloadInfoResult)
	}

	return downloadInfoResults, nil
}

func (s *Scheduler) getDeviccePrivateKey(deviceID string) (*rsa.PrivateKey, error) {
	edge := s.nodeManager.GetEdgeNode(deviceID)
	if edge != nil {
		return edge.PrivateKey, nil
	}

	candidate := s.nodeManager.GetCandidateNode(deviceID)
	if candidate != nil {
		return candidate.PrivateKey, nil
	}

	authInfo, err := persistent.GetDB().GetNodeAuthInfo(deviceID)
	if err != nil {
		return nil, err
	}

	privateKey, err := titanRsa.Pem2PrivateKey(authInfo.PrivateKey)
	if err != nil {
		return nil, err
	}

	return privateKey, nil
}

func (s *Scheduler) recordDownloadBlock(record cache.DownloadBlockRecord, nodeResult *api.NodeBlockDownloadResult, deviceID string, clientIP string) error {
	info, err := persistent.GetDB().GetBlockDownloadInfoByID(record.ID)
	if err != nil {
		return err
	}

	if info == nil {
		info = &api.BlockDownloadInfo{ID: record.ID, CreatedTime: time.Unix(record.SignTime, 0)}

		blockInfo, err := s.getBlockInfoFromDB(record.Cid, clientIP)
		if err != nil {
			return err
		}
		if blockInfo != nil {
			carfileCID, err := helper.HashString2CidString(blockInfo.CarfileHash)
			if err != nil {
				return err
			}
			// block cid and carfile cid is convert to same version cid, so can compare later
			blockCID, err := helper.HashString2CidString(blockInfo.CIDHash)
			if err != nil {
				return err
			}
			info.CarfileCID = carfileCID
			info.BlockCID = blockCID
			info.BlockSize = blockInfo.Size
			info.ClientIP = clientIP
		}

		if info.BlockCID == info.CarfileCID {
			cache.GetDB().AddLatestDownloadCarfile(info.CarfileCID, clientIP)
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

	if record.NodeStatus == int(blockDownloadStatusSuccess) && record.UserStatus == int(blockDownloadStatusSuccess) {
		info.CompleteTime = time.Now()
		info.Reward = 1
		info.Status = int(blockDownloadStatusSuccess)
		err = cache.GetDB().RemoveDownloadBlockRecord(record.SN)
		if err != nil {
			return err
		}
		// add reward
		err = incrDeviceReward(info.DeviceID, info.Reward)
		if err != nil {
			return err
		}
		err = s.deviceBlockDownloadCount(info.DeviceID, info.BlockSize)
		if err != nil {
			return err
		}
	} else {
		err = cache.GetDB().SetDownloadBlockRecord(record)
		if err != nil {
			return err
		}
	}

	return persistent.GetDB().SetBlockDownloadInfo(info)
}

func (s *Scheduler) getBlockInfoFromDB(cid string, userIP string) (*api.BlockInfo, error) {
	blockHash, err := helper.CIDString2HashString(cid)
	if err != nil {
		return nil, err
	}

	blockInfos, err := persistent.GetDB().GetBlocksWithHash(blockHash)
	if err != nil {
		return nil, err
	}

	if len(blockInfos) == 0 {
		return nil, fmt.Errorf("Block %s not exist in db", cid)
	}

	blockInfo := s.getBlockInfoIfCarfile(blockHash, blockInfos)
	if blockInfo != nil {
		return blockInfo, nil
	}

	if len(blockInfos) == 1 {
		for _, v := range blockInfos {
			return v, nil
		}
	}

	latestDownloadList, err := cache.GetDB().GetLatestDownloadCarfiles(userIP)
	if err != nil {
		return nil, err
	}

	return s.getBlockInfoWithLatestDownloadList(blockInfos, latestDownloadList), nil

}

func (s *Scheduler) getBlockInfoIfCarfile(blockHash string, blocks map[string]*api.BlockInfo) *api.BlockInfo {
	for k, v := range blocks {
		if k == blockHash {
			return v
		}
	}

	return nil
}

func (s *Scheduler) getFirstElementFromMap(blocks map[string]*api.BlockInfo) *api.BlockInfo {
	for _, v := range blocks {
		return v
	}

	return nil
}

func (s *Scheduler) getBlockInfoWithLatestDownloadList(blockInfos map[string]*api.BlockInfo, latestDowwnloadCarfiles []string) *api.BlockInfo {
	for _, carfileCID := range latestDowwnloadCarfiles {
		blockInfo, ok := blockInfos[carfileCID]
		if ok {
			return blockInfo
		}
	}

	for _, v := range blockInfos {
		return v
	}

	return nil
}

func (s *Scheduler) deviceBlockDownloadCount(deviceID string, blockSize int) error {
	return cache.GetDB().UpdateDeviceInfo(deviceID, func(deviceInfo *api.DevicesInfo) {
		deviceInfo.DownloadCount += 1
		deviceInfo.TotalUpload += float64(blockSize)
	})
}
