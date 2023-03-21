package scheduler

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rsa"
	"encoding/gob"
	"time"

	"github.com/google/uuid"
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

		carfileInfo, _ := s.NodeManager.CarfileDB.CarfileInfo(blockHash)
		if carfileInfo != nil && carfileInfo.CarfileCID != "" {
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
		s.handleUserDownloadBlockResult(ctx, result)
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

	infos, err := s.NodeManager.FindNodeDownloadInfos(cid, userURL)
	if err != nil {
		return nil, err
	}

	err = s.signDownloadInfos(cid, infos, make(map[string]*rsa.PublicKey))
	if err != nil {
		return nil, err
	}

	return infos, nil
}

func (s *Scheduler) signDownloadInfos(cid string, results []*types.DownloadInfo, publicKeys map[string]*rsa.PublicKey) error {
	titanRsa := titanrsa.New(crypto.SHA256, crypto.SHA256.New())

	for index := range results {
		nodeID := results[index].NodeID

		publicKey, exist := publicKeys[nodeID]
		if !exist {
			var err error
			publicKey, err = s.getNodePublicKey(nodeID)
			if err != nil {
				log.Errorf("signDownloadInfos get private key error:%s", err.Error())
				return err
			}
			publicKeys[nodeID] = publicKey
		}

		credentials := &types.Credentials{
			ID:        uuid.NewString(),
			NodeID:    nodeID,
			CarCID:    cid,
			ValidTime: time.Now().Add(10 * time.Hour).Unix(),
		}
		ciphertext, err := s.encryptCredentials(credentials, publicKey, titanRsa)
		if err != nil {
			return err
		}

		sign, err := titanRsa.Sign(s.PrivateKey, ciphertext)
		if err != nil {
			return err
		}
		results[index].Credentials = &types.GatewayCredentials{Ciphertext: string(ciphertext), Sign: string(sign)}
	}
	return nil
}

func (s *Scheduler) getNodesUnValidate(minute int) ([]string, error) {
	return s.NodeManager.CarfileDB.GetNodesByUserDownloadBlockIn(minute)
}

func (s *Scheduler) getNodePublicKey(nodeID string) (*rsa.PublicKey, error) {
	pem, err := s.NodeManager.NodeMgrDB.NodePublicKey(nodeID)
	if err != nil {
		return nil, err
	}

	return titanrsa.Pem2PublicKey([]byte(pem))
}

func (s *Scheduler) encryptCredentials(at *types.Credentials, publicKey *rsa.PublicKey, rsa *titanrsa.Rsa) ([]byte, error) {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(at)
	if err != nil {
		return nil, err
	}

	return rsa.Encrypt(buffer.Bytes(), publicKey)
}
