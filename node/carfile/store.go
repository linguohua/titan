package carfile

import (
	"context"
	"path"
	"strings"

	"github.com/ipfs/go-datastore"
	"github.com/linguohua/titan/node/helper"
)

const (
	carfileLinkPath = "/link/"
)

func (carfileOperation *CarfileOperation) deleteBlock(blockCID, carfileCID string) error {
	blockHash, err := helper.CIDString2HashString(blockCID)
	if err != nil {
		return err
	}

	linkStr, err := carfileOperation.removeCarfileLinkFromBlock(blockCID, carfileCID)
	if err != nil {
		return err
	}

	if len(linkStr) > 0 {
		return nil
	}

	err = carfileOperation.carfileStore.DeleteBlock(blockHash)
	if err != nil {
		log.Errorf("deleteBlock delete block %s error:%s", blockHash, err.Error())
		return err
	}

	log.Infof("Delete block %s", blockCID)
	return nil
}

func (carfileOperation *CarfileOperation) saveBlock(data []byte, blockCID, carfileCID string) error {
	blockHash, err := helper.CIDString2HashString(blockCID)
	if err != nil {
		return err
	}

	exist, err := carfileOperation.carfileStore.HasBlock(blockHash)
	if err != nil {
		return err
	}

	if exist {
		log.Warnf("saveBlock, block %s aready exist", blockCID)
		return nil
	}

	err = carfileOperation.carfileStore.SaveBlock(blockHash, data)
	if err != nil {
		return err
	}

	err = carfileOperation.addCarfileLinkToBlock(blockCID, carfileCID)
	if err != nil {
		log.Errorf("saveBlock, addCarfileLinkToBlock error:%s", err)
	}

	log.Infof("saveBlock cid:%s", blockCID)
	return nil
}

func (carfileOperation *CarfileOperation) getCarfileLinkWithBlockHash(blockHash string) (string, error) {
	linkPath := path.Join(carfileLinkPath, blockHash)
	value, err := carfileOperation.ds.Get(context.Background(), datastore.NewKey(linkPath))
	if err == datastore.ErrNotFound {
		return "", nil
	}

	if err != nil {
		return "", err
	}

	return string(value), nil
}

func (carfileOperation *CarfileOperation) addCarfileLinkToBlock(blockCID, carfileCID string) error {
	blockHash, err := helper.CIDString2HashString(blockCID)
	if err != nil {
		return err
	}

	carfileHash, err := helper.CIDString2HashString(carfileCID)
	if err != nil {
		return err
	}

	linkStr, err := carfileOperation.getCarfileLinkWithBlockHash(blockHash)
	if err != nil {
		return err
	}

	if strings.Contains(linkStr, carfileHash) {
		return nil
	}

	linkStr += carfileCID

	linkPath := path.Join(carfileLinkPath, blockCID)
	return carfileOperation.ds.Put(context.Background(), datastore.NewKey(linkPath), []byte(linkStr))
}

// return new carfile link
func (carfileOperation *CarfileOperation) removeCarfileLinkFromBlock(blockCID, carfileCID string) (string, error) {
	blockHash, err := helper.CIDString2HashString(blockCID)
	if err != nil {
		return "", err
	}

	carfileHash, err := helper.CIDString2HashString(carfileCID)
	if err != nil {
		return "", err
	}

	linkStr, err := carfileOperation.getCarfileLinkWithBlockHash(blockHash)
	if err != nil {
		return "", err
	}

	if len(linkStr) == 0 {
		return "", nil
	}

	index := strings.Index(linkStr, carfileHash)
	if index < 0 {
		return linkStr, nil
	}
	linkStr = linkStr[:index] + linkStr[index+len(carfileHash):]

	linkPath := path.Join(carfileLinkPath, blockHash)
	if len(linkStr) == 0 {
		return "", carfileOperation.ds.Delete(context.Background(), datastore.NewKey(linkPath))
	}
	return linkStr, carfileOperation.ds.Put(context.Background(), datastore.NewKey(linkPath), []byte(linkStr))
}
