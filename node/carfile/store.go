package carfile

import (
	"context"
	"path"
	"strings"

	"github.com/ipfs/go-datastore"
)

const (
	carfileLinkPath = "/link/"
)

func (carfileOperation *CarfileOperation) deleteBlock(blockHash, carfileHash string) error {
	linkStr, err := carfileOperation.removeCarfileLinkFromBlock(blockHash, carfileHash)
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

	log.Infof("Delete block %s", blockHash)
	return nil
}

func (carfileOperation *CarfileOperation) saveBlock(data []byte, blockHash, carfileHash string) error {
	exist, err := carfileOperation.carfileStore.HasBlock(blockHash)
	if err != nil {
		return err
	}

	if exist {
		log.Warnf("saveBlock, block %s aready exist", blockHash)
		return nil
	}

	err = carfileOperation.carfileStore.SaveBlock(blockHash, data)
	if err != nil {
		return err
	}

	err = carfileOperation.addCarfileLinkToBlock(blockHash, carfileHash)
	if err != nil {
		log.Errorf("saveBlock, addCarfileLinkToBlock error:%s", err)
	}

	log.Infof("saveBlock cid:%s", blockHash)
	return nil
}

func (carfileOperation *CarfileOperation) getCarfileLinkFromBlock(blockHash string) (string, error) {
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

func (carfileOperation *CarfileOperation) addCarfileLinkToBlock(blockHash, carfileHash string) error {
	linkStr, err := carfileOperation.getCarfileLinkFromBlock(blockHash)
	if err != nil {
		return err
	}

	if strings.Contains(linkStr, carfileHash) {
		return nil
	}

	linkStr += carfileHash

	linkPath := path.Join(carfileLinkPath, blockHash)
	return carfileOperation.ds.Put(context.Background(), datastore.NewKey(linkPath), []byte(linkStr))
}

// return new carfile link
func (carfileOperation *CarfileOperation) removeCarfileLinkFromBlock(blockHash, carfileHash string) (string, error) {
	linkStr, err := carfileOperation.getCarfileLinkFromBlock(blockHash)
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
