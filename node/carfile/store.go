package carfile

import (
	"context"
	"strings"

	"github.com/ipfs/go-datastore"
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
		return err
	}

	log.Debugf("Delete block %s", blockHash)
	return nil
}

func (carfileOperation *CarfileOperation) saveBlock(data []byte, blockHash, carfileHash string) error {
	exist, err := carfileOperation.carfileStore.HasBlock(blockHash)
	if err != nil {
		return err
	}

	if !exist {
		err = carfileOperation.carfileStore.SaveBlock(blockHash, data)
		if err != nil {
			return err
		}
		carfileOperation.TotalBlockCount++
	} else {
		log.Warnf("saveBlock, block %s aready exist", blockHash)
	}

	err = carfileOperation.addCarfileLinkToBlock(blockHash, carfileHash)
	if err != nil {
		log.Errorf("saveBlock, addCarfileLinkToBlock error:%s", err)
	}

	log.Debugf("saveBlock blockHash:%s", blockHash)
	return nil
}

func (carfileOperation *CarfileOperation) getCarfileLinkFromBlock(blockHash string) (string, error) {
	value, err := carfileOperation.carfileStore.Links(context.Background(), blockHash)
	if err == datastore.ErrNotFound {
		return "", nil
	}

	if err != nil {
		return "", err
	}

	return string(value), nil
}

func (carfileOperation *CarfileOperation) addCarfileLinkToBlock(blockHash, carfileHash string) error {
	carfileOperation.carfileLinkLock.Lock()
	defer carfileOperation.carfileLinkLock.Unlock()

	linkStr, err := carfileOperation.getCarfileLinkFromBlock(blockHash)
	if err != nil {
		return err
	}

	if strings.Contains(linkStr, carfileHash) {
		return nil
	}

	linkStr += carfileHash

	return carfileOperation.carfileStore.SaveLinks(context.Background(), blockHash, []byte(linkStr))
}

// return new storage link
func (carfileOperation *CarfileOperation) removeCarfileLinkFromBlock(blockHash, carfileHash string) (string, error) {
	carfileOperation.carfileLinkLock.Lock()
	defer carfileOperation.carfileLinkLock.Unlock()

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

	if len(linkStr) == 0 {
		return "", carfileOperation.carfileStore.DeleteLinks(context.Background(), blockHash)
	}
	return linkStr, carfileOperation.carfileStore.SaveLinks(context.Background(), blockHash, []byte(linkStr))
}
