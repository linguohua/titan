package carfile

import (
	"github.com/ipfs/go-cid"
)

func (carfileOperation *CarfileOperation) deleteBlock(cidStr string) error {
	cid, err := cid.Decode(cidStr)
	if err != nil {
		log.Errorf("deleteBlock Decode cid %s error:%s", cidStr, err.Error())
		return err
	}

	err = carfileOperation.carfileStore.DeleteBlock(cid.Hash().String())
	if err != nil {
		log.Errorf("deleteBlock blockstore delete block %s error:%s", cid, err.Error())
		return err
	}

	log.Infof("Delete block %s", cid.String())
	return nil
}

func (carfileOperation *CarfileOperation) saveBlock(data []byte, cidStr string) error {
	cid, err := cid.Decode(cidStr)
	if err != nil {
		return err
	}

	exist, err := carfileOperation.carfileStore.HasBlock(cid.Hash().String())
	if err != nil {
		return err
	}

	if exist {
		log.Warnf("saveBlock, block %s aready exist", cidStr)
		return nil
	}

	err = carfileOperation.carfileStore.SaveBlock(cid.Hash().String(), data)
	if err != nil {
		return err
	}

	log.Infof("saveBlock cid:%s", cidStr)
	return nil
}
