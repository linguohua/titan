package block

import (
	"context"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore/query"
	"github.com/linguohua/titan/node/helper"
	"github.com/multiformats/go-multihash"
)

func (block *Block) getFIDFromCID(cid string) (string, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hash, err := helper.CIDString2HashString(cid)
	if err != nil {
		return "", err
	}
	value, err := block.ds.Get(ctx, helper.NewKeyHash(hash))
	if err != nil {
		return "", err
	}
	return string(value), nil
}

func (block *Block) getCIDFromFID(fid string) (*cid.Cid, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	value, err := block.ds.Get(ctx, helper.NewKeyFID(fid))
	if err != nil {
		return nil, err
	}

	multihash, err := multihash.FromHexString(string(value))
	if err != nil {
		return nil, err
	}

	cid := cid.NewCidV1(cid.Raw, multihash)
	return &cid, nil

}

func (block *Block) deleteBlock(cid string) error {
	fid, err := block.getFIDFromCID(cid)
	if err != nil {
		log.Errorf("deleteBlock getFIDFromCID %s error:%s", cid, err.Error())
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = block.ds.Delete(ctx, helper.NewKeyFID(fid))
	if err != nil {
		log.Errorf("deleteBlock datastore delete fid %s error:%s", fid, err.Error())
		return err
	}

	hash, err := helper.CIDString2HashString(cid)
	if err != nil {
		log.Errorf("deleteBlock CIDString2HashString error:%s, cid:%s", err.Error(), cid)
		return err
	}

	err = block.ds.Delete(ctx, helper.NewKeyHash(hash))
	if err != nil {
		log.Errorf("deleteBlock datastore delete cid %s error:%s", cid, err.Error())
		return err
	}

	err = block.blockStore.Delete(cid)
	if err != nil {
		log.Errorf("deleteBlock blockstore delete block %s error:%s", cid, err.Error())
		return err
	}

	log.Infof("Delete block %s fid %s", cid, fid)
	return nil
}

func (block *Block) updateCidAndFid(ctx context.Context, cid, fid string) error {
	// delete old fid relate cid
	oldCid, _ := block.getCIDFromFID(fid)
	if oldCid != nil && oldCid.String() != cid {
		block.ds.Delete(ctx, helper.NewKeyHash(oldCid.Hash().String()))
		log.Errorf("updateCidAndFid Fid %s aready exist, and relate cid %s will be delete", fid, oldCid)
	}
	// delete old cid relate fid
	oldFid, _ := block.getFIDFromCID(cid)
	if len(oldFid) > 0 && oldFid != fid {
		block.ds.Delete(ctx, helper.NewKeyFID(oldFid))
		log.Errorf("updateCidAndFid Cid %s aready exist, and relate fid %s will be delete", cid, oldFid)
	}

	err := block.ds.Put(ctx, helper.NewKeyFID(fid), []byte(cid))
	if err != nil {
		return err
	}

	hash, err := helper.CIDString2HashString(cid)
	if err != nil {
		log.Errorf("updateCidAndFid CIDString2HashString error:%s, cid:%s", err.Error(), cid)
		return err
	}

	err = block.ds.Put(ctx, helper.NewKeyHash(hash), []byte(fid))
	if err != nil {
		return err
	}

	return nil
}

func (block *Block) getBlock(cid string) ([]byte, error) {
	hash, err := helper.CIDString2HashString(cid)
	if err != nil {
		return nil, err
	}
	return block.blockStore.Get(hash)
}

func (block *Block) saveBlock(ctx context.Context, data []byte, cid, fid string) error {
	block.saveBlockLock.Lock()
	defer block.saveBlockLock.Unlock()

	log.Infof("saveBlock fid:%s, cid:%s", fid, cid)
	err := block.blockStore.Put(cid, data)
	if err != nil {
		return err
	}

	return block.updateCidAndFid(ctx, cid, fid)
}

func (block *Block) deleteAllBlocks() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	q := query.Query{Prefix: "fid"}
	results, err := block.ds.Query(ctx, q)
	if err != nil {
		log.Errorf("deleteAllBlocks error:%s", err.Error())
		return err
	}

	result := results.Next()
	for {
		r, ok := <-result
		if !ok {
			log.Info("delete all block complete")
			return nil
		}

		_, err = block.AnnounceBlocksWasDelete(ctx, []string{string(r.Value)})
		if err != nil {
			log.Infof("err:%v, cid:%s", err, string(r.Value))
		}
		log.Infof("deleteAllBlocks key:%s", r.Key)
	}
}
