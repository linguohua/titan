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

	err = carfileOperation.blockStore.Delete(cid.Hash().String())
	if err != nil {
		log.Errorf("deleteBlock blockstore delete block %s error:%s", cid, err.Error())
		return err
	}

	log.Infof("Delete block %s", cid.String())
	return nil
}

func (carfileOperation *CarfileOperation) saveBlock(data []byte, cidStr string) error {
	log.Infof("saveBlock cid:%s", cidStr)

	cid, err := cid.Decode(cidStr)
	if err != nil {
		return err
	}

	err = carfileOperation.blockStore.Put(cid.Hash().String(), data)
	if err != nil {
		return err
	}
	return nil
}

// func (carfileOperation *CarfileOperation) deleteAllBlocks() error {
// 	ctx, cancel := context.WithCancel(context.Background())
// 	defer cancel()

// 	q := query.Query{Prefix: "fid"}
// 	results, err := block.ds.Query(ctx, q)
// 	if err != nil {
// 		log.Errorf("deleteAllBlocks error:%s", err.Error())
// 		return err
// 	}

// 	result := results.Next()
// 	for {
// 		r, ok := <-result
// 		if !ok {
// 			log.Info("delete all block complete")
// 			return nil
// 		}

// 		multihash, err := multihash.FromHexString(string(r.Value))
// 		if err != nil {
// 			log.Errorf("parse block hash error:%s", err.Error())
// 			continue
// 		}

// 		cid := cid.NewCidV1(cid.Raw, multihash)
// 		_, err = block.AnnounceBlocksWasDelete(ctx, []string{cid.String()})
// 		if err != nil {
// 			log.Infof("err:%v, cid:%s", err, cid.String())
// 			continue
// 		}
// 		log.Infof("deleteAllBlocks key:%s", r.Key)
// 	}
// }
