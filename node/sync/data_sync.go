package sync

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"strconv"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/block"
)

var log = logging.Logger("datasync")

type DataSync struct {
	block *block.Block
}

func NewDataSync(block *block.Block) *DataSync {
	return &DataSync{block: block}
}

func (dataSync *DataSync) GetCheckSums(ctx context.Context, req api.ReqCheckSum) (string, error) {
	return dataSync.getCheckSums(ctx, req)
}
func (dataSync *DataSync) ScrubBlocks(ctx context.Context, scrub api.ScrubBlocks) error {
	return dataSync.scrubBlocks(scrub)
}

func (dataSync *DataSync) getCheckSums(ctx context.Context, req api.ReqCheckSum) (string, error) {
	q := query.Query{Prefix: "fid"}
	ds := dataSync.block.GetDatastore(ctx)
	results, err := ds.Query(ctx, q)
	if err != nil {
		log.Errorf("deleteAllBlocks error:%s", err.Error())
		return "", err
	}
	var cidCollection string
	result := results.Next()
	for {
		r, ok := <-result
		if !ok {
			break
		}

		cidCollection += string(r.Value)
	}

	hasher := md5.New()
	hasher.Write([]byte(cidCollection))
	hash := hasher.Sum(nil)

	return hex.EncodeToString(hash), nil
}

func (dataSync *DataSync) scrubBlocks(scrub api.ScrubBlocks) error {
	startFid, err := strconv.Atoi(scrub.StartFid)
	if err != nil {
		log.Errorf("scrubBlockStore parse  error:%s", err.Error())
		return err
	}

	endFid, err := strconv.Atoi(scrub.EndFix)
	if err != nil {
		log.Errorf("scrubBlockStore error:%s", err.Error())
		return err
	}

	need2DeleteBlocks := make([]string, 0)
	blocks := scrub.Blocks
	for i := startFid; i <= endFid; i++ {
		fid := fmt.Sprintf("%d", i)
		cid, err := dataSync.block.GetCID(context.TODO(), fid)
		if err == datastore.ErrNotFound {
			continue
		}

		_, ok := blocks[fid]
		if ok {
			delete(blocks, fid)
		} else {
			need2DeleteBlocks = append(need2DeleteBlocks, cid)
		}
	}

	// delete blocks that not exist on scheduler
	dataSync.block.DeleteBlocks(context.TODO(), need2DeleteBlocks)

	// TODO: download block that not exist in local

	return nil
}
