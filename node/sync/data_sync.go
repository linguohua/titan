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
	"github.com/linguohua/titan/node/helper"
)

var log = logging.Logger("datasync")

type DataSync struct {
	block *block.Block
}

func NewDataSync(block *block.Block) *DataSync {
	return &DataSync{block: block}
}

func (dataSync *DataSync) GetAllCheckSums(ctx context.Context, maxGroupNum int) (api.CheckSumRsp, error) {
	return dataSync.getAllCheckSums(ctx, maxGroupNum)
}
func (dataSync *DataSync) ScrubBlocks(ctx context.Context, scrub api.ScrubBlocks) error {
	return dataSync.scrubBlocks(scrub)
}

func (dataSync *DataSync) GetCheckSumsInRange(ctx context.Context, req api.ReqCheckSumInRange) (api.CheckSumRsp, error) {
	return dataSync.getCheckSumsInRange(ctx, req)
}

func (dataSync *DataSync) getAllCheckSums(ctx context.Context, maxGroupNum int) (api.CheckSumRsp, error) {
	rsp := api.CheckSumRsp{CheckSums: make([]api.CheckSum, 0)}
	ds := dataSync.block.GetDatastore(ctx)

	q := query.Query{Prefix: "fid"}
	results, err := ds.Query(ctx, q)
	if err != nil {
		log.Errorf("getCheckSums datastore query error:%s", err.Error())
		return rsp, err
	}

	type block struct {
		fid string
		cid string
	}

	var blockCollection = make([]block, 0, 10000)

	for {
		r, ok := results.NextSync()
		if !ok {
			break
		}

		block := block{fid: r.Key[len(helper.KeyFidPrefix):], cid: string(r.Value)}
		blockCollection = append(blockCollection, block)
	}

	blockCollectionSize := len(blockCollection)
	groupSize := blockCollectionSize / maxGroupNum
	if blockCollectionSize%maxGroupNum != 0 {
		groupSize += 1
	}

	groupNum := blockCollectionSize / groupSize
	if blockCollectionSize%groupSize != 0 {
		groupNum += 1
	}

	startFid := "0"
	for i := 0; i < groupNum; i++ {
		startIndex := i * groupSize
		endIndex := startIndex + groupSize
		if endIndex > blockCollectionSize {
			endIndex = blockCollectionSize
		}

		var blockCollectionStr string
		for j := startIndex; j < endIndex; j++ {
			blockCollectionStr += blockCollection[j].cid
		}

		hash := string2Hash(blockCollectionStr)
		endFid := blockCollection[endIndex-1].fid
		checkSum := api.CheckSum{Hash: hash, StartFid: startFid, EndFid: endFid}
		rsp.CheckSums = append(rsp.CheckSums, checkSum)

		endFidInt, err := strconv.Atoi(endFid)
		if err != nil {
			log.Panicf("getCheckSums faile:%s", err.Error())
		}

		startFid = fmt.Sprintf("%d", endFidInt+1)

	}

	return rsp, nil
}

func (dataSync *DataSync) getCheckSumsInRange(ctx context.Context, req api.ReqCheckSumInRange) (api.CheckSumRsp, error) {
	rsp := api.CheckSumRsp{CheckSums: make([]api.CheckSum, 0)}
	startFidInt, err := strconv.Atoi(req.StartFid)
	if err != nil {
		log.Errorf("scrubBlockStore parse  error:%s", err.Error())
		return rsp, err
	}

	endFidInt, err := strconv.Atoi(req.EndFid)
	if err != nil {
		log.Errorf("scrubBlockStore error:%s", err.Error())
		return rsp, err
	}

	maxFid, err := dataSync.getMaxFid()
	if err != nil {
		log.Errorf("scrubBlockStore error:%s", err.Error())
		return rsp, err
	}

	if maxFid == 0 {
		log.Infow("scrubBlockStore, no block exist")
		return rsp, nil
	}

	if endFidInt > maxFid {
		endFidInt = maxFid
	}

	type block struct {
		fid string
		cid string
	}

	var blockCollection = make([]block, 0, 10000)

	for i := startFidInt; i <= endFidInt; i++ {
		fid := fmt.Sprintf("%d", i)
		cid, err := dataSync.block.GetCID(context.TODO(), fid)
		if err == datastore.ErrNotFound {
			continue
		}

		block := block{fid: fid, cid: cid}
		blockCollection = append(blockCollection, block)
	}

	blockCollectionSize := len(blockCollection)
	groupSize := blockCollectionSize / req.MaxGroupNum
	if blockCollectionSize%req.MaxGroupNum != 0 {
		groupSize += 1
	}

	groupNum := blockCollectionSize / groupSize
	if blockCollectionSize%groupSize != 0 {
		groupNum += 1
	}

	startFid := req.StartFid
	for i := 0; i < groupNum; i++ {
		startIndex := i * groupSize
		endIndex := startIndex + groupSize
		if endIndex > blockCollectionSize {
			endIndex = blockCollectionSize
		}

		var blockCollectionStr string
		for j := startIndex; j < endIndex; j++ {
			blockCollectionStr += blockCollection[j].cid
		}

		hash := string2Hash(blockCollectionStr)
		endFid := blockCollection[endIndex-1].fid
		checkSum := api.CheckSum{Hash: hash, StartFid: startFid, EndFid: endFid}
		rsp.CheckSums = append(rsp.CheckSums, checkSum)

		endFidInt, err := strconv.Atoi(endFid)
		if err != nil {
			log.Panicf("getCheckSums faile:%s", err.Error())
		}

		startFid = fmt.Sprintf("%d", endFidInt+1)

	}

	return rsp, nil
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

	maxFid, err := dataSync.getMaxFid()
	if err != nil {
		log.Errorf("scrubBlockStore error:%s", err.Error())
		return err
	}

	if maxFid == 0 {
		log.Infow("scrubBlockStore, no block exist")
		return nil
	}

	if endFid > maxFid {
		endFid = maxFid
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
	// blocks is need to download

	return nil
}

func string2Hash(value string) string {
	hasher := md5.New()
	hasher.Write([]byte(value))
	hash := hasher.Sum(nil)
	return hex.EncodeToString(hash)
}

func (dataSync *DataSync) getMaxFid() (int, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ds := dataSync.block.GetDatastore(ctx)

	q := query.Query{Prefix: "fid", Orders: []query.Order{query.OrderByKeyDescending{}}}
	results, err := ds.Query(ctx, q)
	if err != nil {
		log.Errorf("getCheckSums datastore query error:%s", err.Error())
		return 0, err
	}

	r, ok := results.NextSync()
	if ok {
		fid := r.Key[len(helper.KeyFidPrefix):]
		fidInt, err := strconv.Atoi(fid)
		if err != nil {
			log.Errorf("scrubBlockStore error:%s", err.Error())
			return 0, err
		}

		return fidInt, nil
	}

	return 0, nil

}
