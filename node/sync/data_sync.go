package sync

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"sort"
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
		fid int
		cid string
	}

	var blockCollection = make([]block, 0, 10000)

	for {
		r, ok := results.NextSync()
		if !ok {
			break
		}

		fidStr := r.Key[len(helper.KeyFidPrefix)+1:]
		fid, err := strconv.Atoi(fidStr)
		if err != nil {
			log.Errorf("scrubBlockStore error:%s", err.Error())
		}

		block := block{fid: fid, cid: string(r.Value)}
		// log.Infof("fid:%s,key:%s", block.fid, r.Key)
		blockCollection = append(blockCollection, block)
	}

	sort.Slice(blockCollection, func(i, j int) bool {
		return blockCollection[i].fid < blockCollection[j].fid
	})

	blockCollectionSize := len(blockCollection)
	groupSize := blockCollectionSize / maxGroupNum
	if blockCollectionSize%maxGroupNum != 0 {
		groupSize += 1
	}

	groupNum := blockCollectionSize / groupSize
	if blockCollectionSize%groupSize != 0 {
		groupNum += 1
	}

	startFid := 0
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
		checkSum := api.CheckSum{Hash: hash, StartFid: startFid, EndFid: endFid, BlockCount: endIndex - startIndex}

		rsp.CheckSums = append(rsp.CheckSums, checkSum)

		startFid = endFid + 1

	}

	return rsp, nil
}

func (dataSync *DataSync) getCheckSumsInRange(ctx context.Context, req api.ReqCheckSumInRange) (api.CheckSumRsp, error) {
	rsp := api.CheckSumRsp{CheckSums: make([]api.CheckSum, 0)}

	startFid := req.StartFid
	endFid := req.EndFid

	type block struct {
		fid int
		cid string
	}

	var blockCollection = make([]block, 0, 10000)

	for i := startFid; i <= endFid; i++ {
		fid := fmt.Sprintf("%d", i)
		cid, err := dataSync.block.GetCID(context.TODO(), fid)
		if err == datastore.ErrNotFound {
			continue
		}

		block := block{fid: i, cid: cid}
		blockCollection = append(blockCollection, block)
	}

	blockCollectionSize := len(blockCollection)
	// use req endFid return to scheduler
	blockCollection[blockCollectionSize-1].fid = req.EndFid

	groupSize := blockCollectionSize / req.MaxGroupNum
	if blockCollectionSize%req.MaxGroupNum != 0 {
		groupSize += 1
	}

	groupNum := blockCollectionSize / groupSize
	if blockCollectionSize%groupSize != 0 {
		groupNum += 1
	}

	// startFid := req.StartFid
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
		checkSum := api.CheckSum{Hash: hash, StartFid: startFid, EndFid: endFid, BlockCount: endIndex - startIndex}
		rsp.CheckSums = append(rsp.CheckSums, checkSum)

		startFid = endFid + 1

	}

	return rsp, nil
}

func (dataSync *DataSync) scrubBlocks(scrub api.ScrubBlocks) error {
	startFid := scrub.StartFid
	endFid := scrub.EndFid

	need2DeleteBlocks := make([]string, 0)
	blocks := scrub.Blocks
	for i := startFid; i <= endFid; i++ {
		fid := fmt.Sprintf("%d", i)
		cid, err := dataSync.block.GetCID(context.TODO(), fid)
		if err == datastore.ErrNotFound {
			continue
		}

		targetCid, ok := blocks[fid]
		if ok {
			if cid != targetCid {
				log.Errorf("scrubBlocks fid %s, local cid is %s but sheduler cid is %s", cid, targetCid)
				return fmt.Errorf("")
			}
			delete(blocks, fid)
		} else {
			need2DeleteBlocks = append(need2DeleteBlocks, cid)
		}
	}

	// delete blocks that not exist on scheduler
	if len(need2DeleteBlocks) > 0 {
		dataSync.block.DeleteBlocks(context.TODO(), need2DeleteBlocks)
	}

	if len(blocks) > 0 {
		// TODO: download block that not exist in local
		// blocks is need to download
	}

	log.Infof("scrubBlocks need to delete blocks %d, need to download blocks %d", len(need2DeleteBlocks), len(blocks))
	return nil
}

func string2Hash(value string) string {
	hasher := md5.New()
	hasher.Write([]byte(value))
	hash := hasher.Sum(nil)
	return hex.EncodeToString(hash)
}
