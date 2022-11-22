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
	"github.com/linguohua/titan/blockstore"
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

func (dataSync *DataSync) GetAllChecksums(ctx context.Context, maxGroupNum int) (api.ChecksumRsp, error) {
	return dataSync.getAllChecksums(ctx, maxGroupNum)
}
func (dataSync *DataSync) ScrubBlocks(ctx context.Context, scrub api.ScrubBlocks) error {
	return dataSync.scrubBlocks(scrub)
}

func (dataSync *DataSync) GetChecksumsInRange(ctx context.Context, req api.ReqChecksumInRange) (api.ChecksumRsp, error) {
	return dataSync.getChecksumsInRange(ctx, req)
}

func (dataSync *DataSync) getAllChecksums(ctx context.Context, maxGroupNum int) (api.ChecksumRsp, error) {
	if maxGroupNum == 0 {
		maxGroupNum = 10
	}
	rsp := api.ChecksumRsp{Checksums: make([]api.Checksum, 0)}
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

	if groupSize == 0 {
		groupSize = 1
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
		checksum := api.Checksum{Hash: hash, StartFid: startFid, EndFid: endFid, BlockCount: endIndex - startIndex}

		rsp.Checksums = append(rsp.Checksums, checksum)

		startFid = endFid + 1

	}

	return rsp, nil
}

func (dataSync *DataSync) getChecksumsInRange(ctx context.Context, req api.ReqChecksumInRange) (api.ChecksumRsp, error) {
	rsp := api.ChecksumRsp{Checksums: make([]api.Checksum, 0)}

	startFid := req.StartFid
	endFid := req.EndFid

	type block struct {
		fid int
		cid string
	}

	var blockCollection = make([]block, 0, 1000)

	for i := startFid; i <= endFid; i++ {
		fid := fmt.Sprintf("%d", i)
		cid, err := dataSync.block.GetCID(context.TODO(), fid)
		if err == datastore.ErrNotFound {
			continue
		}

		block := block{fid: i, cid: cid}
		blockCollection = append(blockCollection, block)
	}

	if len(blockCollection) == 0 {
		return rsp, nil
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
		checksum := api.Checksum{Hash: hash, StartFid: startFid, EndFid: endFid, BlockCount: endIndex - startIndex}
		rsp.Checksums = append(rsp.Checksums, checksum)

		startFid = endFid + 1

	}

	return rsp, nil
}

func (dataSync *DataSync) scrubBlocks(scrub api.ScrubBlocks) error {
	startFid := scrub.StartFid
	endFid := scrub.EndFid

	need2DeleteBlocks := make([]string, 0)
	blocks := scrub.Blocks
	for fid := startFid; fid <= endFid; fid++ {
		fidStr := fmt.Sprintf("%d", fid)
		cid, err := dataSync.block.GetCID(context.TODO(), fidStr)
		if err == datastore.ErrNotFound {
			continue
		}

		targetCid, ok := blocks[fid]
		if ok {
			if cid != targetCid {
				log.Errorf("scrubBlocks fid %s, local cid is %s but sheduler cid is %s", fid, cid, targetCid)
				need2DeleteBlocks = append(need2DeleteBlocks, cid)
			} else {
				delete(blocks, fid)
			}
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

	log.Infof("scrubBlocks, delete blocks %d, download blocks %d", len(need2DeleteBlocks), len(blocks))
	return nil
}

func string2Hash(value string) string {
	hasher := md5.New()
	hasher.Write([]byte(value))
	hash := hasher.Sum(nil)
	return hex.EncodeToString(hash)
}

func SyncLocalBlockstore(ds datastore.Batching, blockstore blockstore.BlockStore) error {
	log.Info("start sync local block store")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// ds := dataSync.block.GetDatastore(ctx)

	fidQuery := query.Query{Prefix: "fid"}
	results, err := ds.Query(ctx, fidQuery)
	if err != nil {
		log.Errorf("getCheckSums datastore query error:%s", err.Error())
		return err
	}

	targetCidMap := make(map[string]string)

	for {
		r, ok := results.NextSync()
		if !ok {
			break
		}

		fidStr := r.Key[len(helper.KeyFidPrefix)+1:]
		cid := string(r.Value)
		targetCidMap[cid] = fidStr
	}

	log.Info("start sync fid and cid")

	cidQuery := query.Query{Prefix: "cid"}
	results, err = ds.Query(ctx, cidQuery)
	if err != nil {
		log.Errorf("getCheckSums datastore query error:%s", err.Error())
		return err
	}

	cidMap := make(map[string]string)

	for {
		r, ok := results.NextSync()
		if !ok {
			break
		}

		cidStr := r.Key[len(helper.KeyCidPrefix)+1:]
		cidMap[cidStr] = string(r.Value)
	}

	for targetCid, targetFid := range targetCidMap {
		fid, ok := cidMap[targetCid]
		if ok {
			delete(cidMap, targetCid)
			if fid == targetFid {
				continue
			}

		}
		ds.Put(ctx, helper.NewKeyCID(targetCid), []byte(targetFid))
		log.Warnf("cid:fid %s ==> %s not match in target fid:cid %s ==> %s ", targetCid, fid, targetFid, targetCid)
	}

	for cid, fid := range cidMap {
		ds.Delete(ctx, helper.NewKeyCID(cid))
		log.Warnf("cid:fid %s ==> %s not in fid map, was delete", cid, fid)
	}

	cids, err := blockstore.GetAllKeys()
	if err != nil {
		return err
	}

	log.Info("start sync block")
	need2DeleteBlock := make([]string, 0)
	for _, cid := range cids {
		_, ok := targetCidMap[cid]
		if !ok {
			need2DeleteBlock = append(need2DeleteBlock, cid)
		} else {
			delete(targetCidMap, cid)
		}

	}

	for _, cid := range need2DeleteBlock {
		blockstore.Delete(cid)
		log.Warnf("block %s not exist fid, was delete", cid)
	}

	// TODO: need to download targetCidMap block

	log.Info("sync local block store complete")

	return nil
}
