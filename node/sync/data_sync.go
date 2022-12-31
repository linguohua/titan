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
	ds    datastore.Batching
}

func NewDataSync(block *block.Block, ds datastore.Batching) *DataSync {
	return &DataSync{block: block, ds: ds}
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

	q := query.Query{Prefix: "fid"}
	results, err := dataSync.ds.Query(ctx, q)
	if err != nil {
		log.Errorf("getCheckSums datastore query error:%s", err.Error())
		return rsp, err
	}

	type block struct {
		fid  int
		hash string
	}

	blockCollection := make([]block, 0, 10000)

	for {
		r, exist := results.NextSync()
		if !exist {
			break
		}

		fidStr := r.Key[len(helper.KeyFidPrefix)+1:]
		fid, err := strconv.Atoi(fidStr)
		if err != nil {
			log.Errorf("scrubBlockStore error:%s", err.Error())
		}

		block := block{fid: fid, hash: string(r.Value)}
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
			blockCollectionStr += blockCollection[j].hash
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
		fid  int
		hash string
	}

	blockCollection := make([]block, 0, 1000)

	for i := startFid; i <= endFid; i++ {
		fid := fmt.Sprintf("%d", i)
		cid, err := dataSync.block.GetCID(context.TODO(), fid)
		if err == datastore.ErrNotFound {
			continue
		}

		hash, err := helper.CIDString2HashString(cid)
		if err != nil {
			continue
		}

		block := block{fid: i, hash: hash}
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
			blockCollectionStr += blockCollection[j].hash
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
	// blocks key fid, value hash
	blocksHashMap := make(map[int]string)
	for fid, cid := range scrub.Blocks {
		hash, err := helper.CIDString2HashString(cid)
		if err != nil {
			log.Errorf("scrubBlocks, CIDString2HashString error:%s, cid:%s", err.Error(), cid)
			continue
		}

		blocksHashMap[fid] = hash
	}

	startFid := scrub.StartFid
	endFid := scrub.EndFid
	need2DeleteBlocks := make([]string, 0)

	for fid := startFid; fid <= endFid; fid++ {
		fidStr := fmt.Sprintf("%d", fid)
		cid, err := dataSync.block.GetCID(context.TODO(), fidStr)
		if err == datastore.ErrNotFound {
			continue
		}

		if err != nil {
			log.Infof("get cid from fid error:%s, fid:%s", err.Error(), fidStr)
			continue
		}

		hash, err := helper.CIDString2HashString(cid)
		if err != nil {
			log.Infof("CIDString2HashString error:%s, cid:%s", err.Error(), cid)
			continue
		}

		targetHash, exist := blocksHashMap[fid]
		if exist {
			if hash != targetHash {
				log.Errorf("scrubBlocks fid %s, local block hash is %s but sheduler block hash is %s", fid, hash, targetHash)
				need2DeleteBlocks = append(need2DeleteBlocks, cid)
			} else {
				delete(blocksHashMap, fid)
			}
		} else {
			need2DeleteBlocks = append(need2DeleteBlocks, cid)
		}
	}

	// delete blocks that not exist on scheduler
	if len(need2DeleteBlocks) > 0 {
		dataSync.block.DeleteBlocks(context.TODO(), need2DeleteBlocks)
	}

	blocksCIDMap := make(map[int]string)
	for fid, hash := range blocksHashMap {
		cid, err := helper.HashString2CidString(hash)
		if err != nil {
			continue
		}
		blocksCIDMap[fid] = cid
	}

	dataSync.block.SyncData(blocksCIDMap)

	log.Infof("scrubBlocks, fid %d~%d delete blocks %d, download blocks %d", startFid, endFid, len(need2DeleteBlocks), len(blocksCIDMap))
	return nil
}

func string2Hash(value string) string {
	hasher := md5.New()
	hasher.Write([]byte(value))
	hash := hasher.Sum(nil)
	return hex.EncodeToString(hash)
}

func SyncLocalBlockstore(ds datastore.Batching, blockstore blockstore.BlockStore, block *block.Block) error {
	log.Info("start sync local block store")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fidQuery := query.Query{Prefix: "fid"}
	results, err := ds.Query(ctx, fidQuery)
	if err != nil {
		log.Errorf("getCheckSums datastore query error:%s", err.Error())
		return err
	}
	// key hash, value fid
	targetMap := make(map[string]string)

	for {
		r, exist := results.NextSync()
		if !exist {
			break
		}

		fidStr := r.Key[len(helper.KeyFidPrefix)+1:]
		hash := string(r.Value)
		targetMap[hash] = fidStr
	}

	log.Info("start sync fid and cid")

	hashQuery := query.Query{Prefix: "hash"}
	results, err = ds.Query(ctx, hashQuery)
	if err != nil {
		log.Errorf("getCheckSums datastore query error:%s", err.Error())
		return err
	}

	hashMap := make(map[string]string)

	for {
		r, exist := results.NextSync()
		if !exist {
			break
		}

		hash := r.Key[len(helper.KeyCidPrefix)+1:]
		hashMap[hash] = string(r.Value)
	}

	for targetHash, targetFid := range targetMap {
		fid, exist := hashMap[targetHash]
		if exist {
			delete(hashMap, targetHash)
			if fid == targetFid {
				continue
			}

		}

		ds.Put(ctx, helper.NewKeyHash(targetHash), []byte(targetFid))

		log.Warnf("hash:fid %s ==> %s not match in target fid:hash %s ==> %s ", targetHash, fid, targetFid, targetHash)
	}

	for hash, fid := range hashMap {
		ds.Delete(ctx, helper.NewKeyHash(hash))
		log.Warnf("hash:fid %s ==> %s not in fid map, was delete", hash, fid)
	}

	hashs, err := blockstore.GetAllKeys()
	if err != nil {
		return err
	}

	log.Info("start sync block")
	need2DeleteBlockHashs := make([]string, 0)
	for _, hash := range hashs {
		_, exist := targetMap[hash]
		if !exist {
			need2DeleteBlockHashs = append(need2DeleteBlockHashs, hash)
		} else {
			delete(targetMap, hash)
		}

	}

	need2DeleteBlockCids := make([]string, 0)
	for _, hash := range need2DeleteBlockHashs {
		cid, err := helper.HashString2CidString(hash)
		if err != nil {
			continue
		}

		need2DeleteBlockCids = append(need2DeleteBlockCids, cid)
	}

	block.DeleteBlocks(context.Background(), need2DeleteBlockCids)

	blocksCIDMap := make(map[int]string)
	for hash, fidStr := range targetMap {
		fid, err := strconv.Atoi(fidStr)
		if err != nil {
			continue
		}
		cid, err := helper.HashString2CidString(hash)
		if err != nil {
			continue
		}
		blocksCIDMap[fid] = cid
	}

	block.SyncData(blocksCIDMap)

	log.Infof("sync local block store complete, delete blocks %d, download blocks %d", len(need2DeleteBlockCids), len(blocksCIDMap))

	return nil
}
