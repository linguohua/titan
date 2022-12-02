package scheduler

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"sort"

	"github.com/ipfs/go-cid"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
)

const (
	maxGroupNum        = 10
	maxNumOfScrubBlock = 1000
	maxRow             = 10000
)

type blockItem struct {
	fid int
	cid string
}

type inconformityBlocks struct {
	blocks []*blockItem
	// startFid is not same as block first item
	startFid int
	// endFid is not same as block last item
	endFid int
}

func doDataSync(syncApi api.DataSync, deviceID string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rsp, err := syncApi.GetAllChecksums(ctx, maxGroupNum)
	if err != nil {
		log.Errorf("doDataSync error:%s", err.Error())
	}

	inconformityBlocksList := make([]*inconformityBlocks, 0)
	for _, checksum := range rsp.Checksums {
		blockItems, err := loadBlockItemsFromDB(deviceID, checksum.StartFid, checksum.EndFid)
		if err != nil {
			log.Errorf("doDataSync loadBlockItemsFromDB, deviceID:%s, range %d ~ %d, error:%s", deviceID, checksum.StartFid, checksum.EndFid, err.Error())
			return
		}

		hash := getBlockItemsHash(blockItems)
		if hash == checksum.Hash {
			continue
		}

		inconfBlocks := &inconformityBlocks{blocks: blockItems, startFid: checksum.StartFid, endFid: checksum.EndFid}
		if checksum.BlockCount <= maxNumOfScrubBlock {
			inconformityBlocksList = append(inconformityBlocksList, inconfBlocks)
			continue
		}

		blocksList, err := getInconformityBlocksList(syncApi, inconfBlocks)
		if err != nil {
			log.Errorf("doDataSync getInconformityBlocksList, deviceID:%s, range %d ~ %d, error:%s", deviceID, checksum.StartFid, checksum.EndFid, err.Error())
			return
		}

		inconformityBlocksList = append(inconformityBlocksList, blocksList...)
	}

	for _, inconfBlocks := range inconformityBlocksList {
		err = scrubBlocks(syncApi, inconfBlocks.startFid, inconfBlocks.endFid, inconfBlocks.blocks)
		if err != nil {
			log.Errorf("doDataSync scrubBlocks, deviceID:%s fid range %d ~ %d, error:%s", deviceID, inconfBlocks.startFid, inconfBlocks.endFid, err.Error())
		}

		log.Infof("doDataSync scrub inconformity blocks, deviceID:%s fid range %d ~ %d", deviceID, inconfBlocks.startFid, inconfBlocks.endFid)
	}

	var endFid = 0
	if len(rsp.Checksums) > 0 {
		lasChecksum := rsp.Checksums[len(rsp.Checksums)-1]
		endFid = lasChecksum.EndFid

	}
	blocks, err := loadBlockItemsBiggerThan(endFid, deviceID)
	if err != nil {
		log.Errorf("doDataSync loadBlockItemsBiggerThan, deviceID:%s, startFid:%d , error:%s", deviceID, endFid, err.Error())
		return
	}
	log.Infof("loadBlockItemsBiggerThan fid:%d, blocks len:%d", endFid, len(blocks))
	if len(blocks) > 0 {
		startBlock := blocks[0]
		endBlock := blocks[len(blocks)-1]
		err = scrubBlocks(syncApi, startBlock.fid, endBlock.fid, blocks)
		if err != nil {
			log.Errorf("doDataSync scrub blocks, deviceID:%s fid range %d ~ %d error:%s", deviceID, startBlock.fid, endBlock.fid, err)
		}
	}

}

func scrubBlocks(syncApi api.DataSync, startFid, endFid int, blocks []*blockItem) error {
	// TODO: do in batches
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	blockMap := make(map[int]string)

	for _, block := range blocks {
		blockMap[block.fid] = block.cid
	}

	req := api.ScrubBlocks{StartFid: startFid, EndFid: endFid, Blocks: blockMap}
	return syncApi.ScrubBlocks(ctx, req)

}

func getInconformityBlocksList(syncApi api.DataSync, inconfBlocks *inconformityBlocks) ([]*inconformityBlocks, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req := api.ReqChecksumInRange{StartFid: inconfBlocks.startFid, EndFid: inconfBlocks.endFid, MaxGroupNum: maxGroupNum}
	rsp, err := syncApi.GetChecksumsInRange(ctx, req)
	if err != nil {
		return []*inconformityBlocks{}, err
	}

	inconformityBlocksList := make([]*inconformityBlocks, 0)
	for _, checksum := range rsp.Checksums {
		blocks := getBlockItemWith(checksum.StartFid, checksum.EndFid, inconfBlocks.blocks)
		hash := getBlockItemsHash(blocks)
		if checksum.Hash == hash {
			continue
		}

		inconfBlocks := &inconformityBlocks{blocks: blocks, startFid: checksum.StartFid, endFid: checksum.EndFid}
		if checksum.BlockCount <= maxNumOfScrubBlock {
			inconformityBlocksList = append(inconformityBlocksList, inconfBlocks)
			continue
		}

		blocksList, err := getInconformityBlocksList(syncApi, inconfBlocks)
		if err != nil {
			return inconformityBlocksList, err
		}
		inconformityBlocksList = append(inconformityBlocksList, blocksList...)
	}

	return inconformityBlocksList, nil

}

func getBlockItemWith(startFid, endFid int, blocks []*blockItem) []*blockItem {
	blockItems := make([]*blockItem, 0)
	for _, block := range blocks {
		if block.fid >= startFid && block.fid <= endFid {
			blockItems = append(blockItems, block)
		}

		if block.fid > endFid {
			break
		}
	}
	return blockItems
}

func loadBlockItemsFromDB(deviceID string, startFid, endFid int) ([]*blockItem, error) {
	if endFid < startFid {
		log.Errorf("loadBlockItemsFromDB")
		return []*blockItem{}, fmt.Errorf("error param endFid < startFid, startFid:%d,, endFid:%d", startFid, endFid)
	}
	// TODO: get in batches
	result := make([]*blockItem, 0)
	cidMap, err := persistent.GetDB().GetBlocksInRange(startFid, endFid, deviceID)
	if err != nil {
		return result, err
	}

	for fid, cid := range cidMap {
		block := &blockItem{fid: fid, cid: cid}
		result = append(result, block)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].fid < result[j].fid
	})

	return result, nil
}

func loadBlockItemsBiggerThan(startFid int, deviceID string) ([]*blockItem, error) {
	result := make([]*blockItem, 0)
	cidMap, err := persistent.GetDB().GetBlocksBiggerThan(startFid, deviceID)
	if err != nil {
		return result, err
	}

	for fid, cid := range cidMap {
		block := &blockItem{fid: fid, cid: cid}
		result = append(result, block)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].fid < result[j].fid
	})

	return result, nil

}

func getBlockItemsHash(blocks []*blockItem) string {
	if len(blocks) == 0 {
		return ""
	}

	var hashCollection string
	for _, block := range blocks {
		hash, _ := cidString2HashString(block.cid)
		hashCollection += hash
	}

	return string2Hash(hashCollection)
}

func string2Hash(value string) string {
	hasher := md5.New()
	hasher.Write([]byte(value))
	hash := hasher.Sum(nil)
	return hex.EncodeToString(hash)
}

func cidString2HashString(cidString string) (string, error) {
	cid, err := cid.Decode(cidString)
	if err != nil {
		return "", err
	}

	return cid.Hash().String(), nil
}
