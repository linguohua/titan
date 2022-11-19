package scheduler

import (
	"context"
	"crypto/md5"
	"encoding/hex"

	"github.com/linguohua/titan/api"
)

const (
	maxGroupNum        = 10
	maxNumOfScrubBlock = 1000
)

type blockItem struct {
	fid int
	cid string
}

type inconformityBlocks struct {
	blocks   []*blockItem
	startFid int
	endFid   int
}

func doDataSync(syncApi api.DataSync, deviceID string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rsp, err := syncApi.GetAllChecksums(ctx, maxGroupNum)
	if err != nil {
		log.Errorf("doDataSync error:%s", err.Error())
	}

	inconformityBlocksList := make([]*inconformityBlocks, 0)
	for _, checksum := range rsp.CheckSums {
		blockItems := loadBlockItemsFromDB(deviceID, checksum.StartFid, checksum.EndFid)
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
			log.Errorf("getInconformityBlocksList error:%s", err.Error())
			return
		}

		inconformityBlocksList = append(inconformityBlocksList, blocksList...)
	}

	for _, inconfBlocks := range inconformityBlocksList {
		req := api.ScrubBlocks{StartFid: inconfBlocks.startFid, EndFid: inconfBlocks.endFid}
		err = syncApi.ScrubBlocks(ctx, req)
		if err != nil {
			log.Errorf("doDataSync scrub blocks, fid range %d ~ %d error:%s", inconfBlocks.startFid, inconfBlocks.endFid, err)
			continue
		}

		log.Infof("doDataSync scrub inconformity blocks, fid range %d ~ %d", inconfBlocks.startFid, inconfBlocks.endFid)
	}

	// 注意:sheduler的最大fid有可能大于设备的最大fid
}

func getInconformityBlocksList(syncApi api.DataSync, inconfBlocks *inconformityBlocks) ([]*inconformityBlocks, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req := api.ReqCheckSumInRange{StartFid: inconfBlocks.startFid, EndFid: inconfBlocks.endFid, MaxGroupNum: maxGroupNum}
	rsp, err := syncApi.GetChecksumsInRange(ctx, req)
	if err != nil {
		return []*inconformityBlocks{}, err
	}

	inconformityBlocksList := make([]*inconformityBlocks, 0)
	for _, checksum := range rsp.CheckSums {
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

// TODO: change to map
func loadBlockItemsFromDB(deviceID string, startFid, endFid int) []*blockItem {
	// TODO get block cid from database
	return []*blockItem{}
}

func getBlockItemsHash(blocks []*blockItem) string {
	if len(blocks) == 0 {
		return ""
	}

	var cidCollection string
	for _, block := range blocks {
		cidCollection += block.cid
	}

	return string2Hash(cidCollection)
}

func string2Hash(value string) string {
	hasher := md5.New()
	hasher.Write([]byte(value))
	hash := hasher.Sum(nil)
	return hex.EncodeToString(hash)
}
