package sync

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"sort"
	"strconv"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/carfile/carfilestore"
	"github.com/linguohua/titan/node/helper"
)

var log = logging.Logger("datasync")

type DataSync struct {
	// block *block.Block
	carfileStore *carfilestore.CarfileStore
	ds           datastore.Batching
}

func NewDataSync(ds datastore.Batching) *DataSync {
	return &DataSync{ds: ds}
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

	return api.ChecksumRsp{}, nil
}

func (dataSync *DataSync) scrubBlocks(scrub api.ScrubBlocks) error {

	return nil
}

func string2Hash(value string) string {
	hasher := md5.New()
	hasher.Write([]byte(value))
	hash := hasher.Sum(nil)
	return hex.EncodeToString(hash)
}

func SyncLocalBlockstore(ds datastore.Batching, carfileStore *carfilestore.CarfileStore) error {
	log.Info("start sync local block store")

	return nil
}
