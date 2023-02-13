package sync

import (
	"context"
	"crypto/md5"
	"encoding/hex"

	"github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/carfile/carfilestore"
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
	//TODO: need implement
	return api.ChecksumRsp{}, nil
}

func (dataSync *DataSync) getChecksumsInRange(ctx context.Context, req api.ReqChecksumInRange) (api.ChecksumRsp, error) {
	//TODO: need implement
	return api.ChecksumRsp{}, nil
}

func (dataSync *DataSync) scrubBlocks(scrub api.ScrubBlocks) error {
	//TODO: need implement
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
