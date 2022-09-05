package base

import (
	"context"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/block"
	"github.com/linguohua/titan/node/download"
)

var log = logging.Logger("base")

type Base struct {
	block         *block.Block
	blockDownload *download.BlockDownload
}

func NewBase(block *block.Block, blockDownload *download.BlockDownload) api.Base {
	return &Base{block: block, blockDownload: blockDownload}
}

func (base *Base) WaitQuiet(ctx context.Context) error {
	log.Info("WaitQuiet")
	return nil
}

func (base *Base) CacheBlocks(ctx context.Context, req api.ReqCacheData) error {
	log.Infof("CacheData, req len:%d", len(req.Cids))
	return base.block.OnCacheBlockReq(req)
}

func (base *Base) BlockStoreStat(ctx context.Context) error {
	log.Info("BlockStoreStat")

	return nil
}

func (base *Base) LoadData(ctx context.Context, cid string) ([]byte, error) {
	log.Info("LoadData")
	return base.block.LoadBlockWithCid(cid)
}

// call by scheduler
func (base *Base) DeleteData(ctx context.Context, cids []string) (api.DelResult, error) {
	log.Info("DeleteData")
	return base.block.DeleteData(ctx, cids)
}

// call by edge or candidate
func (base *Base) DeleteBlocks(ctx context.Context, cids []string) (api.DelResult, error) {
	log.Info("DeleteBlock")
	return base.block.DeleteBlocks(ctx, cids)
}

func (base *Base) QueryCacheStat(ctx context.Context) (api.CacheStat, error) {
	return base.block.QueryCacheStat()
}

func (base *Base) QueryCachingBlocks(ctx context.Context) (api.CachingBlockList, error) {
	result := api.CachingBlockList{}
	return result, nil
}

func (base *Base) SetDownloadSpeed(ctx context.Context, speedRate int64) error {
	log.Debug("set download speed %d", speedRate)
	return base.blockDownload.SetDownloadSpeed(speedRate)
}

func (base *Base) UnlimitDownloadSpeed(ctx context.Context) error {
	log.Debug("UnlimitDownloadSpeed")
	return base.blockDownload.UnlimitDownloadSpeed()
}

func (base *Base) GenerateDownloadToken(ctx context.Context) (string, error) {
	log.Debug("GenerateDownloadToken")
	return base.blockDownload.GenerateDownloadToken()
}
