package downloader

import (
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api"
)

var log = logging.Logger("downloader")

const (
	blockDownloadTimeout  = 15
	blockDownloadRetryNum = 1
)

type BlockDownloader interface {
	// download blocks
	DownloadBlocks(cids []string, sources []*api.DowloadSource) ([]blocks.Block, error)
}

func newBlock(cidStr string, data []byte) (blocks.Block, error) {
	cid, err := cid.Decode(cidStr)
	if err != nil {
		return nil, err
	}

	basicBlock, err := blocks.NewBlockWithCid(data, cid)
	if err != nil {
		return nil, err
	}

	return basicBlock, nil
}
