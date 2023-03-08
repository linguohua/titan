package downloader

import (
	blocks "github.com/ipfs/go-block-format"
	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api/types"
)

var log = logging.Logger("downloader")

const (
	timeout    = 15
	retryCount = 1
)

type DownloadBlockser interface {
	// download blocks
	DownloadBlocks(cids []string, dss []*types.DownloadSource) ([]blocks.Block, error)
}
