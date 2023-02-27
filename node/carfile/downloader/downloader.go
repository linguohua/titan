package downloader

import (
	blocks "github.com/ipfs/go-block-format"
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
