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

type DownloadBlockser interface {
	// download blocks
	DownloadBlocks(cids []string, sources []*api.DownloadSource) ([]blocks.Block, error)
}
