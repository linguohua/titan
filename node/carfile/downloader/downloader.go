package downloader

import (
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api"
)

var log = logging.Logger("downloader")

type BlockDownloader interface {
	// scheduler request cache carfile
	DownloadBlocks(cids []string, sources []*api.DowloadSource) ([]blocks.Block, error)
	// // local sync miss data
	// syncData(block *Block, reqs map[int]string) error
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
