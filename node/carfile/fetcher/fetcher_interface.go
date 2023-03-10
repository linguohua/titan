package fetcher

import (
	"context"

	blocks "github.com/ipfs/go-block-format"
	"github.com/linguohua/titan/api/types"
)

type BlockFetcher interface {
	// get block from remote
	Fetch(ctx context.Context, cids []string, dss []*types.DownloadSource) ([]blocks.Block, error)
}
