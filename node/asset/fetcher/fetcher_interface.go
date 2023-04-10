package fetcher

import (
	"context"

	"github.com/ipfs/go-libipfs/blocks"
	"github.com/linguohua/titan/api/types"
)

type BlockFetcher interface {
	// get block from remote
	Fetch(ctx context.Context, cids []string, dss []*types.CandidateDownloadInfo) ([]blocks.Block, error)
}
