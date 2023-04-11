package fetcher

import (
	"context"

	"github.com/ipfs/go-libipfs/blocks"
	"github.com/linguohua/titan/api/types"
)

// BlockFetcher is an interface for fetching blocks from remote sources
type BlockFetcher interface {
	// FetchBlocks retrieves blocks with the given cids from remote sources using the provided CandidateDownloadInfo
	Fetch(ctx context.Context, cids []string, dss []*types.CandidateDownloadInfo) ([]blocks.Block, error)
}
