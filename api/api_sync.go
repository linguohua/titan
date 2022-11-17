package api

import "context"

type DataSync interface {
	// hash to check block store data consistent
	GetCheckSums(ctx context.Context, req ReqCheckSum) (CheckSumRsp, error) //perm:read
	ScrubBlocks(ctx context.Context, scrub ScrubBlocks) error               //perm:write
}

type ScrubBlocks struct {
	// key fid, value cid
	// compare cid one by one
	Blocks   map[string]string
	StartFid string
	EndFix   string
}

type CheckSum struct {
	Hash     string
	StartFid string
	EndFid   string
}

type CheckSumRsp struct {
	CheckSums []CheckSum
}

type ReqCheckSum struct {
	// StartFID default 0
	StartFid string
	// EndFID, -1 is end of block fid
	EndFid string
	// block number in group
	GroupSize string
}
