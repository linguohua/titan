package api

import "context"

type DataSync interface {
	// get all block check sum
	GetAllCheckSums(ctx context.Context, maxGroupNum int) (CheckSumRsp, error) //perm:write
	// get block check sum in this range
	GetCheckSumsInRange(ctx context.Context, reqCheckSum ReqCheckSumInRange) (CheckSumRsp, error) //perm:write
	// scrub block that is repair blockstore
	ScrubBlocks(ctx context.Context, scrub ScrubBlocks) error //perm:write
}

type ScrubBlocks struct {
	// key fid, value cid
	// compare cid one by one
	Blocks   map[string]string
	StartFid int
	EndFid   int
}

type CheckSum struct {
	Hash       string
	StartFid   int
	EndFid     int
	BlockCount int
}

type CheckSumRsp struct {
	CheckSums []CheckSum
}

type ReqCheckSumInRange struct {
	// StartFID default 0
	StartFid int
	// EndFID, -1 is end of block fid
	EndFid int
	// rsp max group num
	MaxGroupNum int
}
