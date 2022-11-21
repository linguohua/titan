package api

import "context"

type DataSync interface {
	// get all block check sum
	GetAllChecksums(ctx context.Context, maxGroupNum int) (ChecksumRsp, error) //perm:write
	// get block check sum in this range
	GetChecksumsInRange(ctx context.Context, reqCheckSum ReqChecksumInRange) (ChecksumRsp, error) //perm:write
	// scrub block that is repair blockstore
	ScrubBlocks(ctx context.Context, scrub ScrubBlocks) error //perm:write
}

type ScrubBlocks struct {
	// key fid, value cid
	// compare cid one by one
	Blocks   map[int]string
	StartFid int
	EndFid   int
}

type Checksum struct {
	Hash       string
	StartFid   int
	EndFid     int
	BlockCount int
}

type ChecksumRsp struct {
	Checksums []Checksum
}

type ReqChecksumInRange struct {
	// StartFID default 0
	StartFid int
	// EndFID, -1 is end of block fid
	EndFid int
	// rsp max group num
	MaxGroupNum int
}
