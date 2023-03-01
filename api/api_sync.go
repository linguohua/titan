package api

import "context"

type DataSync interface {
	CompareChecksum(ctx context.Context, succeededCarfilesChecksum, unsucceededCarfilesChecksum string) (*CompareResult, error) //perm:write

	// BeginCheckCarfiles, PrepareCarfiles and DoCheckCarfiles is transaction
	BeginCheckCarfiles(ctx context.Context) error
	// send carfile list to node in multiple times
	PrepareCarfiles(ctx context.Context, carfileHashes []string) error //perm:write
	// DoCheckCarfiles, carfilesHash is checksum of carfiles
	DoCheckCarfiles(ctx context.Context, carfilesChecksum string, isSusseedCarfiles bool) error //perm:write
}

type CompareResult struct {
	IsSusseedCarfilesOk   bool
	IsUnsusseedCarfilesOk bool
}
