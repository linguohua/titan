package api

import "context"

type DataSync interface {
	// simple to check carfile table
	CheckSummary(ctx context.Context, susseedCarfilesHash, failedCarfilesHash string) (*CheckSummaryResult, error) //perm:write
}

type CheckSummaryResult struct {
	IsSusseedCarfilesOk   bool
	IsUnsusseedCarfilesOk bool
}
