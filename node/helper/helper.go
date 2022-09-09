package helper

import "time"

const (
	Batch       = 10
	MaxReqCount = 5
	// Millisecond
	LoadBockTick = 10
	// validate timeout
	ValidateTimeout = 5

	DownloadSrvPath          = "/block/get"
	DownloadTokenExpireAfter = 24 * time.Hour
)
