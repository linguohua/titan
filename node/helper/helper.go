package helper

import (
	"fmt"
	"time"

	"github.com/ipfs/go-datastore"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/stores"
)

const (
	Batch       = 10
	MaxReqCount = 5
	// Millisecond
	LoadBockTick = 10
	// validate timeout
	ValidateTimeout = 5

	DownloadSrvPath          = "/block/get"
	DownloadTokenExpireAfter = 24 * time.Hour

	KeyFidPrefix = "fid/"
	KeyCidPrefix = "cid/"
)

type NodeParams struct {
	DS         datastore.Batching
	Scheduler  api.Scheduler
	BlockStore stores.BlockStore
	// Device          *device.Device
	DownloadSrvKey  string
	DownloadSrvAddr string
	IsExternal      bool
}

func NewKeyFID(fid string) datastore.Key {
	key := fmt.Sprintf("%s%s", KeyFidPrefix, fid)
	return datastore.NewKey(key)
}

func NewKeyCID(cid string) datastore.Key {
	key := fmt.Sprintf("%s%s", KeyCidPrefix, cid)
	return datastore.NewKey(key)
}
