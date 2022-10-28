package helper

import (
	"fmt"
	"time"

	"github.com/ipfs/go-datastore"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/blockstore"
)

const (
	Batch       = 5
	MaxReqCount = 5
	// Millisecond
	LoadBockTick = 10
	// validate timeout
	ValidateTimeout = 5

	DownloadSrvPath          = "/block/get"
	DownloadTokenExpireAfter = 24 * time.Hour

	KeyFidPrefix     = "fid/"
	KeyCidPrefix     = "cid/"
	TcpPackMaxLength = 52428800
)

type NodeParams struct {
	DS         datastore.Batching
	Scheduler  api.Scheduler
	BlockStore blockstore.BlockStore
	// Device          *device.Device
	DownloadSrvKey  string
	DownloadSrvAddr string
}

func NewKeyFID(fid string) datastore.Key {
	key := fmt.Sprintf("%s%s", KeyFidPrefix, fid)
	return datastore.NewKey(key)
}

func NewKeyCID(cid string) datastore.Key {
	key := fmt.Sprintf("%s%s", KeyCidPrefix, cid)
	return datastore.NewKey(key)
}
