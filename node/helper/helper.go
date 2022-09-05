package helper

import (
	"fmt"

	"github.com/ipfs/go-datastore"
)

const (
	Batch       = 10
	MaxReqCount = 5
	// Millisecond
	LoadBockTick = 10

	KeyFidPrefix    = "fid-"
	KeyCidPrefix    = "cid-"
	DownloadSrvPath = "/block/get"
)

func NewKeyFID(fid string) datastore.Key {
	key := fmt.Sprintf("%s%s", KeyFidPrefix, fid)
	return datastore.NewKey(key)
}

func NewKeyCID(cid string) datastore.Key {
	key := fmt.Sprintf("%s%s", KeyCidPrefix, cid)
	return datastore.NewKey(key)
}
