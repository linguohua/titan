package edge

import (
	"context"
	"fmt"

	"github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
)

var (
	log = logging.Logger("edge")

	candidateApiMap = make(map[string]*candidateApi)
	batch           = 10
	reqList         []delayReq
	cachingList     []delayReq
	maxReqCount     = 5

	keyFidPrefix    = "fid-"
	keyCidPrefix    = "cid-"
	downloadSrvPath = "/rpc/v0/block/get"
)

func newKeyFID(fid string) datastore.Key {
	key := fmt.Sprintf("%s%s", keyFidPrefix, fid)
	return datastore.NewKey(key)
}

func newKeyCID(cid string) datastore.Key {
	key := fmt.Sprintf("%s%s", keyCidPrefix, cid)
	return datastore.NewKey(key)
}

func getCID(edge EdgeAPI, fid string) (string, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	value, err := edge.ds.Get(ctx, newKeyFID(fid))
	if err != nil {
		// log.Errorf("Get cid from store error:%v, fid:%s", err, fid)
		return "", err
	}

	return string(value), nil
}

func getFID(edge EdgeAPI, cid string) (string, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	value, err := edge.ds.Get(ctx, newKeyCID(cid))
	if err != nil {
		// log.Errorf("Get fid from store error:%v, cid:%s", err, cid)
		return "", err
	}

	return string(value), nil
}
