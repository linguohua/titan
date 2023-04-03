package gateway

import (
	"context"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/linguohua/titan/node/carfile/cache"
	"github.com/linguohua/titan/node/carfile/fetcher"
	"github.com/linguohua/titan/node/carfile/storage"
)

func TestGateway(t *testing.T) {
	t.Log("TestGateway")
}

func TestResolvePath(t *testing.T) {
	t.Log("TestResolvePath")

	storePath := "C:/Users/aaa/.titanedge-1/carfilestore"
	p := "/ipfs/QmNXoAB3ZNoFQQZMGk4utybuvABdLTz6hcVmtHnV4FUp3S/log"
	storageMgr, err := storage.NewManager(storePath, nil)
	if err != nil {
		t.Errorf("NewCarfileStore err:%s", err)
		return
	}

	car, err := cid.Decode(p)
	if err != nil {
		t.Errorf("Decode err:%s", err)
		return
	}

	bFetcher := fetcher.NewIPFS("http://192.168.0.132:5001", 15, 1)
	opts := &cache.ManagerOptions{Storage: storageMgr, BFetcher: bFetcher, DownloadBatch: 5}

	mgr, err := cache.NewManager(opts)
	if err != nil {
		t.Errorf("TestResolvePath error:%s", err.Error())
		return
	}

	gw := &Gateway{storage: mgr}

	resolvePath, err := gw.resolvePath(context.Background(), path.New(p), car)
	if err != nil {
		t.Errorf("TestResolvePath error:%s", err.Error())
		return
	}

	t.Logf("root: %s, cid: %s, rest:%v", resolvePath.Root().String(), resolvePath.Cid().String(), resolvePath.Remainder())
}
