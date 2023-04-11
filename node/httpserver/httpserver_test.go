package httpserver

import (
	"context"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/linguohua/titan/node/asset"
	"github.com/linguohua/titan/node/asset/fetcher"
	"github.com/linguohua/titan/node/asset/storage"
)

func TestGateway(t *testing.T) {
	t.Log("TestGateway")
}

func TestResolvePath(t *testing.T) {
	t.Log("TestResolvePath")

	storePath := "C:/Users/aaa/.titanedge-1/asset"
	p := "/ipfs/QmNXoAB3ZNoFQQZMGk4utybuvABdLTz6hcVmtHnV4FUp3S/log"
	storageMgr, err := storage.NewManager(storePath, nil)
	if err != nil {
		t.Errorf("NewManager err:%s", err)
		return
	}

	assetCID, err := cid.Decode(p)
	if err != nil {
		t.Errorf("Decode err:%s", err)
		return
	}

	bFetcher := fetcher.NewIPFSClient("http://192.168.0.132:5001", 15, 1)
	opts := &asset.ManagerOptions{Storage: storageMgr, BFetcher: bFetcher, PullParallel: 5}

	mgr, err := asset.NewManager(opts)
	if err != nil {
		t.Errorf("TestResolvePath error:%s", err.Error())
		return
	}

	hs := &HttpServer{asset: mgr}

	resolvePath, err := hs.resolvePath(context.Background(), path.New(p), assetCID)
	if err != nil {
		t.Errorf("TestResolvePath error:%s", err.Error())
		return
	}

	t.Logf("root: %s, cid: %s, rest:%v", resolvePath.Root().String(), resolvePath.Cid().String(), resolvePath.Remainder())
}
