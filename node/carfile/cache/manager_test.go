package cache

import (
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api/types"
	"github.com/linguohua/titan/node/carfile/fetcher"
	"github.com/linguohua/titan/node/carfile/storage"
)

type TestCachedResultImpl struct {
	t       *testing.T
	storage storage.Storage
}

func (t *TestCachedResultImpl) CacheResult(result *types.PullResult) error {
	t.t.Logf("result:%#v", *result)
	return nil
}

func TestManager(t *testing.T) {
	t.Log("TestManager")
	_ = logging.SetLogLevel("/carfile/cache", "DEBUG")

	cidStr := "QmTcAg1KeDYJFpTJh3rkZGLhnnVKeXWNtjwPufjVvwPTpG"
	c, err := cid.Decode(cidStr)
	if err != nil {
		t.Errorf("Decode err:%s", err)
		return
	}

	storageMgr, err := storage.NewManager("./test", nil)
	if err != nil {
		t.Errorf("NewCarfileStore err:%s", err)
		return
	}

	bFetcher := fetcher.NewIPFS("http://192.168.0.132:5001", 15, 1)
	opts := &ManagerOptions{Storage: storageMgr, BFetcher: bFetcher, DownloadBatch: 5}

	mgr, err := NewManager(opts)
	if err != nil {
		t.Errorf("new manager err:%s", err)
		return
	}

	mgr.AddToWaitList(c, nil)

	time.Sleep(1 * time.Minute)
}
