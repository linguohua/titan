package cache

import (
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api/types"
	"github.com/linguohua/titan/node/carfile/fetcher"
	"github.com/linguohua/titan/node/carfile/store"
)

type TestCachedResultImpl struct {
	t        *testing.T
	carStore *store.CarfileStore
}

func (t *TestCachedResultImpl) CacheResult(result *types.CacheResult) error {
	t.t.Logf("result:%#v", *result)

	count, err := t.carStore.BlockCount()
	if err != nil {
		t.t.Errorf("get block count error:%s", err.Error())
		return err
	}

	t.t.Logf("total block %d in store", count)

	return nil
}

func TestManager(t *testing.T) {
	t.Log("TestManager")
	logging.SetLogLevel("/carfile/cache", "DEBUG")

	cidStr := "QmTcAg1KeDYJFpTJh3rkZGLhnnVKeXWNtjwPufjVvwPTpG"
	c, err := cid.Decode(cidStr)
	if err != nil {
		t.Errorf("Decode err:%s", err)
		return
	}

	carfileStore, err := store.NewCarfileStore("./test")
	if err != nil {
		t.Errorf("NewCarfileStore err:%s", err)
		return
	}

	bFetcher := fetcher.NewIPFS("http://192.168.0.132:5001", 15, 1)
	opts := &ManagerOptions{CarfileStore: carfileStore, BFetcher: bFetcher, DownloadBatch: 5}

	mgr := NewManager(opts)
	mgr.AddToWaitList(c, nil)

	time.Sleep(1 * time.Minute)

}
