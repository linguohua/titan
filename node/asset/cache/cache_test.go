package cache

import (
	"testing"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/node/asset/fetcher"
	"github.com/linguohua/titan/node/asset/storage"
)

func TestCache(t *testing.T) {
	t.Log("TestCache")

	_ = logging.SetLogLevel("/carfile/cache", "DEBUG")
	_ = logging.SetLogLevel("datastore", "DEBUG")
	_ = logging.SetLogLevel("dagstore/upgrade", "DEBUG")

	cidStr := "QmTcAg1KeDYJFpTJh3rkZGLhnnVKeXWNtjwPufjVvwPTpG"
	c, err := cid.Decode(cidStr)
	if err != nil {
		t.Errorf("Decode err:%s", err)
		return
	}

	manger, err := storage.NewManager("./test", nil)
	if err != nil {
		t.Errorf("NewCarfileStore err:%s", err)
		return
	}

	carCache := newCarfileCache(&options{root: c, dss: nil, storage: manger, bFetcher: fetcher.NewIPFS("http://192.168.0.132:5001", 15, 1), batch: 5})

	err = carCache.downloadCar()
	if err != nil {
		t.Errorf("downloadCar, error:%s", err)
		return
	}
}