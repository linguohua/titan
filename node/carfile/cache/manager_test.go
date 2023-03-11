package cache

import (
	"testing"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/node/carfile/fetcher"
	"github.com/linguohua/titan/node/carfile/store"
)

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

	bsrw, err := carfileStore.NewCarfileWriter(c)
	if err != nil {
		log.Errorf("NewCarfileWriter error:%s", err)
		return
	}

	carCache := newCarfileCache(&options{root: c, dss: nil, bsrw: bsrw, bFetcher: fetcher.NewIPFS("http://192.168.0.132:5001", 15, 1), batch: 5})

	err = carCache.downloadCar()
	if err != nil {
		log.Errorf("downloadCar, error:%s", err)
		return
	}
}
