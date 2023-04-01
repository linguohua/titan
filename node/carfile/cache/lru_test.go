package cache

import (
	"context"
	"os"
	"testing"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-car/v2/index"
	"github.com/linguohua/titan/node/carfile/storage"
	"github.com/multiformats/go-multihash"
)

func init() {
	_ = logging.SetLogLevel("carfile/store", "DEBUG")
	_ = logging.SetLogLevel("carfile/cache", "DEBUG")
}
func TestLRUCache(t *testing.T) {
	t.Logf("TestLRUCache")
	storageMgr, err := storage.NewManager("C:/Users/aaa/.titanedge-1/carfilestore/cars", nil)
	if err != nil {
		t.Errorf("new manager err:%s", err.Error())
		return
	}
	cache, err := newLRUCache(storageMgr, 1)
	if err != nil {
		t.Errorf("new block error:%s", err.Error())
		return
	}

	cidStr := "QmUuNfFwuRrxbRFt5ze3EhuQgkGnutwZtsYMbAcYbtb6j3"
	root, err := cid.Decode(cidStr)
	if err != nil {
		t.Errorf("decode error:%s", err.Error())
		return
	}

	blk, err := cache.getBlock(context.Background(), root, root)
	if err != nil {
		t.Errorf("decode error:%s", err.Error())
		return
	}

	cidStr = "QmTcAg1KeDYJFpTJh3rkZGLhnnVKeXWNtjwPufjVvwPTpG"
	root, err = cid.Decode(cidStr)
	if err != nil {
		t.Errorf("decode error:%s", err.Error())
		return
	}

	t.Logf("block size:%d", len(blk.RawData()))
	blk, err = cache.getBlock(context.Background(), root, root)
	if err != nil {
		t.Errorf("decode error:%s", err.Error())
		return
	}

	t.Logf("block size:%d", len(blk.RawData()))
}

func TestIndex(t *testing.T) {
	t.Logf("TestIndex")

	storageMgr, err := storage.NewManager("C:/Users/aaa/.titanedge-1/carfilestore", nil)
	if err != nil {
		t.Errorf("new manager err:%s", err.Error())
		return
	}
	cache, err := newLRUCache(storageMgr, 1)
	if err != nil {
		t.Errorf("new block error:%s", err.Error())
		return
	}

	cidStr := "QmTcAg1KeDYJFpTJh3rkZGLhnnVKeXWNtjwPufjVvwPTpG"
	root, err := cid.Decode(cidStr)
	if err != nil {
		t.Errorf("decode error:%s", err.Error())
		return
	}

	reader, err := storageMgr.GetCar(root)
	if err != nil {
		t.Errorf("decode error:%s", err.Error())
		return
	}

	f, ok := reader.(*os.File)
	if !ok {
		t.Errorf("can not convert car %s reader to file", root.String())
		return
	}

	idx, err := cache.getCarIndex(f)
	if err != nil {
		t.Errorf("decode error:%s", err.Error())
		return
	}

	mhIdx, ok := idx.(*index.MultihashIndexSorted)
	if !ok {
		t.Errorf("can not convert index to MultihashIndexSorted")
		return
	}

	records := make([]index.Record, 0)
	mhIdx.ForEach(func(mh multihash.Multihash, offset uint64) error {
		c := cid.NewCidV1(cid.Raw, mh)
		records = append(records, index.Record{Cid: c, Offset: offset})

		return nil
	})

	index := index.NewMultihashSorted()
	index.Load(records)
	t.Logf("len:%d", len(records))

}
