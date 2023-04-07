package cache

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-car/v2/index"
	titanindex "github.com/linguohua/titan/node/asset/index"
	"github.com/linguohua/titan/node/asset/storage"
	"github.com/multiformats/go-multihash"
)

func init() {
	_ = logging.SetLogLevel("carfile/store", "DEBUG")
	_ = logging.SetLogLevel("carfile/cache", "DEBUG")
}
func TestLRUCache(t *testing.T) {
	t.Logf("TestLRUCache")
	storageMgr, err := storage.NewManager("C:/Users/aaa/.titancandidate-1/storage/", nil)
	if err != nil {
		t.Errorf("new manager err:%s", err.Error())
		return
	}
	cache, err := newLRUCache(storageMgr, 1)
	if err != nil {
		t.Errorf("new block error:%s", err.Error())
		return
	}

	cidStr := "bafkreib5rfwmim6vvuf76fi3uiqbenadoexqvq5vf64a776tybqcpkes4q"
	root, err := cid.Decode(cidStr)
	if err != nil {
		t.Errorf("decode error:%s", err.Error())
		return
	}

	t.Logf("hash %s", root.Hash().String())
	blk, err := cache.getBlock(context.Background(), root, root)
	if err != nil {
		t.Errorf("get block error:%s", err.Error())
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

	t.Logf("record count:%d", len(records))

	multiIndexSorted := titanindex.NewMultiIndexSorted(128)
	err = multiIndexSorted.Load(records)
	if err != nil {
		t.Errorf("multiIndexSorted load error:%s", err.Error())
		return
	}

	var buffer bytes.Buffer
	n, err := multiIndexSorted.Marshal(&buffer)
	if err != nil {
		t.Errorf("marsahl error:%s, n:%d", err.Error(), n)
		return
	}

	newIndex := titanindex.NewMultiIndexSorted(0)
	err = newIndex.Unmarshal(&buffer)
	if err != nil {
		t.Errorf("Unmarshal error:%s, n:%d", err.Error(), n)
		return
	}

	t.Logf("bucket size:%d, record count:%d", newIndex.BucketSize(), newIndex.RecordCount())

	size := newIndex.BucketSize()
	rcs, err := newIndex.GetBucket(12345 % size)
	if err != nil {
		t.Errorf("GetBucket error:%s", err.Error())
		return
	}

	for _, record := range rcs {
		t.Logf("record: %s", record.String())
	}

}
