package store

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/filecoin-project/dagstore/shard"
	"github.com/ipfs/go-libipfs/blocks"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipfs/go-merkledag"
	"github.com/multiformats/go-multihash"
)

const (
	tmpDir = "./tmp"
)

func init() {
	_ = logging.SetLogLevel("dagstore", "DEBUG")
	_ = logging.SetLogLevel("dagstore/upgrader", "DEBUG")
}

func TestCarfilestore(t *testing.T) {
	t.Logf("TestCarfilestore")

}

func TestWriteBlock(t *testing.T) {
	t.Logf("TestWriteBlock")
	carsDirPath := filepath.Join(tmpDir, carsDir)
	err := os.MkdirAll(carsDirPath, 0o755)
	if err != nil {
		t.Errorf("new tmp dir error:%s", err.Error())
		return
	}

	cs, err := NewCarfileStore(tmpDir)
	if err != nil {
		t.Errorf("rew carfile store error:%s", err.Error())
		return
	}

	bk := newBlock()
	w, err := cs.NewCarfileWriter(bk.Cid())
	if err != nil {
		t.Errorf("rew carfile writer error:%s", err.Error())
		return
	}

	for i := 0; i < 100; i++ {
		w.Put(context.Background(), bk)
	}

	w.Finalize()
	err = cs.closeDagstore()
	if err != nil {
		t.Errorf("closeDagstore error:%s", err.Error())
		return
	}

	exist, err := cs.HashCarfile(bk.Cid())
	if err != nil {
		t.Errorf("check carfile exist error:%s", err.Error())
		return
	}

	t.Logf("exist:%v", exist)
}

func newBlock() blocks.Block {
	return merkledag.NewRawNode([]byte("1234567890")).Block
}

func TestRegisterShard(t *testing.T) {
	t.Logf("TestRegisterShard")
	carsDirPath := filepath.Join(tmpDir, carsDir)
	err := os.MkdirAll(carsDirPath, 0o755)
	if err != nil {
		t.Errorf("new tmp dir error:%s", err.Error())
		return
	}

	cs, err := NewCarfileStore(tmpDir)
	if err != nil {
		t.Errorf("rew carfile store error:%s", err.Error())
		return
	}

	bk := newBlock()
	w, err := cs.NewCarfileWriter(bk.Cid())
	if err != nil {
		t.Errorf("rew carfile writer error:%s", err.Error())
		return
	}

	for i := 0; i < 100; i++ {
		w.Put(context.Background(), bk)
	}

	w.Finalize()

	cs.RegisterShared(bk.Cid())
	if err != nil {
		t.Errorf("register shared error:%s", err.Error())
		return
	}

	t.Logf("RegisterShared success")
}

func TestShardIndices(t *testing.T) {
	carsDirPath := filepath.Join(tmpDir, carsDir)
	err := os.MkdirAll(carsDirPath, 0o755)
	if err != nil {
		t.Errorf("new tmp dir error:%s", err.Error())
		return
	}

	cs, err := NewCarfileStore(tmpDir)
	if err != nil {
		t.Errorf("rew carfile store error:%s", err.Error())
		return
	}

	bk := newBlock()
	ii, err := cs.dagst.GetIterableIndex(shard.KeyFromString(bk.Cid().Hash().String()))
	if err != nil {
		t.Errorf("GetIterableIndexerror:%s", err.Error())
		return
	}

	ii.ForEach(func(m multihash.Multihash, u uint64) error {
		t.Logf("block :%s", m.String())
		return nil
	})
}

func TestGetBlock(t *testing.T) {
	t.Logf("TestGetBlock")

	cs, err := NewCarfileStore(tmpDir)
	if err != nil {
		t.Errorf("rew carfile store error:%s", err.Error())
		return
	}

	bk := newBlock()
	blk, err := cs.Block(bk.Cid())
	if err != nil {
		t.Errorf("get block error:%s", err.Error())
		return
	}

	t.Logf("get block %s size:%d", blk.Cid().String(), len(blk.RawData()))
}

func TestDeleteCarfile(t *testing.T) {
	t.Logf("TestGetBlock")

	cs, err := NewCarfileStore(tmpDir)
	if err != nil {
		t.Errorf("rew carfile store error:%s", err.Error())
		return
	}

	bk := newBlock()
	err = cs.DeleteCarfile(bk.Cid())
	if err != nil {
		t.Errorf("DeleteCarfile error:%s", err.Error())
		return
	}

	t.Logf("delete carfile success")
}

func TestGetShards(t *testing.T) {
	t.Logf("TestGetBlock")

	cs, err := NewCarfileStore(tmpDir)
	if err != nil {
		t.Errorf("rew carfile store error:%s", err.Error())
		return
	}

	infos := cs.dagst.AllShardsInfo()
	for k := range infos {
		t.Logf("shard:%s", k.String())
	}
}

func TestGetShard(t *testing.T) {
	t.Logf("TestGetBlock")

	cs, err := NewCarfileStore(tmpDir)
	if err != nil {
		t.Errorf("rew carfile store error:%s", err.Error())
		return
	}

	bk := newBlock()
	ks, err := cs.dagst.TopLevelIndex.GetShardsForMultihash(context.Background(), bk.Cid().Hash())
	if err != nil {
		t.Errorf("GetShardsForMultihash error:%s", err.Error())
		return
	}

	if len(ks) == 0 {
		t.Errorf("len(ks) == 0 ")
		return
	}

	t.Errorf("len(ks) == 0 ")

}

func TestBlocksOfCarfile(t *testing.T) {
	cs, err := NewCarfileStore(tmpDir)
	if err != nil {
		t.Errorf("rew carfile store error:%s", err.Error())
		return
	}

	bk := newBlock()
	cids, err := cs.BlocksOfCarfile(bk.Cid())
	if err != nil {
		t.Errorf("BlocksOfCarfile err:%s", err.Error())
		return
	}
	t.Logf("cids:%v", cids)

	count, err := cs.BlockCount()
	if err != nil {
		t.Errorf("BlockCount error:%s", err.Error())
		return
	}

	t.Logf("total block count:%v", count)

	count, err = cs.BlockCountOfCarfile(bk.Cid())
	if err != nil {
		t.Errorf("BlockCountOfCarfile err:%s", err.Error())
		return
	}

	t.Logf("block count of %s:%v", bk.Cid().String(), count)

	count, err = cs.CarfileCount()

	t.Logf("car count :%v", count)
}
