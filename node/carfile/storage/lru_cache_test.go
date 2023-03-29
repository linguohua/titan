package storage

import (
	"context"
	"testing"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
)

func init() {
	_ = logging.SetLogLevel("carfile/store", "DEBUG")
}
func TestBlock(t *testing.T) {
	t.Logf("TestRegisterShard")
	block, err := NewLRUCache("C:/Users/aaa/.titanedge-1/carfilestore/cars", 1)
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

	blk, err := block.GetBlock(context.Background(), root, root)
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
	blk, err = block.GetBlock(context.Background(), root, root)
	if err != nil {
		t.Errorf("decode error:%s", err.Error())
		return
	}

	t.Logf("block size:%d", len(blk.RawData()))
}
