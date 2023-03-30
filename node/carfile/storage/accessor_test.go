package storage

import (
	"testing"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-car/v2/index"
	"github.com/multiformats/go-multihash"
)

func init() {
	_ = logging.SetLogLevel("carfile/store", "DEBUG")
}
func TestAccessor(t *testing.T) {
	accessor, err := NewAccessor("C:/Users/aaa/.titanedge-1/carfilestore")
	if err != nil {
		t.Errorf("new accessor error:%s", err.Error())
		return
	}

	cidStr := "QmTcAg1KeDYJFpTJh3rkZGLhnnVKeXWNtjwPufjVvwPTpG"
	c, err := cid.Decode(cidStr)
	if err != nil {
		t.Errorf("decode error:%s", err.Error())
		return
	}

	idx, err := accessor.getCarIndex(c)
	if err != nil {
		t.Errorf("get car index error:%s", err.Error())
		return
	}

	iterableIdx, ok := idx.(index.IterableIndex)
	if !ok {
		t.Errorf("can not convert index IterableIndex")
		return
	}

	iterableIdx.ForEach(func(m multihash.Multihash, u uint64) error {
		t.Logf("mh: %s, offset %d", m.String(), u)
		return nil
	})
}
