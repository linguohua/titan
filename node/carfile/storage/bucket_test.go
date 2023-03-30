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
func TestBucket(t *testing.T) {
	bucket, err := newBucket("./test", 100)
	if err != nil {
		t.Errorf("new bucket error:%s", err.Error())
		return
	}

	cidStr := "QmTcAg1KeDYJFpTJh3rkZGLhnnVKeXWNtjwPufjVvwPTpG"
	c1, err := cid.Decode(cidStr)
	if err != nil {
		t.Errorf("Decode cid error:%s", err.Error())
		return
	}

	bucket.put(context.Background(), c1)

	cidStr = "QmUuNfFwuRrxbRFt5ze3EhuQgkGnutwZtsYMbAcYbtb6j3"
	c2, err := cid.Decode(cidStr)
	if err != nil {
		t.Errorf("Decode cid error:%s", err.Error())
		return
	}

	err = bucket.put(context.Background(), c2)
	if err != nil {
		t.Errorf("put error:%s", err.Error())
		return
	}

	index := bucket.index(c1.Hash())
	mhs, err := bucket.get(context.Background(), index)
	if err != nil {
		t.Errorf("put error:%s", err.Error())
		return
	}

	t.Logf("index:%d", index)

	for _, mh := range mhs {
		t.Logf("mh:%s", mh.String())
	}

	index = bucket.index(c2.Hash())
	mhs, err = bucket.get(context.Background(), index)
	if err != nil {
		t.Errorf("put error:%s", err.Error())
		return
	}

	t.Logf("index:%d", index)

	for _, mh := range mhs {
		t.Logf("mh:%s", mh.String())
	}
}
