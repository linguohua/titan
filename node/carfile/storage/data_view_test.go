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
	ds, err := newKVstore("./test")
	if err != nil {
		t.Errorf("new kv store error:%s", err.Error())
		return
	}

	bucket := &bucket{ds: ds, size: 100}

	cidStr := "QmTcAg1KeDYJFpTJh3rkZGLhnnVKeXWNtjwPufjVvwPTpG"
	c1, err := cid.Decode(cidStr)
	if err != nil {
		t.Errorf("Decode cid error:%s", err.Error())
		return
	}

	bucket.addCar(context.Background(), c1)

	cidStr = "QmUuNfFwuRrxbRFt5ze3EhuQgkGnutwZtsYMbAcYbtb6j3"
	c2, err := cid.Decode(cidStr)
	if err != nil {
		t.Errorf("Decode cid error:%s", err.Error())
		return
	}

	err = bucket.addCar(context.Background(), c2)
	if err != nil {
		t.Errorf("put error:%s", err.Error())
		return
	}

	index := bucket.bucketID(c1)
	cars, err := bucket.getCars(context.Background(), uint32(index))
	if err != nil {
		t.Errorf("put error:%s", err.Error())
		return
	}

	t.Logf("index:%d", index)

	for _, car := range cars {
		t.Logf("mh:%s", car.String())
	}

	index = bucket.bucketID(c2)
	cars, err = bucket.getCars(context.Background(), uint32(index))
	if err != nil {
		t.Errorf("put error:%s", err.Error())
		return
	}

	t.Logf("index:%d", index)

	for _, car := range cars {
		t.Logf("mh:%s", car.String())
	}
}
