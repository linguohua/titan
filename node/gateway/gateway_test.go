package gateway

import (
	"context"
	"testing"

	logging "github.com/ipfs/go-log/v2"
	"github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/linguohua/titan/node/carfile/store"
)

func TestGateway(t *testing.T) {
	t.Log("TestGateway")
}

func TestResolvePath(t *testing.T) {
	t.Log("TestResolvePath")

	logging.SetLogLevel("pathresolv", "debug")

	storePath := "C:/Users/aaa/.titanedge-1/carfilestore"
	p := "/ipfs/QmNXoAB3ZNoFQQZMGk4utybuvABdLTz6hcVmtHnV4FUp3S/log"
	cs, err := store.NewCarfileStore(storePath)
	if err != nil {
		t.Errorf("TestResolvePath error:%s", err.Error())
		return
	}

	gw := &Gateway{carStore: cs}

	resolvePath, err := gw.resolvePath(context.Background(), path.New(p))
	if err != nil {
		t.Errorf("TestResolvePath error:%s", err.Error())
		return
	}

	t.Logf("root: %s, cid: %s, rest:%v", resolvePath.Root().String(), resolvePath.Cid().String(), resolvePath.Remainder())
}
