package gateway

import (
	"context"
	"io"
	"os"
	"testing"

	"github.com/ipfs/go-libipfs/files"
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

	gw := &Gateway{cs: cs}

	resolvePath, err := gw.resolvePath(context.Background(), path.New(p))
	if err != nil {
		t.Errorf("TestResolvePath error:%s", err.Error())
		return
	}

	t.Logf("root: %s, cid: %s, rest:%v", resolvePath.Root().String(), resolvePath.Cid().String(), resolvePath.Remainder())

	fnode, err := gw.getUnixFsNode(context.Background(), resolvePath)
	if err != nil {
		t.Errorf("getUnixFsNode error:%s", err)
		return
	}

	// open output file
	fo, err := os.Create("output.txt")
	if err != nil {
		panic(err)
	}
	defer fo.Close()

	f, ok := fnode.(files.File)
	if !ok {
		t.Logf("can not chang to file")
		return
	}

	defer f.Close()
	// make a buffer to keep chunks that are read
	buf := make([]byte, 1024)
	for {
		// read a chunk
		n, err := f.Read(buf)
		if err != nil && err != io.EOF {
			panic(err)
		}
		if n == 0 {
			break
		}

		// write a chunk
		if _, err := fo.Write(buf[:n]); err != nil {
			panic(err)
		}
	}

}
