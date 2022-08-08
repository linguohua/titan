package p2p

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	exchange "github.com/ipfs/go-ipfs-exchange-interface"
	"github.com/linguohua/titan/build"
)

func TestBootstrap(t *testing.T) {
	t.Log("TestBootstrap")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peers, err := build.BuiltinBootstrap()
	if err != nil {
		t.Fatal(err)
	}

	exchange, err := Bootstrap(ctx, peers)
	if err != nil {
		t.Fatalf("Bootstrap error:%v", err)
	}

	t.Log("Bootstrap success")
	getblock(t, exchange)

}

func getblock(t *testing.T, exchange exchange.Interface) {
	cidstr := "QmeUqw4FY1wqnh2FMvuc2v8KAapE7fYwu2Up4qNwhZiRk7"
	target, err := cid.Decode(cidstr)
	if err != nil {
		t.Logf("failed to decode CID '%q': %s", os.Args[2], err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	now := time.Now().Unix()
	block, err := exchange.GetBlock(ctx, target)
	if err != nil {
		t.Log("get block error:", err)
	}

	if block != nil {
		t.Log("block:", string(block.RawData()))
	}

	t.Log("cost :", time.Now().Unix()-now)

}
