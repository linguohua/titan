package main

import (
	"fmt"
	"os"

	"github.com/linguohua/titan/node/scheduler/assets"
	gen "github.com/whyrusleeping/cbor-gen"
)

func main() {
	err := gen.WriteMapEncodersToFile("../cbor_gen.go", "assets",
		assets.AssetCachingInfo{},
		assets.NodeCacheResultInfo{},
	)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
