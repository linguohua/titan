package main

import (
	"fmt"
	"os"

	"github.com/linguohua/titan/node/scheduler/caching"
	gen "github.com/whyrusleeping/cbor-gen"
)

func main() {
	err := gen.WriteMapEncodersToFile("../cbor_gen.go", "cache",
		caching.CarfileCacheInfo{},
		caching.NodeCacheResultInfo{},
	)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
