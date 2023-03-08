package main

import (
	"fmt"
	"github.com/linguohua/titan/api/types"
	"github.com/linguohua/titan/node/scheduler/storage"
	gen "github.com/whyrusleeping/cbor-gen"
	"os"
)

func main() {
	err := gen.WriteMapEncodersToFile("../cbor_gen.go", "storage",
		storage.CarfileInfo{},
		storage.Log{},
		storage.NodeCacheResult{},
	)

	err = gen.WriteMapEncodersToFile("../../../../api/types/cbor_gen.go", "types",
		types.DownloadSource{},
	)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
