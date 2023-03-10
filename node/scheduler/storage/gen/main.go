package main

import (
	"fmt"
	"os"

	"github.com/linguohua/titan/node/scheduler/storage"
	gen "github.com/whyrusleeping/cbor-gen"
)

func main() {
	err := gen.WriteMapEncodersToFile("../cbor_gen.go", "storage",
		storage.CarfileInfo{},
		storage.Log{},
		storage.CacheResultInfo{},
		storage.CompletedValue{},
	)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
