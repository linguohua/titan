package cache

import (
	"testing"

	logging "github.com/ipfs/go-log/v2"
)

func TestManager(t *testing.T) {
	t.Log("TestManager")
	logging.SetLogLevel("/carfile/cache", "DEBUG")

}
