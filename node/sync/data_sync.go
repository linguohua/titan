package sync

import (
	"github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/node/carfile/carfilestore"
)

var log = logging.Logger("datasync")

type DataSync struct {
	carfileStore *carfilestore.CarfileStore
	ds           datastore.Batching
}

// TODO: implement data sync interface
func NewDataSync(ds datastore.Batching) *DataSync {
	return &DataSync{ds: ds}
}
