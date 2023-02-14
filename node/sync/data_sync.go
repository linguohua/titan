package sync

import (
	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/node/carfile/carfilestore"
)

var log = logging.Logger("datasync")

type DataSync struct {
	carfileStore *carfilestore.CarfileStore
}

// TODO: implement data sync interface
func NewDataSync(carfileStore *carfilestore.CarfileStore) *DataSync {
	return &DataSync{carfileStore: carfileStore}
}
