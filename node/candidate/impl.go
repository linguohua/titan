package candidate

import (
	"context"

	"github.com/linguohua/titan/node/config"
	"go.uber.org/fx"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/carfile"
	"github.com/linguohua/titan/node/common"
	"github.com/linguohua/titan/node/device"
	datasync "github.com/linguohua/titan/node/sync"

	logging "github.com/ipfs/go-log/v2"
	vd "github.com/linguohua/titan/node/validate"
)

var log = logging.Logger("candidate")

const (
	schedulerAPITimeout = 3
	validateTimeout     = 5
	tcpPackMaxLength    = 52428800
)

type Candidate struct {
	fx.In

	*common.CommonAPI
	*carfile.CarfileImpl
	*device.Device
	*vd.Validate
	*datasync.DataSync

	Scheduler api.Scheduler
	Config    *config.CandidateCfg
	TCPSrv    *TCPServer
}

func (candidate *Candidate) WaitQuiet(ctx context.Context) error {
	log.Debug("WaitQuiet")
	return nil
}

func (candidate *Candidate) GetBlocksOfCarfile(ctx context.Context, carfileCID string, randomSeed int64, randomCount int) (map[int]string, error) {
	return candidate.CarfileImpl.GetBlocksOfCar(carfileCID, randomSeed, randomCount)
}
