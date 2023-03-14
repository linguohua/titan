package edge

import (
	"context"
	"net"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/client"
	"github.com/linguohua/titan/node/carfile"
	"github.com/linguohua/titan/node/common"
	"github.com/linguohua/titan/node/device"
	"github.com/linguohua/titan/node/gateway"
	datasync "github.com/linguohua/titan/node/sync"
	"github.com/linguohua/titan/node/validate"
	"go.uber.org/fx"
)

var log = logging.Logger("edge")

type Edge struct {
	fx.In

	*common.CommonAPI
	*device.Device
	*carfile.CarfileImpl
	*gateway.BlockDownload
	*validate.Validate
	*datasync.DataSync

	PConn        net.PacketConn
	SchedulerAPI api.Scheduler
}

func (edge *Edge) WaitQuiet(ctx context.Context) error {
	log.Debug("WaitQuiet")
	return nil
}

func (edge *Edge) ExternalServiceAddrss(ctx context.Context, schedulerURL string) (string, error) {
	schedulerAPI, closer, err := client.NewScheduler(ctx, schedulerURL, nil)
	if err != nil {
		return "", err
	}
	defer closer()

	return schedulerAPI.NodeExternalServiceAddress(ctx)
}

func (edge *Edge) UserNATTravel(ctx context.Context, userServiceAddress string) error {
	// TODO: implemnet nat travel for user download carfile
	return nil
}
