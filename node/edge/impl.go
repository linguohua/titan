package edge

import (
	"context"
	"net"
	"sync"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/client"
	"github.com/linguohua/titan/node/carfile"
	"github.com/linguohua/titan/node/common"
	"github.com/linguohua/titan/node/device"
	"github.com/linguohua/titan/node/download"
	datasync "github.com/linguohua/titan/node/sync"
	"github.com/linguohua/titan/node/validate"
	"go.uber.org/fx"
)

var log = logging.Logger("edge")

type Edge struct {
	fx.In

	*common.CommonAPI
	*device.Device
	*carfile.CarfileOperation
	*download.BlockDownload
	*validate.Validate
	*datasync.DataSync

	PConn        net.PacketConn
	Peers        *Peers
	SchedulerAPI api.Scheduler
}

type Peers struct {
	sync.Map
}

func NewPeers() *Peers {
	return &Peers{}
}

func (edge *Edge) WaitQuiet(ctx context.Context) error {
	log.Debug("WaitQuiet")
	return nil
}

func (edge *Edge) GetMyExternalAddr(ctx context.Context, schedulerURL string) (string, error) {
	schedulerAPI, closer, err := client.NewScheduler(ctx, schedulerURL, nil)
	if err != nil {
		return "", err
	}
	defer closer()

	return schedulerAPI.NodeExternalAddr(ctx)
}

func (edge *Edge) PingUser(ctx context.Context, userAddr string) error {
	return nil
}
