package edge

import (
	"context"
	"net"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/client"
	"github.com/linguohua/titan/api/types"
	cliutil "github.com/linguohua/titan/cli/util"
	"github.com/linguohua/titan/node/asset"
	"github.com/linguohua/titan/node/common"
	"github.com/linguohua/titan/node/device"
	datasync "github.com/linguohua/titan/node/sync"
	validate "github.com/linguohua/titan/node/validation"
	"go.uber.org/fx"
	"golang.org/x/xerrors"
)

var log = logging.Logger("edge")

type Edge struct {
	fx.In

	*common.CommonAPI
	*device.Device
	*asset.Asset
	*validate.Validation
	*datasync.DataSync

	PConn        net.PacketConn
	SchedulerAPI api.Scheduler
}

func (edge *Edge) WaitQuiet(ctx context.Context) error {
	log.Debug("WaitQuiet")
	return nil
}

func (edge *Edge) ExternalServiceAddress(ctx context.Context, schedulerURL string) (string, error) {
	schedulerAPI, closer, err := client.NewScheduler(ctx, schedulerURL, nil)
	if err != nil {
		return "", err
	}
	defer closer()

	return schedulerAPI.GetExternalAddress(ctx)
}

func (edge *Edge) UserNATPunch(ctx context.Context, sourceURL string, req *types.NatPunchReq) error {
	return edge.checkNetworkConnectivity(sourceURL, req.Timeout)
}

func (edge *Edge) checkNetworkConnectivity(targetURL string, timeout int) error {
	udpPacketConn, err := net.ListenPacket("udp", ":0")
	if err != nil {
		return xerrors.Errorf("list udp %w", err)
	}

	defer func() {
		err = udpPacketConn.Close()
		if err != nil {
			log.Errorf("udpPacketConn Close err:%s", err.Error())
		}
	}()

	httpClient, err := cliutil.NewHTTP3Client(udpPacketConn, true, "")
	if err != nil {
		return xerrors.Errorf("new http3 client %w", err)
	}
	httpClient.Timeout = time.Duration(timeout) * time.Second

	resp, err := httpClient.Get(targetURL)
	if err != nil {
		return xerrors.Errorf("http3 client get error: %w, url: %s", err, targetURL)
	}
	defer resp.Body.Close()

	return nil
}
