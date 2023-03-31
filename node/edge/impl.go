package edge

import (
	"context"
	"fmt"
	"net"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/client"
	cliutil "github.com/linguohua/titan/cli/util"
	"github.com/linguohua/titan/node/carfile"
	"github.com/linguohua/titan/node/common"
	"github.com/linguohua/titan/node/device"
	datasync "github.com/linguohua/titan/node/sync"
	"github.com/linguohua/titan/node/validate"
	"go.uber.org/fx"
	"golang.org/x/xerrors"
)

var log = logging.Logger("edge")

type Edge struct {
	fx.In

	*common.CommonAPI
	*device.Device
	*carfile.CarfileImpl
	*validate.Validate
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

	return schedulerAPI.NodeExternalServiceAddress(ctx)
}

func (edge *Edge) UserNATTravel(ctx context.Context, userServiceAddress string) error {
	url := fmt.Sprintf("https://%s/ping", userServiceAddress)
	go func() {
		timeout := time.After(15 * time.Second)

		for {
			err := edge.checkNetworkConnectivity(url)
			if err == nil {
				log.Debugf("nat traver, success connect to %s", url)
				return
			}

			log.Debugf("connect failed %s, url %s", err.Error(), url)

			select {
			case <-timeout:
				log.Errorf("timeout, can not connect to %s", url)
				return
			default:

			}
		}
	}()

	return nil
}

func (edge *Edge) checkNetworkConnectivity(targetURL string) error {
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
	httpClient.Timeout = 3 * time.Second

	resp, err := httpClient.Get(targetURL)
	if err != nil {
		return xerrors.Errorf("http3 client get error: %w, url: %s", err, targetURL)
	}
	defer resp.Body.Close()

	return nil
}
