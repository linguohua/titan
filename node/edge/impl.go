package edge

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/client"
	"github.com/linguohua/titan/node/carfile"
	"github.com/linguohua/titan/node/carfile/carfilestore"
	"github.com/linguohua/titan/node/carfile/downloader"
	"github.com/linguohua/titan/node/common"
	datasync "github.com/linguohua/titan/node/sync"
	"github.com/linguohua/titan/node/validate"
	"golang.org/x/time/rate"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/node/device"
	"github.com/linguohua/titan/node/download"
)

var log = logging.Logger("edge")

const pingUserDration = 15 * time.Minute

func NewLocalEdgeNode(ctx context.Context, params *EdgeParams) api.Edge {
	device := params.Device
	rateLimiter := rate.NewLimiter(rate.Limit(device.GetBandwidthUp()), int(device.GetBandwidthUp()))
	validate := validate.NewValidate(params.CarfileStore, device)

	blockDownload := download.NewBlockDownload(rateLimiter, params.Scheduler, params.CarfileStore, device, validate)

	carfileOeration := carfile.NewCarfileOperation(params.CarfileStore, params.Scheduler, downloader.NewCandidate(params.CarfileStore), device)

	edge := &Edge{
		Device:           device,
		CarfileOperation: carfileOeration,
		BlockDownload:    blockDownload,
		Validate:         validate,
		DataSync:         datasync.NewDataSync(params.CarfileStore),
		pConn:            params.PConn,
		userPing:         &sync.Map{},
	}

	go edge.startUserPingTick()

	return edge
}

type Edge struct {
	*common.CommonAPI
	*device.Device
	*carfile.CarfileOperation
	*download.BlockDownload
	*validate.Validate
	*datasync.DataSync

	pConn    net.PacketConn
	userPing *sync.Map
}

type EdgeParams struct {
	Scheduler    api.Scheduler
	CarfileStore *carfilestore.CarfileStore
	Device       *device.Device
	PConn        net.PacketConn
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

	return schedulerAPI.GetExternalAddr(ctx)
}

func (edge *Edge) PingUser(ctx context.Context, userAddr string) error {
	_, ok := edge.userPing.Load(userAddr)
	if ok {
		return nil
	}

	edge.userPing.Store(userAddr, time.Now())
	pingUser(edge.pConn, userAddr)

	return nil
}

// ping user for p2p connection
func (edge *Edge) startUserPingTick() {
	ticker := time.NewTicker(3 * time.Second)
	for {
		select {
		case <-ticker.C:
			edge.pingUsers()
		}
	}
}

func (edge *Edge) pingUsers() {
	edge.userPing.Range(func(k, v interface{}) bool {
		addr := k.(string)
		startTime := v.(time.Time)

		duration := time.Since(startTime)
		if duration >= pingUserDration {
			edge.userPing.Delete(addr)
		}

		err := pingUser(edge.pConn, addr)
		if err != nil {
			log.Errorf("ping user error:%s", err)
		}
		log.Infof("pingUsers addr:%s", addr)
		return true
	})
}

func pingUser(pConn net.PacketConn, addr string) error {
	remoteAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return err
	}

	_, err = pConn.WriteTo([]byte("hello"), remoteAddr)
	return err
}
