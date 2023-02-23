package edge

import (
	"context"
	"fmt"
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
		peers:            &sync.Map{},
		schedulerAPI:     params.Scheduler,
	}

	go edge.startPingPeerTick()

	return edge
}

type Edge struct {
	*common.CommonAPI
	*device.Device
	*carfile.CarfileOperation
	*download.BlockDownload
	*validate.Validate
	*datasync.DataSync

	pConn        net.PacketConn
	peers        *sync.Map
	schedulerAPI api.Scheduler
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
	return nil
}

// ping peer tick
func (edge *Edge) startPingPeerTick() {
	pingPeers := time.Tick(10 * time.Second)
	loadPeers := time.Tick(1 * time.Minute)
	for {
		select {
		case <-pingPeers:
			edge.pingPeers()
		case <-loadPeers:
			edge.loadPeers()
		}
	}
}

func (edge *Edge) pingPeers() {
	edge.peers.Range(func(k, v interface{}) bool {
		peerID := k.(string)
		addr := v.(string)

		ver, err := pingPeer(addr)
		if err != nil {
			log.Errorf("ping peer %s error:%s, addr", peerID, err, addr)
		} else {
			log.Infof("ping peer %s addr:%s, ver:%s", peerID, addr, ver.String())
		}
		return true
	})
}

func (edge *Edge) loadPeers() {
	edges, err := edge.schedulerAPI.GetAllEdgeAddrs(context.Background())
	if err != nil {
		log.Errorf("loadPeers error:%s", err.Error())
		return
	}

	edge.peers.Range(func(k, v interface{}) bool {
		edge.peers.Delete(k)
		return true
	})

	for k, v := range edges {
		edge.peers.Store(k, v)
	}
}

func pingPeer(addr string) (api.APIVersion, error) {
	edgeApi, close, err := client.NewEdge(context.Background(), fmt.Sprintf("https://%s/rpc/v0", addr), nil)
	if err != nil {
		return api.APIVersion{}, err
	}
	defer close()

	return edgeApi.Version(context.Background())
}
