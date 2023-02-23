package scheduler

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/client"
	cliutil "github.com/linguohua/titan/cli/util"
	"github.com/linguohua/titan/node/handler"
	"github.com/linguohua/titan/node/scheduler/node"
)

func (s *Scheduler) GetEdgeExternalAddr(ctx context.Context, deviceID, schedulerURL string) (string, error) {
	eNode := s.nodeManager.GetEdgeNode(deviceID)
	if eNode != nil {
		return eNode.GetAPI().GetMyExternalAddr(ctx, schedulerURL)
	}

	return "", fmt.Errorf("Device %s offline or not exist", deviceID)
}

func (s *Scheduler) GetAllEdgeAddrs(ctx context.Context) (map[string]string, error) {
	myID := handler.GetDeviceID(ctx)
	edges := make(map[string]string)
	s.nodeManager.EdgeNodeMap.Range(func(key, value interface{}) bool {
		deviceID := key.(string)
		if myID != deviceID {
			edgeNode := value.(*node.EdgeNode)
			addr := edgeNode.Node.GetAddr()
			edges[deviceID] = addr
		}
		return true
	})

	return edges, nil
}

func (s *Scheduler) CheckEdgeIfBehindFullConeNAT(ctx context.Context, edgeURL string) (bool, error) {
	udpPacketConn, err := net.ListenPacket("udp", ":0")
	if err != nil {
		return false, err
	}
	defer udpPacketConn.Close()

	httpClient := cliutil.NewHttp3Client(udpPacketConn, true, "")
	edgeAPI, close, err := client.NewEdgeWithHttpClient(context.Background(), edgeURL, nil, httpClient)
	if err != nil {
		return false, err
	}
	defer close()

	if _, err := edgeAPI.Version(context.Background()); err != nil {
		log.Warnf("CheckEdgeIfBehindFullConeNAT,edge %s may be RestrictedNAT or PortRestrictedNAT", edgeURL)
		return false, nil
	}
	return true, nil
}

func (s *Scheduler) checkEdgeIfBehindNAT(ctx context.Context, edgeAddr string) (bool, error) {
	edgeURL := fmt.Sprintf("http://%s/rpc/v0", edgeAddr)

	httpClient := &http.Client{}
	edgeAPI, close, err := client.NewEdgeWithHttpClient(context.Background(), edgeURL, nil, httpClient)
	if err != nil {
		return false, err
	}
	defer close()

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if _, err := edgeAPI.Version(ctx); err != nil {
		log.Warnf("checkEdgeIfBehindNAT,edge %s is behind nat", edgeAddr)
		return true, nil
	}

	return false, nil
}

func (s *Scheduler) checkEdgeIfBehindFullConeNAT(ctx context.Context, schedulerURL, edgeURL string) (bool, error) {
	schedulerAPI, close, err := client.NewScheduler(context.Background(), schedulerURL, nil)
	if err != nil {
		return false, err
	}
	defer close()

	isBehindFullConeNAT, err := schedulerAPI.CheckEdgeIfBehindFullConeNAT(context.Background(), edgeURL)
	if err != nil {
		return false, err
	}

	return isBehindFullConeNAT, nil
}

func (s *Scheduler) checkEdgeIfBehindRestrictedNAT(ctx context.Context, edgeURL string) (bool, error) {
	udpPacketConn, err := net.ListenPacket("udp", ":0")
	if err != nil {
		return false, err
	}
	defer udpPacketConn.Close()

	httpClient := &http.Client{}
	edgeAPI, close, err := client.NewEdgeWithHttpClient(context.Background(), edgeURL, nil, httpClient)
	if err != nil {
		return false, err
	}
	defer close()

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if _, err := edgeAPI.Version(ctx); err != nil {
		log.Warnf("checkEdgeIfBehindRestrictedNAT,edge %s PortRestrictedNAT", edgeURL)
		return false, nil
	}

	return true, nil
}

func (s *Scheduler) checkEdgeNatType(ctx context.Context, edgeAPI api.Edge, edgeAddr string) (api.NatType, error) {
	if len(s.schedulerCfg.SchedulerServer1) == 0 {
		return api.NatTypeUnknow, nil
	}

	externalAddr, err := edgeAPI.GetMyExternalAddr(ctx, s.schedulerCfg.SchedulerServer1)
	if err != nil {
		return api.NatTypeUnknow, err
	}

	if externalAddr != edgeAddr {
		return api.NatTypeSymmetric, nil
	}

	isBindNAT, err := s.checkEdgeIfBehindNAT(ctx, edgeAddr)
	if err != nil {
		return api.NatTypeUnknow, err
	}

	if !isBindNAT {
		return api.NatTypeNo, nil
	}

	if len(s.schedulerCfg.SchedulerServer2) == 0 {
		return api.NatTypeUnknow, nil
	}

	edgeURL := fmt.Sprintf("https://%s/rpc/v0", edgeAddr)
	isBehindFullConeNAT, err := s.checkEdgeIfBehindFullConeNAT(ctx, s.schedulerCfg.SchedulerServer2, edgeURL)
	if err != nil {
		return api.NatTypeUnknow, err
	}

	if isBehindFullConeNAT {
		return api.NatTypeFullCone, nil
	}

	isBehindRestrictedNAT, err := s.checkEdgeIfBehindRestrictedNAT(ctx, edgeURL)
	if isBehindRestrictedNAT {
		return api.NatTypeRestricted, nil
	}

	return api.NatTypePortRestricted, nil
}

func (s *Scheduler) getNatType(ctx context.Context, edgeAPI api.Edge, edgeAddr string) string {
	natType, err := s.checkEdgeNatType(context.Background(), edgeAPI, edgeAddr)
	if err != nil {
		log.Errorf("getNatType, error:%s", err.Error())
		natType = api.NatTypeUnknow
	}
	return s.NatTypeToString(natType)
}

func (s *Scheduler) NatTypeToString(natType api.NatType) string {
	switch natType {
	case api.NatTypeNo:
		return "NoNAT"
	case api.NatTypeSymmetric:
		return "SymmetricNAT"
	case api.NatTypeFullCone:
		return "FullConeNAT"
	case api.NatTypeRestricted:
		return "RestrictedNAT"
	case api.NatTypePortRestricted:
		return "PortRestrictedNAT"
	}

	return "UnknowNAT"
}
