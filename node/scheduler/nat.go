package scheduler

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/client"
	"github.com/linguohua/titan/api/types"
	cliutil "github.com/linguohua/titan/cli/util"
)

func (s *Scheduler) EdgeExternalAddr(ctx context.Context, nodeID, schedulerURL string) (string, error) {
	eNode := s.NodeManager.GetEdgeNode(nodeID)
	if eNode != nil {
		return eNode.API().GetMyExternalAddr(ctx, schedulerURL)
	}

	return "", fmt.Errorf("Node %s offline or not exist", nodeID)
}

func (s *Scheduler) IsBehindFullConeNAT(ctx context.Context, edgeURL string) (bool, error) {
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
		log.Warnf("IsBehindFullConeNAT,edge %s may be RestrictedNAT or PortRestrictedNAT", edgeURL)
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

	isBehindFullConeNAT, err := schedulerAPI.IsBehindFullConeNAT(context.Background(), edgeURL)
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

func (s *Scheduler) checkEdgeNatType(ctx context.Context, edgeAPI api.Edge, edgeAddr string) (types.NatType, error) {
	if len(s.SchedulerCfg.SchedulerServer1) == 0 {
		return types.NatTypeUnknow, nil
	}

	externalAddr, err := edgeAPI.GetMyExternalAddr(ctx, s.SchedulerCfg.SchedulerServer1)
	if err != nil {
		return types.NatTypeUnknow, err
	}

	if externalAddr != edgeAddr {
		return types.NatTypeSymmetric, nil
	}

	isBindNAT, err := s.checkEdgeIfBehindNAT(ctx, edgeAddr)
	if err != nil {
		return types.NatTypeUnknow, err
	}

	if !isBindNAT {
		return types.NatTypeNo, nil
	}

	if len(s.SchedulerCfg.SchedulerServer2) == 0 {
		return types.NatTypeUnknow, nil
	}

	edgeURL := fmt.Sprintf("https://%s/rpc/v0", edgeAddr)
	isBehindFullConeNAT, err := s.checkEdgeIfBehindFullConeNAT(ctx, s.SchedulerCfg.SchedulerServer2, edgeURL)
	if err != nil {
		return types.NatTypeUnknow, err
	}

	if isBehindFullConeNAT {
		return types.NatTypeFullCone, nil
	}

	isBehindRestrictedNAT, err := s.checkEdgeIfBehindRestrictedNAT(ctx, edgeURL)
	if isBehindRestrictedNAT {
		return types.NatTypeRestricted, nil
	}

	return types.NatTypePortRestricted, nil
}

func (s *Scheduler) getNatType(ctx context.Context, edgeAPI api.Edge, edgeAddr string) types.NatType {
	natType, err := s.checkEdgeNatType(context.Background(), edgeAPI, edgeAddr)
	if err != nil {
		log.Errorf("getNatType, error:%s", err.Error())
		natType = types.NatTypeUnknow
	}
	return natType
}

// NodeNatType get node nat type
func (s *Scheduler) NodeNatType(ctx context.Context, nodeID string) (types.NatType, error) {
	eNode := s.NodeManager.GetEdgeNode(nodeID)
	if eNode == nil {
		return types.NatTypeUnknow, fmt.Errorf("Node %s offline or not exist", nodeID)
	}

	return s.getNatType(ctx, eNode.API(), eNode.Addr()), nil
}
