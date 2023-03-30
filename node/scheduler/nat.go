package scheduler

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/linguohua/titan/api/client"
	"github.com/linguohua/titan/api/types"
	cliutil "github.com/linguohua/titan/cli/util"
	"github.com/linguohua/titan/node/scheduler/node"
)

func (s *Scheduler) EdgeExternalServiceAddress(ctx context.Context, nodeID, schedulerURL string) (string, error) {
	eNode := s.NodeManager.GetEdgeNode(nodeID)
	if eNode != nil {
		return eNode.ExternalServiceAddress(ctx, schedulerURL)
	}

	return "", fmt.Errorf("Node %s offline or not exist", nodeID)
}

func (s *Scheduler) CheckEdgeConnectivity(ctx context.Context, url string) error {
	udpPacketConn, err := net.ListenPacket("udp", ":0")
	if err != nil {
		return err
	}
	defer func() {
		err = udpPacketConn.Close()
		if err != nil {
			log.Errorf("udpPacketConn Close err:%s", err.Error())
		}
	}()

	httpClient, err := cliutil.NewHTTP3Client(udpPacketConn, true, "")
	if err != nil {
		return err
	}

	edgeAPI, close, err := client.NewEdgeWithHTTPClient(context.Background(), url, nil, httpClient)
	if err != nil {
		return err
	}
	defer close()

	if _, err := edgeAPI.Version(context.Background()); err != nil {
		return err
	}
	return nil
}

// CheckNetworkConnectivity check tcp or udp network connectivity
// network is "tcp" or "udp"
func (s *Scheduler) CheckNetworkConnectivity(ctx context.Context, network, targetURL string) error {
	switch network {
	case "tcp":
		return s.checkTcpConnectivity(targetURL)
	case "udp":
		return s.checkUdpConnectivity(targetURL)
	}

	return fmt.Errorf("unknow network %s type", network)
}

func (s *Scheduler) checkEdgeIfBehindFullConeNAT(ctx context.Context, schedulerURL, edgeURL string) (bool, error) {
	schedulerAPI, close, err := client.NewScheduler(context.Background(), schedulerURL, nil)
	if err != nil {
		return false, err
	}
	defer close()

	if err = schedulerAPI.CheckNetworkConnectivity(context.Background(), "udp", edgeURL); err == nil {
		return true, nil
	}

	log.Debugf("check udp connectivity failed: %s", err.Error())

	return false, nil
}

func (s *Scheduler) checkEdgeIfBehindRestrictedNAT(ctx context.Context, edgeURL string) (bool, error) {
	udpPacketConn, err := net.ListenPacket("udp", ":0")
	if err != nil {
		return false, err
	}
	defer func() {
		err = udpPacketConn.Close()
		if err != nil {
			log.Errorf("udpPacketConn Close err:%s", err.Error())
		}
	}()

	httpClient := &http.Client{}
	edgeAPI, close, err := client.NewEdgeWithHTTPClient(context.Background(), edgeURL, nil, httpClient)
	if err != nil {
		return false, err
	}
	defer close()

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if _, err := edgeAPI.Version(ctx); err != nil {
		log.Warnf("checkEdgeIfBehindRestrictedNAT,edge %s PortRestrictedNAT", edgeURL)
		return false, nil //nolint:nilerr
	}

	return true, nil
}

func (s *Scheduler) checkEdgeNatType(ctx context.Context, edgeAPI *node.API, edgeAddr string) (types.NatType, error) {
	if len(s.SchedulerCfg.SchedulerServer1) == 0 {
		return types.NatTypeUnknow, nil
	}

	externalAddr, err := edgeAPI.ExternalServiceAddress(ctx, s.SchedulerCfg.SchedulerServer1)
	if err != nil {
		return types.NatTypeUnknow, err
	}

	if externalAddr != edgeAddr {
		return types.NatTypeSymmetric, nil
	}

	if err = s.CheckNetworkConnectivity(ctx, "tcp", edgeAddr); err == nil {
		return types.NatTypeNo, nil
	}

	log.Debugf("CheckTcpConnectivity error: %s", err.Error())

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
	if err != nil {
		return types.NatTypeUnknow, err
	}

	if isBehindRestrictedNAT {
		return types.NatTypeRestricted, nil
	}

	return types.NatTypePortRestricted, nil
}

func (s *Scheduler) getNatType(ctx context.Context, edgeAPI *node.API, edgeAddr string) types.NatType {
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
		return types.NatTypeUnknow, fmt.Errorf("node %s offline or not exist", nodeID)
	}

	return s.getNatType(ctx, eNode.API, eNode.Addr()), nil
}

func (s *Scheduler) checkTcpConnectivity(targetURL string) error {
	url, err := url.ParseRequestURI(targetURL)
	if err != nil {
		return err
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp", url.Host)
	if err != nil {
		return err
	}

	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	return nil
}

func (s *Scheduler) checkUdpConnectivity(targetURL string) error {
	udpPacketConn, err := net.ListenPacket("udp", ":0")
	if err != nil {
		return err
	}
	defer func() {
		err = udpPacketConn.Close()
		if err != nil {
			log.Errorf("udpPacketConn Close err:%s", err.Error())
		}
	}()

	httpClient, err := cliutil.NewHTTP3Client(udpPacketConn, true, "")
	if err != nil {
		return err
	}
	httpClient.Timeout = 5 * time.Second

	resp, err := httpClient.Get(targetURL)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil
}
