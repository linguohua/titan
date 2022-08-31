package device

import (
	"context"
	"fmt"
	"net"
	"strings"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/build"
	"github.com/linguohua/titan/stores"
	"golang.org/x/time/rate"
)

var log = logging.Logger("edge")
var deviceName = "titan-edge"

type Device struct {
	BlockStore     stores.BlockStore
	DeviceID       string
	PublicIP       string
	InternalIP     string
	DownloadSrvURL string
	BandwidthUp    int64
	BandwidthDown  int64
	Limiter        *rate.Limiter
}

func (device *Device) DeviceInfo(ctx context.Context) (api.DevicesInfo, error) {
	info := api.DevicesInfo{}

	stat, err := device.BlockStore.Stat()
	if err != nil {
		return info, err
	}

	v, err := api.VersionForType(api.RunningNodeType)
	if err != nil {
		return info, err
	}

	version := api.APIVersion{
		Version:    build.UserVersion(),
		APIVersion: v,
	}

	info.DeviceId = device.DeviceID
	info.ExternalIp = device.PublicIP
	info.SystemVersion = version.String()
	info.DeviceName = deviceName
	info.InternalIp = device.InternalIP
	info.DownloadSrvURL = device.DownloadSrvURL
	info.BandwidthDown = device.BandwidthDown
	info.BandwidthUp = int64(device.Limiter.Limit())

	mac, err := getMacAddr(info.InternalIp)
	if err != nil {
		log.Infof("getMacAddr err:%v", err)
	}

	info.MacLocation = mac

	if stat.Capacity > 0 {
		info.DiskUsage = fmt.Sprintf("%f", float32(stat.Capacity-stat.Available)/float32(stat.Capacity))
	}

	return info, nil
}

func getMacAddr(ip string) (string, error) {
	ifas, err := net.Interfaces()
	if err != nil {
		return "", err
	}

	for _, ifa := range ifas {
		addrs, err := ifa.Addrs()
		if err != nil {
			continue
		}

		for _, addr := range addrs {
			localAddr := addr.(*net.IPNet)
			localIP := strings.Split(localAddr.IP.String(), ":")[0]

			if localIP == ip {
				return ifa.HardwareAddr.String(), nil
			}
		}
	}
	return "", nil
}
