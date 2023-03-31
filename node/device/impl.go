package device

import (
	"context"
	"net"
	"strings"

	"github.com/shirou/gopsutil/v3/cpu"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/types"
	"github.com/linguohua/titan/build"
	"github.com/shirou/gopsutil/v3/mem"
)

var log = logging.Logger("device")

type Device struct {
	nodeID        string
	publicIP      string
	internalIP    string
	bandwidthUp   int64
	bandwidthDown int64
	storage       Storage
}

type Storage interface {
	GetDiskUsageStat() (totalSpace, usage float64)
	GetFilesystemType() string
}

func NewDevice(nodeID, internalIP string, bandwidthUp, bandwidthDown int64, storage Storage) *Device {
	device := &Device{
		nodeID:        nodeID,
		internalIP:    internalIP,
		bandwidthUp:   bandwidthUp,
		bandwidthDown: bandwidthDown,
		storage:       storage,
	}

	if _, err := cpu.Percent(0, false); err != nil {
		log.Errorf("stat cpu percent error: %s", err.Error())
	}

	return device
}

func (device *Device) NodeInfo(ctx context.Context) (types.NodeInfo, error) {
	info := types.NodeInfo{}

	v, err := api.VersionForType(types.RunningNodeType)
	if err != nil {
		return info, err
	}

	version := api.APIVersion{
		Version:    build.UserVersion(),
		APIVersion: v,
	}

	info.NodeID = device.nodeID

	name := device.nodeID
	if len(name) > 10 {
		info.NodeName = name[0:10]
	}
	info.ExternalIP = device.publicIP
	info.SystemVersion = version.String()
	info.InternalIP = device.internalIP
	info.BandwidthDown = float64(device.bandwidthDown)
	info.BandwidthUp = float64(device.bandwidthUp)

	mac, err := getMacAddr(info.InternalIP)
	if err != nil {
		log.Errorf("NodeInfo getMacAddr err:%s", err.Error())
		return types.NodeInfo{}, err
	}

	info.MacLocation = mac

	vmStat, err := mem.VirtualMemory()
	if err != nil {
		log.Errorf("getMemory: %s", err.Error())
	}

	if vmStat != nil {
		info.MemoryUsage = vmStat.UsedPercent
		info.Memory = float64(vmStat.Total)
	}

	cpuPercent, err := cpu.Percent(0, false)
	if err != nil {
		log.Errorf("getCpuInfo: %s", err.Error())
	}

	info.CPUUsage = cpuPercent[0]
	info.CPUCores, _ = cpu.Counts(false)
	info.DiskSpace, info.DiskUsage = device.storage.GetDiskUsageStat()
	info.IoSystem = device.storage.GetFilesystemType()
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

func (device *Device) SetBandwidthUp(bandwidthUp int64) {
	device.bandwidthUp = bandwidthUp
}

func (device *Device) GetBandwidthUp() int64 {
	return device.bandwidthUp
}

func (device *Device) GetBandwidthDown() int64 {
	return device.bandwidthDown
}

func (device *Device) GetInternalIP() string {
	return device.internalIP
}

func (device *Device) NodeID(ctx context.Context) (string, error) {
	return device.nodeID, nil
}
