package device

import (
	"context"
	"net"
	"path/filepath"
	"strings"
	"time"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/build"
	"github.com/linguohua/titan/node/carfile/carfilestore"
	"github.com/linguohua/titan/node/fsutil"
	"github.com/shirou/gopsutil/v3/mem"
)

var log = logging.Logger("device")

const schedulerApiTimeout = 3

type Device struct {
	nodeID        string
	publicIP      string
	internalIP    string
	bandwidthUp   int64
	bandwidthDown int64
	carfileStore  *carfilestore.CarfileStore
}

func NewDevice(nodeID, internalIP string, bandwidthUp, bandwidthDown int64, carfileStore *carfilestore.CarfileStore) *Device {
	device := &Device{
		nodeID:        nodeID,
		internalIP:    internalIP,
		bandwidthUp:   bandwidthUp,
		bandwidthDown: bandwidthDown,
		carfileStore:  carfileStore,
	}

	cpu.Percent(0, false)

	return device
}

func (device *Device) NodeInfo(ctx context.Context) (api.NodeInfo, error) {
	info := api.NodeInfo{}

	v, err := api.VersionForType(api.RunningNodeType)
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
	info.Blocks, _ = device.carfileStore.BlockCount()

	mac, err := getMacAddr(info.InternalIP)
	if err != nil {
		log.Errorf("NodeInfo getMacAddr err:%s", err.Error())
		return api.NodeInfo{}, err
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
	info.DiskSpace, info.DiskUsage = device.GetDiskUsageStat()

	absPath, err := filepath.Abs(device.carfileStore.Path())
	if err != nil {
		return api.NodeInfo{}, err
	}

	info.IoSystem = fsutil.GetFilesystemType(absPath)
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

func getExternalIP(api api.Scheduler, timeout time.Duration) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	addr, err := api.NodeExternalAddr(ctx)
	if err != nil {
		return "", err
	}

	ip, _, err := net.SplitHostPort(addr)
	if err != nil {
		return "", err
	}
	return ip, nil
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

func (device *Device) GetExternaIP() string {
	return device.publicIP
}

func (device *Device) GetInternalIP() string {
	return device.internalIP
}

func (device *Device) NodeID(ctx context.Context) (string, error) {
	return device.nodeID, nil
}

func (device *Device) GetDiskUsageStat() (totalSpace, usage float64) {
	carfileStorePath := device.carfileStore.Path()
	usageStat, err := disk.Usage(carfileStorePath)
	if err != nil {
		log.Errorf("get disk usage stat error: %s", err)
		return 0, 0
	}
	return float64(usageStat.Total), usageStat.UsedPercent
}
