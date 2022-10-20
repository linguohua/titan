package device

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/shirou/gopsutil/v3/cpu"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/build"
	"github.com/linguohua/titan/node/download"
	"github.com/linguohua/titan/stores"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/mem"
)

var log = logging.Logger("device")

const deviceName = "titan-edge"

type Device struct {
	blockStore    stores.BlockStore
	deviceID      string
	publicIP      string
	internalIP    string
	bandwidthUp   int64
	bandwidthDown int64
	blockDownload *download.BlockDownload
}

func NewDevice(blockStore stores.BlockStore, deviceID, publicIP, internalIP string, bandwidthUp, bandwidthDown int64) *Device {
	device := &Device{
		blockStore:    blockStore,
		deviceID:      deviceID,
		publicIP:      publicIP,
		internalIP:    internalIP,
		bandwidthUp:   bandwidthUp,
		bandwidthDown: bandwidthDown,
	}

	return device
}
func (device *Device) DeviceInfo(ctx context.Context) (api.DevicesInfo, error) {
	info := api.DevicesInfo{}

	v, err := api.VersionForType(api.RunningNodeType)
	if err != nil {
		return info, err
	}

	version := api.APIVersion{
		Version:    build.UserVersion(),
		APIVersion: v,
	}

	info.DeviceId = device.deviceID
	info.ExternalIp = device.publicIP
	info.SystemVersion = version.String()
	info.DeviceName = deviceName
	info.InternalIp = device.internalIP
	info.BandwidthDown = device.bandwidthDown

	if device.blockDownload != nil {
		info.BandwidthUp = int64(device.blockDownload.GetRateLimit())
	}

	mac, err := getMacAddr(info.InternalIp)
	if err != nil {
		log.Errorf("DeviceInfo getMacAddr err:%s", err.Error())
		return info, err
	}

	info.MacLocation = mac

	vmStat, err := mem.VirtualMemory()
	if err != nil {
		log.Errorf("getMemory: %v", err)
	}

	if vmStat != nil {
		info.MemoryUsage = strconv.FormatFloat(vmStat.UsedPercent, 'f', 10, 32)
	}

	cpuPercent, err := cpu.Percent(0, false)
	if err != nil {
		log.Errorf("getCpuInfo: %v", err)
	}

	info.CpuUsage = strconv.FormatFloat(cpuPercent[0], 'f', 10, 32)

	partitions, err := disk.Partitions(true)
	if len(partitions) > 0 {
		info.DiskType = partitions[0].Fstype

		free := uint64(0)
		total := uint64(0)
		for _, partition := range partitions {
			usageStat, err := disk.Usage(partition.Mountpoint)
			if err != nil {
				log.Errorf("DeviceInfo get disk usage err:%s", err.Error())
				return info, err
			}

			free += usageStat.Used
			total += usageStat.Total
		}

		info.DiskUsage = fmt.Sprintf("%f%%", float64(free)/float64(total))

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

func (device *Device) SetBlockDownload(blockDownload *download.BlockDownload) {
	device.blockDownload = blockDownload
}

func (device *Device) GetDeviceID() string {
	return device.deviceID
}

func (device *Device) GetBandwidthUp() int64 {
	return device.bandwidthUp
}

func (device *Device) GetBandwidthDown() int64 {
	return device.bandwidthDown
}

func (device *Device) GetPublicIP() string {
	return device.publicIP
}

func (device *Device) GetInternalIP() string {
	return device.internalIP
}
