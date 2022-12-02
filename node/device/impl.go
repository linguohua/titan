package device

import (
	"context"
	"net"
	"strings"

	"github.com/shirou/gopsutil/v3/cpu"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/build"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/mem"
)

var log = logging.Logger("device")

type Device struct {
	deviceID      string
	publicIP      string
	internalIP    string
	bandwidthUp   int64
	bandwidthDown int64
}

func NewDevice(deviceID, publicIP, internalIP string, bandwidthUp, bandwidthDown int64) *Device {
	device := &Device{
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

	name := device.deviceID
	if len(name) > 10 {
		info.DeviceName = name[0:10]
	}
	info.ExternalIp = device.publicIP
	info.SystemVersion = version.String()
	info.InternalIp = device.internalIP
	info.BandwidthDown = float64(device.bandwidthDown)
	info.BandwidthUp = float64(device.bandwidthUp)

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
		info.MemoryUsage = vmStat.UsedPercent
		info.Memory = float64(vmStat.Total)
	}

	cpuPercent, err := cpu.Percent(0, false)
	if err != nil {
		log.Errorf("getCpuInfo: %v", err)
	}

	info.CpuUsage = cpuPercent[0]
	info.CPUCores, _ = cpu.Counts(false)

	partitions, err := disk.Partitions(true)
	if len(partitions) > 0 {
		info.IoSystem = partitions[0].Fstype

		use := uint64(0)
		total := uint64(0)
		for _, partition := range partitions {
			usageStat, err := disk.Usage(partition.Mountpoint)
			if err != nil {
				log.Errorf("DeviceInfo get disk usage err:%s", err.Error())
				return info, err
			}

			use += usageStat.Used
			total += usageStat.Total
		}

		info.DiskUsage = float64(use) / float64(total) * 100
		info.DiskSpace = float64(total)

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

func (device *Device) GetDeviceID() string {
	return device.deviceID
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

func (device *Device) SetExternaIP(externaIP string) {
	device.publicIP = externaIP
}

func (device *Device) GetExternaIP() string {
	return device.publicIP
}

func (device *Device) GetInternalIP() string {
	return device.internalIP
}
