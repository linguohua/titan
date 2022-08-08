package device

import (
	"context"
	"fmt"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/build"
	"github.com/linguohua/titan/stores"
)

type DeviceAPI struct {
	BlockStore stores.BlockStore
	DeviceID   string
	PublicIP   string
}

func (device DeviceAPI) DeviceInfo(ctx context.Context) (api.DevicesInfo, error) {
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

	if stat.Capacity > 0 {
		info.DiskUsage = fmt.Sprintf("%f", float32(stat.Capacity-stat.Available)/float32(stat.Capacity))
	}

	return info, nil
}
