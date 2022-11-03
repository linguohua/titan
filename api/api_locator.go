package api

import "context"

type Location interface {
	Common
	GetAccessPoints(ctx context.Context, deviceID string, securityKey string) ([]string, error) //perm:read
	AddAccessPoints(ctx context.Context, areaID string, schedulerURL string, weight int) error  //perm:read
	RemoveAccessPoints(ctx context.Context, areaID string) error                                //perm:read                                  //perm:admin
	ListAccessPoints(ctx context.Context) (areaIDs []string, err error)                         //perm:read
	ShowAccessPoint(ctx context.Context, areaID string) (AccessPoint, error)                    //perm:read

	DeviceOnline(ctx context.Context, deviceID string, areaID string, port int) error //perm:read
	DeviceOffline(ctx context.Context, deviceID string) error                         //perm:read

	GetDownloadInfosWithBlocks(ctx context.Context, cids []string) (map[string][]DownloadInfo, error) //perm:read
	GetDownloadInfoWithBlocks(ctx context.Context, cids []string) (map[string]DownloadInfo, error)    //perm:read
	GetDownloadInfoWithBlock(ctx context.Context, cid string) (DownloadInfo, error)                   //perm:read
}

type SchedulerInfo struct {
	URL    string
	Weight int
	Online bool
}
type AccessPoint struct {
	AreaID         string
	SchedulerInfos []SchedulerInfo
}
