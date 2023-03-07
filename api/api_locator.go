package api

import (
	"context"
)

type Locator interface {
	Common
	GetAccessPoints(ctx context.Context, nodeID string) ([]string, error)                                                  //perm:read
	AddAccessPoint(ctx context.Context, areaID string, schedulerURL string, weight int, schedulerAccessToken string) error //perm:admin
	RemoveAccessPoints(ctx context.Context, areaID string) error                                                           //perm:admin                                  //perm:admin
	ListAreaIDs(ctx context.Context) (areaIDs []string, err error)                                                         //perm:admin
	ShowAccessPoint(ctx context.Context, areaID string) (AccessPoint, error)                                               //perm:admin

	SetNodeOnlineStatus(ctx context.Context, nodeID string, isOnline bool) error //perm:write

	// user api
	GetDownloadInfosWithCarfile(ctx context.Context, cid string) ([]*DownloadInfoResult, error) //perm:read
	// user send result when user download block complete
	UserDownloadBlockResults(ctx context.Context, results []UserBlockDownloadResult) error //perm:read

	// api for web
	AllocateNodes(ctx context.Context, schedulerURL string, nt NodeType, count int) ([]NodeAllocateInfo, error) // perm:admin
	LoadAccessPointsForWeb(ctx context.Context) ([]AccessPoint, error)                                          // perm:admin
	LoadUserAccessPoint(ctx context.Context, userIP string) (AccessPoint, error)                                // perm:admin
}

type SchedulerInfo struct {
	URL    string
	Weight int
	// Online      bool
	// AccessToken string
}
type AccessPoint struct {
	AreaID         string
	SchedulerInfos []SchedulerInfo
}
