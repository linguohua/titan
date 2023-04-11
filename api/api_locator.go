package api

import (
	"context"

	"github.com/linguohua/titan/api/types"
)

// Locator is an interface for locator services
type Locator interface {
	Common
	// GetAccessPoints retrieves all access points associated with a node.
	GetAccessPoints(ctx context.Context, nodeID, areaID string) ([]string, error) //perm:read
	// user api
	// EdgeDownloadInfos retrieves download information for a content identifier (CID).
	EdgeDownloadInfos(ctx context.Context, cid string) (*types.EdgeDownloadInfoList, error) //perm:read
	// GetUserAccessPoint retrieves an access point for a user with a specified IP address.
	GetUserAccessPoint(ctx context.Context, userIP string) (AccessPoint, error) // perm:admin
}

// SchedulerInfo contains information about a scheduler, including its URL and weight.
type SchedulerInfo struct {
	URL    string
	Weight int
}

// AccessPoint represents an access point within an area, containing scheduler information.
type AccessPoint struct {
	AreaID         string
	SchedulerInfos []SchedulerInfo
}
