package api

import (
	"context"

	"github.com/linguohua/titan/api/types"
)

// Locator is an interface for locator services
type Locator interface {
	Common
	// GetAccessPoints retrieves all access points associated with a node.
	GetAccessPoints(ctx context.Context, nodeID string) ([]string, error) //perm:read
	// AddAccessPoint  adds an access point to an area with a specified weight and scheduler access token.
	AddAccessPoint(ctx context.Context, areaID string, schedulerURL string, weight int, schedulerAccessToken string) error //perm:admin
	// RemoveAccessPoints removes all access points from a specified area.
	RemoveAccessPoints(ctx context.Context, areaID string) error //perm:admin                                  //perm:admin
	// ListAreaIDs retrieves all area IDs.
	ListAreaIDs(ctx context.Context) (areaIDs []string, err error) //perm:admin
	// GetAccessPoint returns an access point for a specified area.
	GetAccessPoint(ctx context.Context, areaID string) (AccessPoint, error) //perm:admin

	// UpdateNodeOnlineStatus sets the online status of a node.
	UpdateNodeOnlineStatus(ctx context.Context, nodeID string, isOnline bool) error //perm:write

	// user api
	// EdgeDownloadInfos retrieves download information for a content identifier (CID).
	EdgeDownloadInfos(ctx context.Context, cid string) ([]*types.DownloadInfo, error) //perm:read
	// UserDownloadBlockResults  accepts a user's download block results.
	UserDownloadBlockResults(ctx context.Context, results []types.UserBlockDownloadResult) error //perm:read

	// api for web
	// RegisterNewNode  registers a new node with the specified scheduler URL, node ID, public key, and node type.
	RegisterNewNode(ctx context.Context, schedulerURL, nodeID, publicKey string, nt types.NodeType) error // perm:admin
	// GetWebAccessPoints retrieves all access points for web usage.
	GetWebAccessPoints(ctx context.Context) ([]AccessPoint, error) // perm:admin
	// GetUserAccessPoint retrieves an access point for a user with a specified IP address.
	GetUserAccessPoint(ctx context.Context, userIP string) (AccessPoint, error) // perm:admin
}

// SchedulerInfo contains information about a scheduler, including its URL and weight.
type SchedulerInfo struct {
	URL    string
	Weight int
	// Online      bool
	// AccessToken string
}

// AccessPoint represents an access point within an area, containing scheduler information.
type AccessPoint struct {
	AreaID         string
	SchedulerInfos []SchedulerInfo
}
