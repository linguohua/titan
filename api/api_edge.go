package api

import (
	"context"

	"github.com/linguohua/titan/api/types"
)

// Edge is an interface for edge node
type Edge interface {
	Common
	Device
	Validate
	DataSync
	Asset
	WaitQuiet(ctx context.Context) error //perm:read
	// ExternalServiceAddress check service address with different scheduler server
	// if behind nat, service address maybe different
	ExternalServiceAddress(ctx context.Context, schedulerURL string) (string, error) //perm:write
	// UserNATTravel build connection for user
	UserNATTravel(ctx context.Context, userServiceAddress string, req *types.NatTravelReq) error //perm:write
}
