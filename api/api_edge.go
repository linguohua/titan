package api

import (
	"context"
)

type Edge interface {
	Common
	Device
	// Block
	Download
	Validate
	DataSync
	CarfileOperation
	WaitQuiet(ctx context.Context) error                                            //perm:read
	ExternalServiceAddrss(ctx context.Context, schedulerURL string) (string, error) //perm:write
	UserNATTravel(ctx context.Context, userServiceAddress string) error             //perm:write
}
