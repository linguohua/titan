package edge

import (
	"context"
	"titan-ultra-network/api"
	"titan-ultra-network/journal/alerting"

	"github.com/filecoin-project/go-jsonrpc/auth"
	"github.com/google/uuid"
	"github.com/ipfs/go-datastore"
)

func NewLocalEdgeNode(ds datastore.Batching, scheduler api.Scheduler) api.Edge {
	return EdgeAPI{}
}

type EdgeAPI struct {
}

func (edge EdgeAPI) WaitQuiet(ctx context.Context) error {
	return nil
}

func (edge EdgeAPI) Save(ctx context.Context) error {
	return nil
}

// MethodGroup: Auth

func (edge EdgeAPI) AuthVerify(ctx context.Context, token string) ([]auth.Permission, error) {
	return nil, nil
}
func (edge EdgeAPI) AuthNew(ctx context.Context, perms []auth.Permission) ([]byte, error) {
	return nil, nil
}

func (edge EdgeAPI) LogList(context.Context) ([]string, error) {
	return nil, nil
}
func (edge EdgeAPI) LogSetLevel(context.Context, string, string) error {
	return nil
}

// LogAlerts returns list of all, active and inactive alerts tracked by the
// node
func (edge EdgeAPI) LogAlerts(ctx context.Context) ([]alerting.Alert, error) {
	return nil, nil
}

// MethodGroup: Common

// Version provides information about API provider
func (edge EdgeAPI) Version(context.Context) (api.APIVersion, error) {
	ver, err := api.VersionForType(api.NodeEdge)
	if err != nil {
		return api.APIVersion{}, err
	}

	return api.APIVersion{Version: ver.String(), APIVersion: ver}, nil
}

// Discover returns an OpenRPC document describing an RPC API.
func (edge EdgeAPI) Discover(ctx context.Context) (api.OpenRPCDocument, error) {
	return nil, nil
}

// trigger graceful shutdown
func (api EdgeAPI) Shutdown(context.Context) error {
	return nil
}

// Session returns a random UUID of api provider session
func (api EdgeAPI) Session(context.Context) (uuid.UUID, error) {
	return uuid.UUID{}, nil
}

func (api EdgeAPI) Closing(context.Context) (<-chan struct{}, error) {
	return nil, nil
}
