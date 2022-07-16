package client

import (
	"context"
	"net/http"

	"titan-ultra-network/api"

	"github.com/filecoin-project/go-jsonrpc"
)

// NewScheduleClient 新建调度中心clien
func NewScheduleClient(ctx context.Context, addr string, requestHeader http.Header) (api.Schedule, jsonrpc.ClientCloser, error) {
	var res api.ScheduleStruct

	closer, err := jsonrpc.NewMergeClient(ctx, addr, "Titan",
		api.GetInternalStructs(&res), requestHeader)

	return &res, closer, err
}
