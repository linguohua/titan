package client

import (
	"context"
	"net/http"
	"net/url"
	"path"
	"time"

	"github.com/linguohua/titan/api"

	"github.com/filecoin-project/go-jsonrpc"

	"github.com/linguohua/titan/lib/rpcenc"
)

// NewScheduler creates a new http jsonrpc client.
func NewScheduler(ctx context.Context, addr string, requestHeader http.Header) (api.Scheduler, jsonrpc.ClientCloser, error) {
	var res api.SchedulerStruct

	closer, err := jsonrpc.NewMergeClient(ctx, addr, "titan",
		api.GetInternalStructs(&res), requestHeader)

	return &res, closer, err
}

func getPushUrl(addr string) (string, error) {
	pushUrl, err := url.Parse(addr)
	if err != nil {
		return "", err
	}
	switch pushUrl.Scheme {
	case "ws":
		pushUrl.Scheme = "http"
	case "wss":
		pushUrl.Scheme = "https"
	}
	///rpc/v0 -> /rpc/streams/v0/push

	pushUrl.Path = path.Join(pushUrl.Path, "../streams/v0/push")
	return pushUrl.String(), nil
}

// NewCandicate creates a new http jsonrpc client for candidate
func NewCandicate(ctx context.Context, addr string, requestHeader http.Header, opts ...jsonrpc.Option) (api.Candidate, jsonrpc.ClientCloser, error) {
	pushUrl, err := getPushUrl(addr)
	if err != nil {
		return nil, nil, err
	}

	var res api.CandidateStruct
	closer, err := jsonrpc.NewMergeClient(ctx, addr, "titan",
		api.GetInternalStructs(&res), requestHeader,
		append([]jsonrpc.Option{
			rpcenc.ReaderParamEncoder(pushUrl),
		}, opts...)...)

	return &res, closer, err
}

func NewEdge(ctx context.Context, addr string, requestHeader http.Header) (api.Edge, jsonrpc.ClientCloser, error) {
	pushUrl, err := getPushUrl(addr)
	if err != nil {
		return nil, nil, err
	}

	var res api.EdgeStruct
	closer, err := jsonrpc.NewMergeClient(ctx, addr, "titan",
		api.GetInternalStructs(&res),
		requestHeader,
		rpcenc.ReaderParamEncoder(pushUrl),
		jsonrpc.WithNoReconnect(),
		jsonrpc.WithTimeout(30*time.Second),
	)

	return &res, closer, err
}

// custom http client for edge client
func NewEdgeWithHttpClient(ctx context.Context, addr string, requestHeader http.Header, httpClient *http.Client) (api.Edge, jsonrpc.ClientCloser, error) {
	pushUrl, err := getPushUrl(addr)
	if err != nil {
		return nil, nil, err
	}

	var res api.EdgeStruct
	closer, err := jsonrpc.NewMergeClient(ctx, addr, "titan",
		api.GetInternalStructs(&res),
		requestHeader,
		rpcenc.ReaderParamEncoder(pushUrl),
		jsonrpc.WithNoReconnect(),
		jsonrpc.WithTimeout(30*time.Second),
		jsonrpc.WithHTTPClient(httpClient),
	)

	return &res, closer, err
}

// NewCommonRPCV0 creates a new http jsonrpc client.
func NewCommonRPCV0(ctx context.Context, addr string, requestHeader http.Header) (api.Common, jsonrpc.ClientCloser, error) {
	var res api.CommonStruct
	closer, err := jsonrpc.NewMergeClient(ctx, addr, "titan",
		api.GetInternalStructs(&res), requestHeader)

	return &res, closer, err
}

func NewLocator(ctx context.Context, addr string, requestHeader http.Header) (api.Locator, jsonrpc.ClientCloser, error) {
	pushUrl, err := getPushUrl(addr)
	if err != nil {
		return nil, nil, err
	}

	var res api.LocatorStruct
	closer, err := jsonrpc.NewMergeClient(ctx, addr, "titan",
		api.GetInternalStructs(&res),
		requestHeader,
		rpcenc.ReaderParamEncoder(pushUrl),
		jsonrpc.WithNoReconnect(),
		jsonrpc.WithTimeout(30*time.Second),
	)

	return &res, closer, err
}
