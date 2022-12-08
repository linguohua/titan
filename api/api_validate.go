package api

import "context"

type Validate interface {
	BeValidate(ctx context.Context, reqValidate ReqValidate, candidateTcpSrvAddr string) error //perm:read
}

type ValidateTcpMsgType int

const (
	ValidateTcpMsgTypeDeviceID ValidateTcpMsgType = iota + 1
	ValidateTcpMsgTypeBlockContent
	ValidateTcpMsgTypeCancelValidate
)
