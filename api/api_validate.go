package api

import "context"

type Validate interface {
	BeValidate(ctx context.Context, reqValidate ReqValidate, candidateTCPSrvAddr string) error //perm:read
}

// TODO: new tcp package, add thoes to tcp package
// candidate use tcp server to validate edge
type TCPMsgType int

const (
	TCPMsgTypeNodeID TCPMsgType = iota + 1
	TCPMsgTypeBlock
	TCPMsgTypeCancel
)
