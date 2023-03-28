package api

import "context"

type Validate interface {
	BeValidate(ctx context.Context, req *BeValidateReq) error //perm:read
}

type BeValidateReq struct {
	// Candidate tcp server address
	TCPSrvAddr string
	// CID        string
	RandomSeed int64
	// seconds
	Duration int
	// TODO: get carfile from random number
	CID string
}

// TODO: new tcp package, add these to tcp package
// candidate use tcp server to validate edge
type TCPMsgType int

const (
	TCPMsgTypeNodeID TCPMsgType = iota + 1
	TCPMsgTypeBlock
	TCPMsgTypeCancel
)
