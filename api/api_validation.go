package api

import "context"

// Validation is an interface for validate-related operations
type Validation interface {
	// Validatable initiates a validation request to be-validate
	Validatable(ctx context.Context, req *ValidationReq) error //perm:read
}

// ValidationReq represents the request parameters for validation
type ValidationReq struct {
	// TCPSrvAddr Candidate tcp server address
	TCPSrvAddr string
	RandomSeed int64
	Duration   int
}

// TODO: new tcp package, add these to tcp package
// candidate use tcp server to validate edge
type TCPMsgType int

const (
	TCPMsgTypeNodeID TCPMsgType = iota + 1
	TCPMsgTypeBlock
	TCPMsgTypeCancel
)
