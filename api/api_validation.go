package api

import "context"

// Validation is an interface for validate-related operations
type Validation interface {
	// BeValidate initiates a validation request to be-validate
	Validatable(ctx context.Context, req *ValidationReq) error //perm:read
}

// BeValidateReq represents the request parameters for validation
type ValidationReq struct {
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
