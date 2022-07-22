// Code generated by titan-ultra-network/gen/api. DO NOT EDIT.

package api

import (
	"context"
	"github.com/filecoin-project/go-jsonrpc/auth"
	"github.com/google/uuid"
	xerrors "golang.org/x/xerrors"
	"titan-ultra-network/journal/alerting"

)


var ErrNotSupported = xerrors.New("method not supported")


type CandidateStruct struct {

	CommonStruct

	Internal struct {

	}
}

type CandidateStub struct {

	CommonStub

}

type CommonStruct struct {

	Internal struct {

		AuthNew func(p0 context.Context, p1 []auth.Permission) ([]byte, error) `perm:"admin"`

		AuthVerify func(p0 context.Context, p1 string) ([]auth.Permission, error) `perm:"read"`

		Closing func(p0 context.Context) (<-chan struct{}, error) `perm:"read"`

		Discover func(p0 context.Context) (OpenRPCDocument, error) `perm:"read"`

		LogAlerts func(p0 context.Context) ([]alerting.Alert, error) `perm:"admin"`

		LogList func(p0 context.Context) ([]string, error) `perm:"write"`

		LogSetLevel func(p0 context.Context, p1 string, p2 string) (error) `perm:"write"`

		Session func(p0 context.Context) (uuid.UUID, error) `perm:"read"`

		Shutdown func(p0 context.Context) (error) `perm:"admin"`

		Version func(p0 context.Context) (APIVersion, error) `perm:"read"`

	}
}

type CommonStub struct {

}

type EdgeStruct struct {

	CommonStruct

	Internal struct {

		Save func(p0 context.Context) (error) ``

		WaitQuiet func(p0 context.Context) (error) ``

	}
}

type EdgeStub struct {

	CommonStub

}

type SchedulerStruct struct {

	CommonStruct

	Internal struct {

		EdgeNodeConnect func(p0 context.Context, p1 string) (error) ``

	}
}

type SchedulerStub struct {

	CommonStub

}








func (s *CommonStruct) AuthNew(p0 context.Context, p1 []auth.Permission) ([]byte, error) {
	if s.Internal.AuthNew == nil {
		return *new([]byte), ErrNotSupported
	}
	return s.Internal.AuthNew(p0, p1)
}

func (s *CommonStub) AuthNew(p0 context.Context, p1 []auth.Permission) ([]byte, error) {
	return *new([]byte), ErrNotSupported
}

func (s *CommonStruct) AuthVerify(p0 context.Context, p1 string) ([]auth.Permission, error) {
	if s.Internal.AuthVerify == nil {
		return *new([]auth.Permission), ErrNotSupported
	}
	return s.Internal.AuthVerify(p0, p1)
}

func (s *CommonStub) AuthVerify(p0 context.Context, p1 string) ([]auth.Permission, error) {
	return *new([]auth.Permission), ErrNotSupported
}

func (s *CommonStruct) Closing(p0 context.Context) (<-chan struct{}, error) {
	if s.Internal.Closing == nil {
		return nil, ErrNotSupported
	}
	return s.Internal.Closing(p0)
}

func (s *CommonStub) Closing(p0 context.Context) (<-chan struct{}, error) {
	return nil, ErrNotSupported
}

func (s *CommonStruct) Discover(p0 context.Context) (OpenRPCDocument, error) {
	if s.Internal.Discover == nil {
		return *new(OpenRPCDocument), ErrNotSupported
	}
	return s.Internal.Discover(p0)
}

func (s *CommonStub) Discover(p0 context.Context) (OpenRPCDocument, error) {
	return *new(OpenRPCDocument), ErrNotSupported
}

func (s *CommonStruct) LogAlerts(p0 context.Context) ([]alerting.Alert, error) {
	if s.Internal.LogAlerts == nil {
		return *new([]alerting.Alert), ErrNotSupported
	}
	return s.Internal.LogAlerts(p0)
}

func (s *CommonStub) LogAlerts(p0 context.Context) ([]alerting.Alert, error) {
	return *new([]alerting.Alert), ErrNotSupported
}

func (s *CommonStruct) LogList(p0 context.Context) ([]string, error) {
	if s.Internal.LogList == nil {
		return *new([]string), ErrNotSupported
	}
	return s.Internal.LogList(p0)
}

func (s *CommonStub) LogList(p0 context.Context) ([]string, error) {
	return *new([]string), ErrNotSupported
}

func (s *CommonStruct) LogSetLevel(p0 context.Context, p1 string, p2 string) (error) {
	if s.Internal.LogSetLevel == nil {
		return ErrNotSupported
	}
	return s.Internal.LogSetLevel(p0, p1, p2)
}

func (s *CommonStub) LogSetLevel(p0 context.Context, p1 string, p2 string) (error) {
	return ErrNotSupported
}

func (s *CommonStruct) Session(p0 context.Context) (uuid.UUID, error) {
	if s.Internal.Session == nil {
		return *new(uuid.UUID), ErrNotSupported
	}
	return s.Internal.Session(p0)
}

func (s *CommonStub) Session(p0 context.Context) (uuid.UUID, error) {
	return *new(uuid.UUID), ErrNotSupported
}

func (s *CommonStruct) Shutdown(p0 context.Context) (error) {
	if s.Internal.Shutdown == nil {
		return ErrNotSupported
	}
	return s.Internal.Shutdown(p0)
}

func (s *CommonStub) Shutdown(p0 context.Context) (error) {
	return ErrNotSupported
}

func (s *CommonStruct) Version(p0 context.Context) (APIVersion, error) {
	if s.Internal.Version == nil {
		return *new(APIVersion), ErrNotSupported
	}
	return s.Internal.Version(p0)
}

func (s *CommonStub) Version(p0 context.Context) (APIVersion, error) {
	return *new(APIVersion), ErrNotSupported
}




func (s *EdgeStruct) Save(p0 context.Context) (error) {
	if s.Internal.Save == nil {
		return ErrNotSupported
	}
	return s.Internal.Save(p0)
}

func (s *EdgeStub) Save(p0 context.Context) (error) {
	return ErrNotSupported
}

func (s *EdgeStruct) WaitQuiet(p0 context.Context) (error) {
	if s.Internal.WaitQuiet == nil {
		return ErrNotSupported
	}
	return s.Internal.WaitQuiet(p0)
}

func (s *EdgeStub) WaitQuiet(p0 context.Context) (error) {
	return ErrNotSupported
}




func (s *SchedulerStruct) EdgeNodeConnect(p0 context.Context, p1 string) (error) {
	if s.Internal.EdgeNodeConnect == nil {
		return ErrNotSupported
	}
	return s.Internal.EdgeNodeConnect(p0, p1)
}

func (s *SchedulerStub) EdgeNodeConnect(p0 context.Context, p1 string) (error) {
	return ErrNotSupported
}



var _ Candidate = new(CandidateStruct)
var _ Common = new(CommonStruct)
var _ Edge = new(EdgeStruct)
var _ Scheduler = new(SchedulerStruct)


