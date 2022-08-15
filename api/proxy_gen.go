// Code generated by titan/gen/api. DO NOT EDIT.

package api

import (
	"context"
	"github.com/filecoin-project/go-jsonrpc/auth"
	"github.com/google/uuid"
	"github.com/linguohua/titan/journal/alerting"
	xerrors "golang.org/x/xerrors"
)


var ErrNotSupported = xerrors.New("method not supported")


type CandidateStruct struct {

	EdgeStruct

	Internal struct {

		VerifyData func(p0 context.Context, p1 []ReqVarify) ([]VarifyResult, error) `perm:"read"`

	}
}

type CandidateStub struct {

	EdgeStub

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

type DeviceStruct struct {

	Internal struct {

		DeviceInfo func(p0 context.Context) (DevicesInfo, error) `perm:"read"`

	}
}

type DeviceStub struct {

}

type EdgeStruct struct {

	CommonStruct

	DeviceStruct

	Internal struct {

		BlockStoreStat func(p0 context.Context) (error) `perm:"read"`

		CacheData func(p0 context.Context, p1 []ReqCacheData) (error) `perm:"read"`

		LoadData func(p0 context.Context, p1 string) ([]byte, error) `perm:"read"`

		LoadDataByVerifier func(p0 context.Context, p1 string) ([]byte, error) `perm:"read"`

		WaitQuiet func(p0 context.Context) (error) `perm:"read"`

	}
}

type EdgeStub struct {

	CommonStub

	DeviceStub

}

type SchedulerStruct struct {

	CommonStruct

	Internal struct {

		CacheData func(p0 context.Context, p1 []string, p2 string) (error) `perm:"read"`

		CacheResult func(p0 context.Context, p1 string, p2 string, p3 bool) (error) `perm:"read"`

		CandidateNodeConnect func(p0 context.Context, p1 string) (error) `perm:"read"`

		EdgeNodeConnect func(p0 context.Context, p1 string) (error) `perm:"read"`

		ElectionValidators func(p0 context.Context) (error) `perm:"read"`

		FindNodeWithData func(p0 context.Context, p1 string, p2 string) (string, error) `perm:"read"`

		GetCacheTag func(p0 context.Context, p1 string, p2 string) (string, error) `perm:"read"`

		GetDeviceDiagnosisDaily func(p0 context.Context, p1 IncomeDailySearch) (IncomeDailyRes, error) `perm:"read"`

		GetDeviceDiagnosisHour func(p0 context.Context, p1 IncomeDailySearch) (HourDailyRes, error) `perm:"read"`

		GetDeviceIDs func(p0 context.Context, p1 NodeTypeName) ([]string, error) `perm:"read"`

		GetDevicesCount func(p0 context.Context, p1 DevicesSearch) (DeviceType, error) `perm:"read"`

		GetDevicesInfo func(p0 context.Context, p1 DevicesSearch) (DevicesInfoPage, error) `perm:"read"`

		GetDownloadURLWithData func(p0 context.Context, p1 string, p2 string) (string, error) `perm:"read"`

		GetIndexInfo func(p0 context.Context, p1 IndexRequest) (IndexPageRes, error) `perm:"read"`

		Retrieval func(p0 context.Context, p1 IndexPageSearch) (RetrievalPageRes, error) `perm:"read"`

		SaveDailyInfo func(p0 context.Context, p1 IncomeDaily) (error) `perm:"read"`

		SpotCheck func(p0 context.Context) (error) `perm:"read"`

	}
}

type SchedulerStub struct {

	CommonStub

}





func (s *CandidateStruct) VerifyData(p0 context.Context, p1 []ReqVarify) ([]VarifyResult, error) {
	if s.Internal.VerifyData == nil {
		return *new([]VarifyResult), ErrNotSupported
	}
	return s.Internal.VerifyData(p0, p1)
}

func (s *CandidateStub) VerifyData(p0 context.Context, p1 []ReqVarify) ([]VarifyResult, error) {
	return *new([]VarifyResult), ErrNotSupported
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




func (s *DeviceStruct) DeviceInfo(p0 context.Context) (DevicesInfo, error) {
	if s.Internal.DeviceInfo == nil {
		return *new(DevicesInfo), ErrNotSupported
	}
	return s.Internal.DeviceInfo(p0)
}

func (s *DeviceStub) DeviceInfo(p0 context.Context) (DevicesInfo, error) {
	return *new(DevicesInfo), ErrNotSupported
}




func (s *EdgeStruct) BlockStoreStat(p0 context.Context) (error) {
	if s.Internal.BlockStoreStat == nil {
		return ErrNotSupported
	}
	return s.Internal.BlockStoreStat(p0)
}

func (s *EdgeStub) BlockStoreStat(p0 context.Context) (error) {
	return ErrNotSupported
}

func (s *EdgeStruct) CacheData(p0 context.Context, p1 []ReqCacheData) (error) {
	if s.Internal.CacheData == nil {
		return ErrNotSupported
	}
	return s.Internal.CacheData(p0, p1)
}

func (s *EdgeStub) CacheData(p0 context.Context, p1 []ReqCacheData) (error) {
	return ErrNotSupported
}

func (s *EdgeStruct) LoadData(p0 context.Context, p1 string) ([]byte, error) {
	if s.Internal.LoadData == nil {
		return *new([]byte), ErrNotSupported
	}
	return s.Internal.LoadData(p0, p1)
}

func (s *EdgeStub) LoadData(p0 context.Context, p1 string) ([]byte, error) {
	return *new([]byte), ErrNotSupported
}

func (s *EdgeStruct) LoadDataByVerifier(p0 context.Context, p1 string) ([]byte, error) {
	if s.Internal.LoadDataByVerifier == nil {
		return *new([]byte), ErrNotSupported
	}
	return s.Internal.LoadDataByVerifier(p0, p1)
}

func (s *EdgeStub) LoadDataByVerifier(p0 context.Context, p1 string) ([]byte, error) {
	return *new([]byte), ErrNotSupported
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




func (s *SchedulerStruct) CacheData(p0 context.Context, p1 []string, p2 string) (error) {
	if s.Internal.CacheData == nil {
		return ErrNotSupported
	}
	return s.Internal.CacheData(p0, p1, p2)
}

func (s *SchedulerStub) CacheData(p0 context.Context, p1 []string, p2 string) (error) {
	return ErrNotSupported
}

func (s *SchedulerStruct) CacheResult(p0 context.Context, p1 string, p2 string, p3 bool) (error) {
	if s.Internal.CacheResult == nil {
		return ErrNotSupported
	}
	return s.Internal.CacheResult(p0, p1, p2, p3)
}

func (s *SchedulerStub) CacheResult(p0 context.Context, p1 string, p2 string, p3 bool) (error) {
	return ErrNotSupported
}

func (s *SchedulerStruct) CandidateNodeConnect(p0 context.Context, p1 string) (error) {
	if s.Internal.CandidateNodeConnect == nil {
		return ErrNotSupported
	}
	return s.Internal.CandidateNodeConnect(p0, p1)
}

func (s *SchedulerStub) CandidateNodeConnect(p0 context.Context, p1 string) (error) {
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

func (s *SchedulerStruct) ElectionValidators(p0 context.Context) (error) {
	if s.Internal.ElectionValidators == nil {
		return ErrNotSupported
	}
	return s.Internal.ElectionValidators(p0)
}

func (s *SchedulerStub) ElectionValidators(p0 context.Context) (error) {
	return ErrNotSupported
}

func (s *SchedulerStruct) FindNodeWithData(p0 context.Context, p1 string, p2 string) (string, error) {
	if s.Internal.FindNodeWithData == nil {
		return "", ErrNotSupported
	}
	return s.Internal.FindNodeWithData(p0, p1, p2)
}

func (s *SchedulerStub) FindNodeWithData(p0 context.Context, p1 string, p2 string) (string, error) {
	return "", ErrNotSupported
}

func (s *SchedulerStruct) GetCacheTag(p0 context.Context, p1 string, p2 string) (string, error) {
	if s.Internal.GetCacheTag == nil {
		return "", ErrNotSupported
	}
	return s.Internal.GetCacheTag(p0, p1, p2)
}

func (s *SchedulerStub) GetCacheTag(p0 context.Context, p1 string, p2 string) (string, error) {
	return "", ErrNotSupported
}

func (s *SchedulerStruct) GetDeviceDiagnosisDaily(p0 context.Context, p1 IncomeDailySearch) (IncomeDailyRes, error) {
	if s.Internal.GetDeviceDiagnosisDaily == nil {
		return *new(IncomeDailyRes), ErrNotSupported
	}
	return s.Internal.GetDeviceDiagnosisDaily(p0, p1)
}

func (s *SchedulerStub) GetDeviceDiagnosisDaily(p0 context.Context, p1 IncomeDailySearch) (IncomeDailyRes, error) {
	return *new(IncomeDailyRes), ErrNotSupported
}

func (s *SchedulerStruct) GetDeviceDiagnosisHour(p0 context.Context, p1 IncomeDailySearch) (HourDailyRes, error) {
	if s.Internal.GetDeviceDiagnosisHour == nil {
		return *new(HourDailyRes), ErrNotSupported
	}
	return s.Internal.GetDeviceDiagnosisHour(p0, p1)
}

func (s *SchedulerStub) GetDeviceDiagnosisHour(p0 context.Context, p1 IncomeDailySearch) (HourDailyRes, error) {
	return *new(HourDailyRes), ErrNotSupported
}

func (s *SchedulerStruct) GetDeviceIDs(p0 context.Context, p1 NodeTypeName) ([]string, error) {
	if s.Internal.GetDeviceIDs == nil {
		return *new([]string), ErrNotSupported
	}
	return s.Internal.GetDeviceIDs(p0, p1)
}

func (s *SchedulerStub) GetDeviceIDs(p0 context.Context, p1 NodeTypeName) ([]string, error) {
	return *new([]string), ErrNotSupported
}

func (s *SchedulerStruct) GetDevicesCount(p0 context.Context, p1 DevicesSearch) (DeviceType, error) {
	if s.Internal.GetDevicesCount == nil {
		return *new(DeviceType), ErrNotSupported
	}
	return s.Internal.GetDevicesCount(p0, p1)
}

func (s *SchedulerStub) GetDevicesCount(p0 context.Context, p1 DevicesSearch) (DeviceType, error) {
	return *new(DeviceType), ErrNotSupported
}

func (s *SchedulerStruct) GetDevicesInfo(p0 context.Context, p1 DevicesSearch) (DevicesInfoPage, error) {
	if s.Internal.GetDevicesInfo == nil {
		return *new(DevicesInfoPage), ErrNotSupported
	}
	return s.Internal.GetDevicesInfo(p0, p1)
}

func (s *SchedulerStub) GetDevicesInfo(p0 context.Context, p1 DevicesSearch) (DevicesInfoPage, error) {
	return *new(DevicesInfoPage), ErrNotSupported
}

func (s *SchedulerStruct) GetDownloadURLWithData(p0 context.Context, p1 string, p2 string) (string, error) {
	if s.Internal.GetDownloadURLWithData == nil {
		return "", ErrNotSupported
	}
	return s.Internal.GetDownloadURLWithData(p0, p1, p2)
}

func (s *SchedulerStub) GetDownloadURLWithData(p0 context.Context, p1 string, p2 string) (string, error) {
	return "", ErrNotSupported
}

func (s *SchedulerStruct) GetIndexInfo(p0 context.Context, p1 IndexRequest) (IndexPageRes, error) {
	if s.Internal.GetIndexInfo == nil {
		return *new(IndexPageRes), ErrNotSupported
	}
	return s.Internal.GetIndexInfo(p0, p1)
}

func (s *SchedulerStub) GetIndexInfo(p0 context.Context, p1 IndexRequest) (IndexPageRes, error) {
	return *new(IndexPageRes), ErrNotSupported
}

func (s *SchedulerStruct) Retrieval(p0 context.Context, p1 IndexPageSearch) (RetrievalPageRes, error) {
	if s.Internal.Retrieval == nil {
		return *new(RetrievalPageRes), ErrNotSupported
	}
	return s.Internal.Retrieval(p0, p1)
}

func (s *SchedulerStub) Retrieval(p0 context.Context, p1 IndexPageSearch) (RetrievalPageRes, error) {
	return *new(RetrievalPageRes), ErrNotSupported
}

func (s *SchedulerStruct) SaveDailyInfo(p0 context.Context, p1 IncomeDaily) (error) {
	if s.Internal.SaveDailyInfo == nil {
		return ErrNotSupported
	}
	return s.Internal.SaveDailyInfo(p0, p1)
}

func (s *SchedulerStub) SaveDailyInfo(p0 context.Context, p1 IncomeDaily) (error) {
	return ErrNotSupported
}

func (s *SchedulerStruct) SpotCheck(p0 context.Context) (error) {
	if s.Internal.SpotCheck == nil {
		return ErrNotSupported
	}
	return s.Internal.SpotCheck(p0)
}

func (s *SchedulerStub) SpotCheck(p0 context.Context) (error) {
	return ErrNotSupported
}



var _ Candidate = new(CandidateStruct)
var _ Common = new(CommonStruct)
var _ Device = new(DeviceStruct)
var _ Edge = new(EdgeStruct)
var _ Scheduler = new(SchedulerStruct)


