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


type BlockStruct struct {

	Internal struct {

		AnnounceBlocksWasDelete func(p0 context.Context, p1 []string) ([]BlockOperationResult, error) `perm:"write"`

		BlockStoreStat func(p0 context.Context) (error) `perm:"read"`

		CacheBlocks func(p0 context.Context, p1 ReqCacheData) (error) `perm:"write"`

		DeleteAllBlocks func(p0 context.Context) (error) `perm:"admin"`

		DeleteBlocks func(p0 context.Context, p1 []string) ([]BlockOperationResult, error) `perm:"write"`

		GetBlockStoreCheckSum func(p0 context.Context) (string, error) `perm:"read"`

		GetCID func(p0 context.Context, p1 string) (string, error) `perm:"read"`

		GetFID func(p0 context.Context, p1 string) (string, error) `perm:"read"`

		LoadBlock func(p0 context.Context, p1 string) ([]byte, error) `perm:"read"`

		QueryCacheStat func(p0 context.Context) (CacheStat, error) `perm:"read"`

		QueryCachingBlocks func(p0 context.Context) (CachingBlockList, error) `perm:"read"`

		ScrubBlocks func(p0 context.Context, p1 ScrubBlocks) (error) `perm:"read"`

	}
}

type BlockStub struct {

}

type CandidateStruct struct {

	CommonStruct

	DeviceStruct

	BlockStruct

	DownloadStruct

	ValidateStruct

	Internal struct {

		ValidateBlocks func(p0 context.Context, p1 []ReqValidate) (error) `perm:"read"`

		WaitQuiet func(p0 context.Context) (error) `perm:"read"`

	}
}

type CandidateStub struct {

	CommonStub

	DeviceStub

	BlockStub

	DownloadStub

	ValidateStub

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

		Session func(p0 context.Context, p1 string) (uuid.UUID, error) `perm:"read"`

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

type DownloadStruct struct {

	Internal struct {

		GetDownloadInfo func(p0 context.Context) (DownloadInfo, error) `perm:"read"`

		SetDownloadSpeed func(p0 context.Context, p1 int64) (error) `perm:"write"`

	}
}

type DownloadStub struct {

}

type EdgeStruct struct {

	CommonStruct

	DeviceStruct

	BlockStruct

	DownloadStruct

	ValidateStruct

	Internal struct {

		WaitQuiet func(p0 context.Context) (error) `perm:"read"`

	}
}

type EdgeStub struct {

	CommonStub

	DeviceStub

	BlockStub

	DownloadStub

	ValidateStub

}

type LocationStruct struct {

	CommonStruct

	Internal struct {

		AddAccessPoints func(p0 context.Context, p1 string, p2 string, p3 int) (error) `perm:"read"`

		DeviceOffline func(p0 context.Context, p1 string) (error) `perm:"read"`

		DeviceOnline func(p0 context.Context, p1 string, p2 string, p3 int) (error) `perm:"read"`

		GetAccessPoints func(p0 context.Context, p1 string, p2 string) ([]string, error) `perm:"read"`

		GetDownloadInfoWithBlock func(p0 context.Context, p1 string) (DownloadInfo, error) `perm:"read"`

		GetDownloadInfoWithBlocks func(p0 context.Context, p1 []string) (map[string]DownloadInfo, error) `perm:"read"`

		GetDownloadInfosWithBlocks func(p0 context.Context, p1 []string) (map[string][]DownloadInfo, error) `perm:"read"`

		ListAccessPoints func(p0 context.Context) ([]string, error) `perm:"read"`

		RemoveAccessPoints func(p0 context.Context, p1 string) (error) `perm:"read"`

		ShowAccessPoint func(p0 context.Context, p1 string) (AccessPoint, error) `perm:"read"`

	}
}

type LocationStub struct {

	CommonStub

}

type SchedulerStruct struct {

	CommonStruct

	Internal struct {

		CacheCarfile func(p0 context.Context, p1 string, p2 int) (error) `perm:"admin"`

		CacheContinue func(p0 context.Context, p1 string, p2 string) (error) `perm:"admin"`

		CacheResult func(p0 context.Context, p1 string, p2 CacheResultInfo) (string, error) `perm:"write"`

		CandidateNodeConnect func(p0 context.Context, p1 int, p2 string) (string, error) `perm:"write"`

		DeleteBlockRecords func(p0 context.Context, p1 string, p2 []string) (map[string]string, error) `perm:"admin"`

		DownloadBlockResult func(p0 context.Context, p1 string, p2 string) (error) `perm:"write"`

		EdgeNodeConnect func(p0 context.Context, p1 int, p2 string) (string, error) `perm:"write"`

		ElectionValidators func(p0 context.Context) (error) `perm:"admin"`

		FindNodeWithBlock func(p0 context.Context, p1 string) (string, error) `perm:"read"`

		GetDevicesInfo func(p0 context.Context, p1 string) (DevicesInfo, error) `perm:"read"`

		GetDownloadInfoWithBlock func(p0 context.Context, p1 string) (DownloadInfo, error) `perm:"read"`

		GetDownloadInfoWithBlocks func(p0 context.Context, p1 []string) (map[string]DownloadInfo, error) `perm:"read"`

		GetDownloadInfosWithBlocks func(p0 context.Context, p1 []string) (map[string][]DownloadInfo, error) `perm:"read"`

		GetOnlineDeviceIDs func(p0 context.Context, p1 NodeTypeName) ([]string, error) `perm:"read"`

		GetToken func(p0 context.Context, p1 string, p2 string) (string, error) `perm:"write"`

		ListDatas func(p0 context.Context, p1 int) (DataListInfo, error) `perm:"read"`

		LocatorConnect func(p0 context.Context, p1 int) (error) `perm:"write"`

		QueryCacheStatWithNode func(p0 context.Context, p1 string) ([]CacheStat, error) `perm:"read"`

		QueryCachingBlocksWithNode func(p0 context.Context, p1 string) (CachingBlockList, error) `perm:"read"`

		RegisterNode func(p0 context.Context, p1 NodeType) (NodeRegisterInfo, error) `perm:"admin"`

		RemoveCache func(p0 context.Context, p1 string, p2 string) (error) `perm:"admin"`

		RemoveCarfile func(p0 context.Context, p1 string) (error) `perm:"admin"`

		ShowDataInfos func(p0 context.Context, p1 string) ([]CacheDataInfo, error) `perm:"read"`

		StateNetwork func(p0 context.Context) (StateNetwork, error) `perm:"read"`

		Validate func(p0 context.Context) (error) `perm:"admin"`

		ValidateBlockResult func(p0 context.Context, p1 ValidateResults) (error) `perm:"write"`

		ValidateSwitch func(p0 context.Context, p1 bool) (error) `perm:"admin"`

	}
}

type SchedulerStub struct {

	CommonStub

}

type ValidateStruct struct {

	Internal struct {

		BeValidate func(p0 context.Context, p1 ReqValidate, p2 string) (error) `perm:"read"`

	}
}

type ValidateStub struct {

}





func (s *BlockStruct) AnnounceBlocksWasDelete(p0 context.Context, p1 []string) ([]BlockOperationResult, error) {
	if s.Internal.AnnounceBlocksWasDelete == nil {
		return *new([]BlockOperationResult), ErrNotSupported
	}
	return s.Internal.AnnounceBlocksWasDelete(p0, p1)
}

func (s *BlockStub) AnnounceBlocksWasDelete(p0 context.Context, p1 []string) ([]BlockOperationResult, error) {
	return *new([]BlockOperationResult), ErrNotSupported
}

func (s *BlockStruct) BlockStoreStat(p0 context.Context) (error) {
	if s.Internal.BlockStoreStat == nil {
		return ErrNotSupported
	}
	return s.Internal.BlockStoreStat(p0)
}

func (s *BlockStub) BlockStoreStat(p0 context.Context) (error) {
	return ErrNotSupported
}

func (s *BlockStruct) CacheBlocks(p0 context.Context, p1 ReqCacheData) (error) {
	if s.Internal.CacheBlocks == nil {
		return ErrNotSupported
	}
	return s.Internal.CacheBlocks(p0, p1)
}

func (s *BlockStub) CacheBlocks(p0 context.Context, p1 ReqCacheData) (error) {
	return ErrNotSupported
}

func (s *BlockStruct) DeleteAllBlocks(p0 context.Context) (error) {
	if s.Internal.DeleteAllBlocks == nil {
		return ErrNotSupported
	}
	return s.Internal.DeleteAllBlocks(p0)
}

func (s *BlockStub) DeleteAllBlocks(p0 context.Context) (error) {
	return ErrNotSupported
}

func (s *BlockStruct) DeleteBlocks(p0 context.Context, p1 []string) ([]BlockOperationResult, error) {
	if s.Internal.DeleteBlocks == nil {
		return *new([]BlockOperationResult), ErrNotSupported
	}
	return s.Internal.DeleteBlocks(p0, p1)
}

func (s *BlockStub) DeleteBlocks(p0 context.Context, p1 []string) ([]BlockOperationResult, error) {
	return *new([]BlockOperationResult), ErrNotSupported
}

func (s *BlockStruct) GetBlockStoreCheckSum(p0 context.Context) (string, error) {
	if s.Internal.GetBlockStoreCheckSum == nil {
		return "", ErrNotSupported
	}
	return s.Internal.GetBlockStoreCheckSum(p0)
}

func (s *BlockStub) GetBlockStoreCheckSum(p0 context.Context) (string, error) {
	return "", ErrNotSupported
}

func (s *BlockStruct) GetCID(p0 context.Context, p1 string) (string, error) {
	if s.Internal.GetCID == nil {
		return "", ErrNotSupported
	}
	return s.Internal.GetCID(p0, p1)
}

func (s *BlockStub) GetCID(p0 context.Context, p1 string) (string, error) {
	return "", ErrNotSupported
}

func (s *BlockStruct) GetFID(p0 context.Context, p1 string) (string, error) {
	if s.Internal.GetFID == nil {
		return "", ErrNotSupported
	}
	return s.Internal.GetFID(p0, p1)
}

func (s *BlockStub) GetFID(p0 context.Context, p1 string) (string, error) {
	return "", ErrNotSupported
}

func (s *BlockStruct) LoadBlock(p0 context.Context, p1 string) ([]byte, error) {
	if s.Internal.LoadBlock == nil {
		return *new([]byte), ErrNotSupported
	}
	return s.Internal.LoadBlock(p0, p1)
}

func (s *BlockStub) LoadBlock(p0 context.Context, p1 string) ([]byte, error) {
	return *new([]byte), ErrNotSupported
}

func (s *BlockStruct) QueryCacheStat(p0 context.Context) (CacheStat, error) {
	if s.Internal.QueryCacheStat == nil {
		return *new(CacheStat), ErrNotSupported
	}
	return s.Internal.QueryCacheStat(p0)
}

func (s *BlockStub) QueryCacheStat(p0 context.Context) (CacheStat, error) {
	return *new(CacheStat), ErrNotSupported
}

func (s *BlockStruct) QueryCachingBlocks(p0 context.Context) (CachingBlockList, error) {
	if s.Internal.QueryCachingBlocks == nil {
		return *new(CachingBlockList), ErrNotSupported
	}
	return s.Internal.QueryCachingBlocks(p0)
}

func (s *BlockStub) QueryCachingBlocks(p0 context.Context) (CachingBlockList, error) {
	return *new(CachingBlockList), ErrNotSupported
}

func (s *BlockStruct) ScrubBlocks(p0 context.Context, p1 ScrubBlocks) (error) {
	if s.Internal.ScrubBlocks == nil {
		return ErrNotSupported
	}
	return s.Internal.ScrubBlocks(p0, p1)
}

func (s *BlockStub) ScrubBlocks(p0 context.Context, p1 ScrubBlocks) (error) {
	return ErrNotSupported
}




func (s *CandidateStruct) ValidateBlocks(p0 context.Context, p1 []ReqValidate) (error) {
	if s.Internal.ValidateBlocks == nil {
		return ErrNotSupported
	}
	return s.Internal.ValidateBlocks(p0, p1)
}

func (s *CandidateStub) ValidateBlocks(p0 context.Context, p1 []ReqValidate) (error) {
	return ErrNotSupported
}

func (s *CandidateStruct) WaitQuiet(p0 context.Context) (error) {
	if s.Internal.WaitQuiet == nil {
		return ErrNotSupported
	}
	return s.Internal.WaitQuiet(p0)
}

func (s *CandidateStub) WaitQuiet(p0 context.Context) (error) {
	return ErrNotSupported
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

func (s *CommonStruct) Session(p0 context.Context, p1 string) (uuid.UUID, error) {
	if s.Internal.Session == nil {
		return *new(uuid.UUID), ErrNotSupported
	}
	return s.Internal.Session(p0, p1)
}

func (s *CommonStub) Session(p0 context.Context, p1 string) (uuid.UUID, error) {
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




func (s *DownloadStruct) GetDownloadInfo(p0 context.Context) (DownloadInfo, error) {
	if s.Internal.GetDownloadInfo == nil {
		return *new(DownloadInfo), ErrNotSupported
	}
	return s.Internal.GetDownloadInfo(p0)
}

func (s *DownloadStub) GetDownloadInfo(p0 context.Context) (DownloadInfo, error) {
	return *new(DownloadInfo), ErrNotSupported
}

func (s *DownloadStruct) SetDownloadSpeed(p0 context.Context, p1 int64) (error) {
	if s.Internal.SetDownloadSpeed == nil {
		return ErrNotSupported
	}
	return s.Internal.SetDownloadSpeed(p0, p1)
}

func (s *DownloadStub) SetDownloadSpeed(p0 context.Context, p1 int64) (error) {
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




func (s *LocationStruct) AddAccessPoints(p0 context.Context, p1 string, p2 string, p3 int) (error) {
	if s.Internal.AddAccessPoints == nil {
		return ErrNotSupported
	}
	return s.Internal.AddAccessPoints(p0, p1, p2, p3)
}

func (s *LocationStub) AddAccessPoints(p0 context.Context, p1 string, p2 string, p3 int) (error) {
	return ErrNotSupported
}

func (s *LocationStruct) DeviceOffline(p0 context.Context, p1 string) (error) {
	if s.Internal.DeviceOffline == nil {
		return ErrNotSupported
	}
	return s.Internal.DeviceOffline(p0, p1)
}

func (s *LocationStub) DeviceOffline(p0 context.Context, p1 string) (error) {
	return ErrNotSupported
}

func (s *LocationStruct) DeviceOnline(p0 context.Context, p1 string, p2 string, p3 int) (error) {
	if s.Internal.DeviceOnline == nil {
		return ErrNotSupported
	}
	return s.Internal.DeviceOnline(p0, p1, p2, p3)
}

func (s *LocationStub) DeviceOnline(p0 context.Context, p1 string, p2 string, p3 int) (error) {
	return ErrNotSupported
}

func (s *LocationStruct) GetAccessPoints(p0 context.Context, p1 string, p2 string) ([]string, error) {
	if s.Internal.GetAccessPoints == nil {
		return *new([]string), ErrNotSupported
	}
	return s.Internal.GetAccessPoints(p0, p1, p2)
}

func (s *LocationStub) GetAccessPoints(p0 context.Context, p1 string, p2 string) ([]string, error) {
	return *new([]string), ErrNotSupported
}

func (s *LocationStruct) GetDownloadInfoWithBlock(p0 context.Context, p1 string) (DownloadInfo, error) {
	if s.Internal.GetDownloadInfoWithBlock == nil {
		return *new(DownloadInfo), ErrNotSupported
	}
	return s.Internal.GetDownloadInfoWithBlock(p0, p1)
}

func (s *LocationStub) GetDownloadInfoWithBlock(p0 context.Context, p1 string) (DownloadInfo, error) {
	return *new(DownloadInfo), ErrNotSupported
}

func (s *LocationStruct) GetDownloadInfoWithBlocks(p0 context.Context, p1 []string) (map[string]DownloadInfo, error) {
	if s.Internal.GetDownloadInfoWithBlocks == nil {
		return *new(map[string]DownloadInfo), ErrNotSupported
	}
	return s.Internal.GetDownloadInfoWithBlocks(p0, p1)
}

func (s *LocationStub) GetDownloadInfoWithBlocks(p0 context.Context, p1 []string) (map[string]DownloadInfo, error) {
	return *new(map[string]DownloadInfo), ErrNotSupported
}

func (s *LocationStruct) GetDownloadInfosWithBlocks(p0 context.Context, p1 []string) (map[string][]DownloadInfo, error) {
	if s.Internal.GetDownloadInfosWithBlocks == nil {
		return *new(map[string][]DownloadInfo), ErrNotSupported
	}
	return s.Internal.GetDownloadInfosWithBlocks(p0, p1)
}

func (s *LocationStub) GetDownloadInfosWithBlocks(p0 context.Context, p1 []string) (map[string][]DownloadInfo, error) {
	return *new(map[string][]DownloadInfo), ErrNotSupported
}

func (s *LocationStruct) ListAccessPoints(p0 context.Context) ([]string, error) {
	if s.Internal.ListAccessPoints == nil {
		return *new([]string), ErrNotSupported
	}
	return s.Internal.ListAccessPoints(p0)
}

func (s *LocationStub) ListAccessPoints(p0 context.Context) ([]string, error) {
	return *new([]string), ErrNotSupported
}

func (s *LocationStruct) RemoveAccessPoints(p0 context.Context, p1 string) (error) {
	if s.Internal.RemoveAccessPoints == nil {
		return ErrNotSupported
	}
	return s.Internal.RemoveAccessPoints(p0, p1)
}

func (s *LocationStub) RemoveAccessPoints(p0 context.Context, p1 string) (error) {
	return ErrNotSupported
}

func (s *LocationStruct) ShowAccessPoint(p0 context.Context, p1 string) (AccessPoint, error) {
	if s.Internal.ShowAccessPoint == nil {
		return *new(AccessPoint), ErrNotSupported
	}
	return s.Internal.ShowAccessPoint(p0, p1)
}

func (s *LocationStub) ShowAccessPoint(p0 context.Context, p1 string) (AccessPoint, error) {
	return *new(AccessPoint), ErrNotSupported
}




func (s *SchedulerStruct) CacheCarfile(p0 context.Context, p1 string, p2 int) (error) {
	if s.Internal.CacheCarfile == nil {
		return ErrNotSupported
	}
	return s.Internal.CacheCarfile(p0, p1, p2)
}

func (s *SchedulerStub) CacheCarfile(p0 context.Context, p1 string, p2 int) (error) {
	return ErrNotSupported
}

func (s *SchedulerStruct) CacheContinue(p0 context.Context, p1 string, p2 string) (error) {
	if s.Internal.CacheContinue == nil {
		return ErrNotSupported
	}
	return s.Internal.CacheContinue(p0, p1, p2)
}

func (s *SchedulerStub) CacheContinue(p0 context.Context, p1 string, p2 string) (error) {
	return ErrNotSupported
}

func (s *SchedulerStruct) CacheResult(p0 context.Context, p1 string, p2 CacheResultInfo) (string, error) {
	if s.Internal.CacheResult == nil {
		return "", ErrNotSupported
	}
	return s.Internal.CacheResult(p0, p1, p2)
}

func (s *SchedulerStub) CacheResult(p0 context.Context, p1 string, p2 CacheResultInfo) (string, error) {
	return "", ErrNotSupported
}

func (s *SchedulerStruct) CandidateNodeConnect(p0 context.Context, p1 int, p2 string) (string, error) {
	if s.Internal.CandidateNodeConnect == nil {
		return "", ErrNotSupported
	}
	return s.Internal.CandidateNodeConnect(p0, p1, p2)
}

func (s *SchedulerStub) CandidateNodeConnect(p0 context.Context, p1 int, p2 string) (string, error) {
	return "", ErrNotSupported
}

func (s *SchedulerStruct) DeleteBlockRecords(p0 context.Context, p1 string, p2 []string) (map[string]string, error) {
	if s.Internal.DeleteBlockRecords == nil {
		return *new(map[string]string), ErrNotSupported
	}
	return s.Internal.DeleteBlockRecords(p0, p1, p2)
}

func (s *SchedulerStub) DeleteBlockRecords(p0 context.Context, p1 string, p2 []string) (map[string]string, error) {
	return *new(map[string]string), ErrNotSupported
}

func (s *SchedulerStruct) DownloadBlockResult(p0 context.Context, p1 string, p2 string) (error) {
	if s.Internal.DownloadBlockResult == nil {
		return ErrNotSupported
	}
	return s.Internal.DownloadBlockResult(p0, p1, p2)
}

func (s *SchedulerStub) DownloadBlockResult(p0 context.Context, p1 string, p2 string) (error) {
	return ErrNotSupported
}

func (s *SchedulerStruct) EdgeNodeConnect(p0 context.Context, p1 int, p2 string) (string, error) {
	if s.Internal.EdgeNodeConnect == nil {
		return "", ErrNotSupported
	}
	return s.Internal.EdgeNodeConnect(p0, p1, p2)
}

func (s *SchedulerStub) EdgeNodeConnect(p0 context.Context, p1 int, p2 string) (string, error) {
	return "", ErrNotSupported
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

func (s *SchedulerStruct) FindNodeWithBlock(p0 context.Context, p1 string) (string, error) {
	if s.Internal.FindNodeWithBlock == nil {
		return "", ErrNotSupported
	}
	return s.Internal.FindNodeWithBlock(p0, p1)
}

func (s *SchedulerStub) FindNodeWithBlock(p0 context.Context, p1 string) (string, error) {
	return "", ErrNotSupported
}

func (s *SchedulerStruct) GetDevicesInfo(p0 context.Context, p1 string) (DevicesInfo, error) {
	if s.Internal.GetDevicesInfo == nil {
		return *new(DevicesInfo), ErrNotSupported
	}
	return s.Internal.GetDevicesInfo(p0, p1)
}

func (s *SchedulerStub) GetDevicesInfo(p0 context.Context, p1 string) (DevicesInfo, error) {
	return *new(DevicesInfo), ErrNotSupported
}

func (s *SchedulerStruct) GetDownloadInfoWithBlock(p0 context.Context, p1 string) (DownloadInfo, error) {
	if s.Internal.GetDownloadInfoWithBlock == nil {
		return *new(DownloadInfo), ErrNotSupported
	}
	return s.Internal.GetDownloadInfoWithBlock(p0, p1)
}

func (s *SchedulerStub) GetDownloadInfoWithBlock(p0 context.Context, p1 string) (DownloadInfo, error) {
	return *new(DownloadInfo), ErrNotSupported
}

func (s *SchedulerStruct) GetDownloadInfoWithBlocks(p0 context.Context, p1 []string) (map[string]DownloadInfo, error) {
	if s.Internal.GetDownloadInfoWithBlocks == nil {
		return *new(map[string]DownloadInfo), ErrNotSupported
	}
	return s.Internal.GetDownloadInfoWithBlocks(p0, p1)
}

func (s *SchedulerStub) GetDownloadInfoWithBlocks(p0 context.Context, p1 []string) (map[string]DownloadInfo, error) {
	return *new(map[string]DownloadInfo), ErrNotSupported
}

func (s *SchedulerStruct) GetDownloadInfosWithBlocks(p0 context.Context, p1 []string) (map[string][]DownloadInfo, error) {
	if s.Internal.GetDownloadInfosWithBlocks == nil {
		return *new(map[string][]DownloadInfo), ErrNotSupported
	}
	return s.Internal.GetDownloadInfosWithBlocks(p0, p1)
}

func (s *SchedulerStub) GetDownloadInfosWithBlocks(p0 context.Context, p1 []string) (map[string][]DownloadInfo, error) {
	return *new(map[string][]DownloadInfo), ErrNotSupported
}

func (s *SchedulerStruct) GetOnlineDeviceIDs(p0 context.Context, p1 NodeTypeName) ([]string, error) {
	if s.Internal.GetOnlineDeviceIDs == nil {
		return *new([]string), ErrNotSupported
	}
	return s.Internal.GetOnlineDeviceIDs(p0, p1)
}

func (s *SchedulerStub) GetOnlineDeviceIDs(p0 context.Context, p1 NodeTypeName) ([]string, error) {
	return *new([]string), ErrNotSupported
}

func (s *SchedulerStruct) GetToken(p0 context.Context, p1 string, p2 string) (string, error) {
	if s.Internal.GetToken == nil {
		return "", ErrNotSupported
	}
	return s.Internal.GetToken(p0, p1, p2)
}

func (s *SchedulerStub) GetToken(p0 context.Context, p1 string, p2 string) (string, error) {
	return "", ErrNotSupported
}

func (s *SchedulerStruct) ListDatas(p0 context.Context, p1 int) (DataListInfo, error) {
	if s.Internal.ListDatas == nil {
		return *new(DataListInfo), ErrNotSupported
	}
	return s.Internal.ListDatas(p0, p1)
}

func (s *SchedulerStub) ListDatas(p0 context.Context, p1 int) (DataListInfo, error) {
	return *new(DataListInfo), ErrNotSupported
}

func (s *SchedulerStruct) LocatorConnect(p0 context.Context, p1 int) (error) {
	if s.Internal.LocatorConnect == nil {
		return ErrNotSupported
	}
	return s.Internal.LocatorConnect(p0, p1)
}

func (s *SchedulerStub) LocatorConnect(p0 context.Context, p1 int) (error) {
	return ErrNotSupported
}

func (s *SchedulerStruct) QueryCacheStatWithNode(p0 context.Context, p1 string) ([]CacheStat, error) {
	if s.Internal.QueryCacheStatWithNode == nil {
		return *new([]CacheStat), ErrNotSupported
	}
	return s.Internal.QueryCacheStatWithNode(p0, p1)
}

func (s *SchedulerStub) QueryCacheStatWithNode(p0 context.Context, p1 string) ([]CacheStat, error) {
	return *new([]CacheStat), ErrNotSupported
}

func (s *SchedulerStruct) QueryCachingBlocksWithNode(p0 context.Context, p1 string) (CachingBlockList, error) {
	if s.Internal.QueryCachingBlocksWithNode == nil {
		return *new(CachingBlockList), ErrNotSupported
	}
	return s.Internal.QueryCachingBlocksWithNode(p0, p1)
}

func (s *SchedulerStub) QueryCachingBlocksWithNode(p0 context.Context, p1 string) (CachingBlockList, error) {
	return *new(CachingBlockList), ErrNotSupported
}

func (s *SchedulerStruct) RegisterNode(p0 context.Context, p1 NodeType) (NodeRegisterInfo, error) {
	if s.Internal.RegisterNode == nil {
		return *new(NodeRegisterInfo), ErrNotSupported
	}
	return s.Internal.RegisterNode(p0, p1)
}

func (s *SchedulerStub) RegisterNode(p0 context.Context, p1 NodeType) (NodeRegisterInfo, error) {
	return *new(NodeRegisterInfo), ErrNotSupported
}

func (s *SchedulerStruct) RemoveCache(p0 context.Context, p1 string, p2 string) (error) {
	if s.Internal.RemoveCache == nil {
		return ErrNotSupported
	}
	return s.Internal.RemoveCache(p0, p1, p2)
}

func (s *SchedulerStub) RemoveCache(p0 context.Context, p1 string, p2 string) (error) {
	return ErrNotSupported
}

func (s *SchedulerStruct) RemoveCarfile(p0 context.Context, p1 string) (error) {
	if s.Internal.RemoveCarfile == nil {
		return ErrNotSupported
	}
	return s.Internal.RemoveCarfile(p0, p1)
}

func (s *SchedulerStub) RemoveCarfile(p0 context.Context, p1 string) (error) {
	return ErrNotSupported
}

func (s *SchedulerStruct) ShowDataInfos(p0 context.Context, p1 string) ([]CacheDataInfo, error) {
	if s.Internal.ShowDataInfos == nil {
		return *new([]CacheDataInfo), ErrNotSupported
	}
	return s.Internal.ShowDataInfos(p0, p1)
}

func (s *SchedulerStub) ShowDataInfos(p0 context.Context, p1 string) ([]CacheDataInfo, error) {
	return *new([]CacheDataInfo), ErrNotSupported
}

func (s *SchedulerStruct) StateNetwork(p0 context.Context) (StateNetwork, error) {
	if s.Internal.StateNetwork == nil {
		return *new(StateNetwork), ErrNotSupported
	}
	return s.Internal.StateNetwork(p0)
}

func (s *SchedulerStub) StateNetwork(p0 context.Context) (StateNetwork, error) {
	return *new(StateNetwork), ErrNotSupported
}

func (s *SchedulerStruct) Validate(p0 context.Context) (error) {
	if s.Internal.Validate == nil {
		return ErrNotSupported
	}
	return s.Internal.Validate(p0)
}

func (s *SchedulerStub) Validate(p0 context.Context) (error) {
	return ErrNotSupported
}

func (s *SchedulerStruct) ValidateBlockResult(p0 context.Context, p1 ValidateResults) (error) {
	if s.Internal.ValidateBlockResult == nil {
		return ErrNotSupported
	}
	return s.Internal.ValidateBlockResult(p0, p1)
}

func (s *SchedulerStub) ValidateBlockResult(p0 context.Context, p1 ValidateResults) (error) {
	return ErrNotSupported
}

func (s *SchedulerStruct) ValidateSwitch(p0 context.Context, p1 bool) (error) {
	if s.Internal.ValidateSwitch == nil {
		return ErrNotSupported
	}
	return s.Internal.ValidateSwitch(p0, p1)
}

func (s *SchedulerStub) ValidateSwitch(p0 context.Context, p1 bool) (error) {
	return ErrNotSupported
}




func (s *ValidateStruct) BeValidate(p0 context.Context, p1 ReqValidate, p2 string) (error) {
	if s.Internal.BeValidate == nil {
		return ErrNotSupported
	}
	return s.Internal.BeValidate(p0, p1, p2)
}

func (s *ValidateStub) BeValidate(p0 context.Context, p1 ReqValidate, p2 string) (error) {
	return ErrNotSupported
}



var _ Block = new(BlockStruct)
var _ Candidate = new(CandidateStruct)
var _ Common = new(CommonStruct)
var _ Device = new(DeviceStruct)
var _ Download = new(DownloadStruct)
var _ Edge = new(EdgeStruct)
var _ Location = new(LocationStruct)
var _ Scheduler = new(SchedulerStruct)
var _ Validate = new(ValidateStruct)


