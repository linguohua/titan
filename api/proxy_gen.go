// Code generated by titan/gen/api. DO NOT EDIT.

package api

import (
	"context"
	"time"

	"github.com/filecoin-project/go-jsonrpc/auth"
	"github.com/google/uuid"
	"github.com/linguohua/titan/api/types"
	"github.com/linguohua/titan/journal/alerting"
	xerrors "golang.org/x/xerrors"
)

var ErrNotSupported = xerrors.New("method not supported")

type CandidateStruct struct {
	CommonStruct

	DeviceStruct

	ValidateStruct

	DataSyncStruct

	CarfileOperationStruct

	Internal struct {
		GetBlock func(p0 context.Context, p1 string) ([]byte, error) `perm:"read"`

		GetBlocksOfCarfile func(p0 context.Context, p1 string, p2 int64, p3 int) (map[int]string, error) `perm:"read"`

		ValidateNodes func(p0 context.Context, p1 []ReqValidate) error `perm:"read"`

		WaitQuiet func(p0 context.Context) error `perm:"read"`
	}
}

type CandidateStub struct {
	CommonStub

	DeviceStub

	ValidateStub

	DataSyncStub

	CarfileOperationStub
}

type CarfileOperationStruct struct {
	Internal struct {
		CacheCarfile func(p0 context.Context, p1 string, p2 []*types.DownloadSource) error `perm:"write"`

		CachedProgresses func(p0 context.Context, p1 []string) (*types.CacheResult, error) `perm:"write"`

		DeleteAllCarfiles func(p0 context.Context) error `perm:"admin"`

		DeleteCarfile func(p0 context.Context, p1 string) error `perm:"write"`

		QueryCacheStat func(p0 context.Context) (*types.CacheStat, error) `perm:"write"`

		QueryCachingCarfile func(p0 context.Context) (*types.CachingCarfile, error) `perm:"write"`
	}
}

type CarfileOperationStub struct {
}

type CommonStruct struct {
	Internal struct {
		AuthNew func(p0 context.Context, p1 []auth.Permission) (string, error) `perm:"admin"`

		AuthVerify func(p0 context.Context, p1 string) ([]auth.Permission, error) `perm:"read"`

		Closing func(p0 context.Context) (<-chan struct{}, error) `perm:"read"`

		DeleteLogFile func(p0 context.Context) error `perm:"write"`

		Discover func(p0 context.Context) (types.OpenRPCDocument, error) `perm:"read"`

		DownloadLogFile func(p0 context.Context) ([]byte, error) `perm:"write"`

		LogAlerts func(p0 context.Context) ([]alerting.Alert, error) `perm:"admin"`

		LogList func(p0 context.Context) ([]string, error) `perm:"write"`

		LogSetLevel func(p0 context.Context, p1 string, p2 string) error `perm:"write"`

		Session func(p0 context.Context) (uuid.UUID, error) `perm:"read"`

		ShowLogFile func(p0 context.Context) (*LogFile, error) `perm:"write"`

		Shutdown func(p0 context.Context) error `perm:"admin"`

		Version func(p0 context.Context) (APIVersion, error) `perm:"read"`
	}
}

type CommonStub struct {
}

type DataSyncStruct struct {
	Internal struct {
		CompareCarfiles func(p0 context.Context, p1 uint32, p2 map[uint32][]string) error `perm:"write"`

		CompareChecksums func(p0 context.Context, p1 uint32, p2 map[uint32]string) ([]uint32, error) `perm:"write"`
	}
}

type DataSyncStub struct {
}

type DeviceStruct struct {
	Internal struct {
		NodeID func(p0 context.Context) (string, error) `perm:"read"`

		NodeInfo func(p0 context.Context) (types.NodeInfo, error) `perm:"read"`
	}
}

type DeviceStub struct {
}

type EdgeStruct struct {
	CommonStruct

	DeviceStruct

	ValidateStruct

	DataSyncStruct

	CarfileOperationStruct

	Internal struct {
		ExternalServiceAddress func(p0 context.Context, p1 string) (string, error) `perm:"write"`

		UserNATTravel func(p0 context.Context, p1 string) error `perm:"write"`

		WaitQuiet func(p0 context.Context) error `perm:"read"`
	}
}

type EdgeStub struct {
	CommonStub

	DeviceStub

	ValidateStub

	DataSyncStub

	CarfileOperationStub
}

type LocatorStruct struct {
	CommonStruct

	Internal struct {
		AddAccessPoint func(p0 context.Context, p1 string, p2 string, p3 int, p4 string) error `perm:"admin"`

		AllocateNodes func(p0 context.Context, p1 string, p2 types.NodeType, p3 int) ([]*types.NodeAllocateInfo, error) `perm:"admin"`

		EdgeDownloadInfos func(p0 context.Context, p1 string) ([]*types.DownloadInfo, error) `perm:"read"`

		GetAccessPoints func(p0 context.Context, p1 string) ([]string, error) `perm:"read"`

		ListAreaIDs func(p0 context.Context) ([]string, error) `perm:"admin"`

		LoadAccessPointsForWeb func(p0 context.Context) ([]AccessPoint, error) `perm:"admin"`

		LoadUserAccessPoint func(p0 context.Context, p1 string) (AccessPoint, error) `perm:"admin"`

		RemoveAccessPoints func(p0 context.Context, p1 string) error `perm:"admin"`

		SetNodeOnlineStatus func(p0 context.Context, p1 string, p2 bool) error `perm:"write"`

		ShowAccessPoint func(p0 context.Context, p1 string) (AccessPoint, error) `perm:"admin"`

		UserDownloadBlockResults func(p0 context.Context, p1 []types.UserBlockDownloadResult) error `perm:"read"`
	}
}

type LocatorStub struct {
	CommonStub
}

type SchedulerStruct struct {
	CommonStruct

	Internal struct {
		CacheCarfiles func(p0 context.Context, p1 *types.CacheCarfileInfo) error `perm:"admin"`

		CandidateNodeConnect func(p0 context.Context, p1 string) error `perm:"write"`

		CarfileRecord func(p0 context.Context, p1 string) (*types.CarfileRecordInfo, error) `perm:"read"`

		CarfileRecords func(p0 context.Context, p1 int, p2 []string) (*types.ListCarfileRecordRsp, error) `perm:"read"`

		CarfileReplicaList func(p0 context.Context, p1 types.ListCacheInfosReq) (*types.ListCarfileReplicaRsp, error) `perm:"read"`

		DeleteEdgeUpdateInfo func(p0 context.Context, p1 int) error `perm:"admin"`

		DeleteNodeLogFile func(p0 context.Context, p1 string) error `perm:"admin"`

		EdgeDownloadInfos func(p0 context.Context, p1 string) ([]*types.DownloadInfo, error) `perm:"read"`

		EdgeExternalServiceAddress func(p0 context.Context, p1 string, p2 string) (string, error) `perm:"write"`

		EdgeNodeConnect func(p0 context.Context, p1 string) error `perm:"write"`

		EdgeUpdateInfos func(p0 context.Context) (map[int]*EdgeUpdateInfo, error) `perm:"read"`

		GetNodeAppUpdateInfos func(p0 context.Context) (map[int]*EdgeUpdateInfo, error) `perm:"read"`

		IsBehindFullConeNAT func(p0 context.Context, p1 string) (bool, error) `perm:"read"`

		LocatorConnect func(p0 context.Context, p1 string, p2 string) error `perm:"write"`

		NodeAuthNew func(p0 context.Context, p1 string, p2 string) (string, error) `perm:"read"`

		NodeAuthVerify func(p0 context.Context, p1 string) ([]auth.Permission, error) `perm:"read"`

		NodeExternalServiceAddress func(p0 context.Context) (string, error) `perm:"read"`

		NodeInfo func(p0 context.Context, p1 string) (types.NodeInfo, error) `perm:"read"`

		NodeList func(p0 context.Context, p1 int, p2 int) (*types.ListNodesRsp, error) `perm:"read"`

		NodeLogFile func(p0 context.Context, p1 string) ([]byte, error) `perm:"admin"`

		NodeLogFileInfo func(p0 context.Context, p1 string) (*LogFile, error) `perm:"admin"`

		NodeNatType func(p0 context.Context, p1 string) (types.NatType, error) `perm:"write"`

		NodeQuit func(p0 context.Context, p1 string) error `perm:"admin"`

		NodeValidatedResult func(p0 context.Context, p1 ValidatedResult) error `perm:"write"`

		OnlineNodeList func(p0 context.Context, p1 types.NodeType) ([]string, error) `perm:"read"`

		PublicKey func(p0 context.Context) (string, error) `perm:"write"`

		RegisterNode func(p0 context.Context, p1 string, p2 string, p3 types.NodeType) error `perm:"admin"`

		RemoveCarfile func(p0 context.Context, p1 string) error `perm:"admin"`

		RemoveCarfileResult func(p0 context.Context, p1 types.RemoveCarfileResult) error `perm:"write"`

		ResetCarfileExpiration func(p0 context.Context, p1 string, p2 time.Time) error `perm:"admin"`

		RestartFailedCarfiles func(p0 context.Context, p1 []types.CarfileHash) error `perm:"admin"`

		SetEdgeUpdateInfo func(p0 context.Context, p1 *EdgeUpdateInfo) error `perm:"admin"`

		SetEnableValidation func(p0 context.Context, p1 bool) error `perm:"admin"`

		SetNodePort func(p0 context.Context, p1 string, p2 string) error `perm:"admin"`

		StartOnceElection func(p0 context.Context) error `perm:"admin"`

		UserDownloadBlockResults func(p0 context.Context, p1 []types.UserBlockDownloadResult) error `perm:"read"`

		UserDownloadResult func(p0 context.Context, p1 types.UserDownloadResult) error `perm:"write"`

		ValidatedResultList func(p0 context.Context, p1 time.Time, p2 time.Time, p3 int, p4 int) (*types.ListValidatedResultRsp, error) `perm:"read"`
	}
}

type SchedulerStub struct {
	CommonStub
}

type ValidateStruct struct {
	Internal struct {
		BeValidate func(p0 context.Context, p1 ReqValidate, p2 string) error `perm:"read"`
	}
}

type ValidateStub struct {
}

func (s *CandidateStruct) GetBlock(p0 context.Context, p1 string) ([]byte, error) {
	if s.Internal.GetBlock == nil {
		return *new([]byte), ErrNotSupported
	}
	return s.Internal.GetBlock(p0, p1)
}

func (s *CandidateStub) GetBlock(p0 context.Context, p1 string) ([]byte, error) {
	return *new([]byte), ErrNotSupported
}

func (s *CandidateStruct) GetBlocksOfCarfile(p0 context.Context, p1 string, p2 int64, p3 int) (map[int]string, error) {
	if s.Internal.GetBlocksOfCarfile == nil {
		return *new(map[int]string), ErrNotSupported
	}
	return s.Internal.GetBlocksOfCarfile(p0, p1, p2, p3)
}

func (s *CandidateStub) GetBlocksOfCarfile(p0 context.Context, p1 string, p2 int64, p3 int) (map[int]string, error) {
	return *new(map[int]string), ErrNotSupported
}

func (s *CandidateStruct) ValidateNodes(p0 context.Context, p1 []ReqValidate) error {
	if s.Internal.ValidateNodes == nil {
		return ErrNotSupported
	}
	return s.Internal.ValidateNodes(p0, p1)
}

func (s *CandidateStub) ValidateNodes(p0 context.Context, p1 []ReqValidate) error {
	return ErrNotSupported
}

func (s *CandidateStruct) WaitQuiet(p0 context.Context) error {
	if s.Internal.WaitQuiet == nil {
		return ErrNotSupported
	}
	return s.Internal.WaitQuiet(p0)
}

func (s *CandidateStub) WaitQuiet(p0 context.Context) error {
	return ErrNotSupported
}

func (s *CarfileOperationStruct) CacheCarfile(p0 context.Context, p1 string, p2 []*types.DownloadSource) error {
	if s.Internal.CacheCarfile == nil {
		return ErrNotSupported
	}
	return s.Internal.CacheCarfile(p0, p1, p2)
}

func (s *CarfileOperationStub) CacheCarfile(p0 context.Context, p1 string, p2 []*types.DownloadSource) error {
	return ErrNotSupported
}

func (s *CarfileOperationStruct) CachedProgresses(p0 context.Context, p1 []string) (*types.CacheResult, error) {
	if s.Internal.CachedProgresses == nil {
		return nil, ErrNotSupported
	}
	return s.Internal.CachedProgresses(p0, p1)
}

func (s *CarfileOperationStub) CachedProgresses(p0 context.Context, p1 []string) (*types.CacheResult, error) {
	return nil, ErrNotSupported
}

func (s *CarfileOperationStruct) DeleteAllCarfiles(p0 context.Context) error {
	if s.Internal.DeleteAllCarfiles == nil {
		return ErrNotSupported
	}
	return s.Internal.DeleteAllCarfiles(p0)
}

func (s *CarfileOperationStub) DeleteAllCarfiles(p0 context.Context) error {
	return ErrNotSupported
}

func (s *CarfileOperationStruct) DeleteCarfile(p0 context.Context, p1 string) error {
	if s.Internal.DeleteCarfile == nil {
		return ErrNotSupported
	}
	return s.Internal.DeleteCarfile(p0, p1)
}

func (s *CarfileOperationStub) DeleteCarfile(p0 context.Context, p1 string) error {
	return ErrNotSupported
}

func (s *CarfileOperationStruct) QueryCacheStat(p0 context.Context) (*types.CacheStat, error) {
	if s.Internal.QueryCacheStat == nil {
		return nil, ErrNotSupported
	}
	return s.Internal.QueryCacheStat(p0)
}

func (s *CarfileOperationStub) QueryCacheStat(p0 context.Context) (*types.CacheStat, error) {
	return nil, ErrNotSupported
}

func (s *CarfileOperationStruct) QueryCachingCarfile(p0 context.Context) (*types.CachingCarfile, error) {
	if s.Internal.QueryCachingCarfile == nil {
		return nil, ErrNotSupported
	}
	return s.Internal.QueryCachingCarfile(p0)
}

func (s *CarfileOperationStub) QueryCachingCarfile(p0 context.Context) (*types.CachingCarfile, error) {
	return nil, ErrNotSupported
}

func (s *CommonStruct) AuthNew(p0 context.Context, p1 []auth.Permission) (string, error) {
	if s.Internal.AuthNew == nil {
		return "", ErrNotSupported
	}
	return s.Internal.AuthNew(p0, p1)
}

func (s *CommonStub) AuthNew(p0 context.Context, p1 []auth.Permission) (string, error) {
	return "", ErrNotSupported
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

func (s *CommonStruct) DeleteLogFile(p0 context.Context) error {
	if s.Internal.DeleteLogFile == nil {
		return ErrNotSupported
	}
	return s.Internal.DeleteLogFile(p0)
}

func (s *CommonStub) DeleteLogFile(p0 context.Context) error {
	return ErrNotSupported
}

func (s *CommonStruct) Discover(p0 context.Context) (types.OpenRPCDocument, error) {
	if s.Internal.Discover == nil {
		return *new(types.OpenRPCDocument), ErrNotSupported
	}
	return s.Internal.Discover(p0)
}

func (s *CommonStub) Discover(p0 context.Context) (types.OpenRPCDocument, error) {
	return *new(types.OpenRPCDocument), ErrNotSupported
}

func (s *CommonStruct) DownloadLogFile(p0 context.Context) ([]byte, error) {
	if s.Internal.DownloadLogFile == nil {
		return *new([]byte), ErrNotSupported
	}
	return s.Internal.DownloadLogFile(p0)
}

func (s *CommonStub) DownloadLogFile(p0 context.Context) ([]byte, error) {
	return *new([]byte), ErrNotSupported
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

func (s *CommonStruct) LogSetLevel(p0 context.Context, p1 string, p2 string) error {
	if s.Internal.LogSetLevel == nil {
		return ErrNotSupported
	}
	return s.Internal.LogSetLevel(p0, p1, p2)
}

func (s *CommonStub) LogSetLevel(p0 context.Context, p1 string, p2 string) error {
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

func (s *CommonStruct) ShowLogFile(p0 context.Context) (*LogFile, error) {
	if s.Internal.ShowLogFile == nil {
		return nil, ErrNotSupported
	}
	return s.Internal.ShowLogFile(p0)
}

func (s *CommonStub) ShowLogFile(p0 context.Context) (*LogFile, error) {
	return nil, ErrNotSupported
}

func (s *CommonStruct) Shutdown(p0 context.Context) error {
	if s.Internal.Shutdown == nil {
		return ErrNotSupported
	}
	return s.Internal.Shutdown(p0)
}

func (s *CommonStub) Shutdown(p0 context.Context) error {
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

func (s *DataSyncStruct) CompareCarfiles(p0 context.Context, p1 uint32, p2 map[uint32][]string) error {
	if s.Internal.CompareCarfiles == nil {
		return ErrNotSupported
	}
	return s.Internal.CompareCarfiles(p0, p1, p2)
}

func (s *DataSyncStub) CompareCarfiles(p0 context.Context, p1 uint32, p2 map[uint32][]string) error {
	return ErrNotSupported
}

func (s *DataSyncStruct) CompareChecksums(p0 context.Context, p1 uint32, p2 map[uint32]string) ([]uint32, error) {
	if s.Internal.CompareChecksums == nil {
		return *new([]uint32), ErrNotSupported
	}
	return s.Internal.CompareChecksums(p0, p1, p2)
}

func (s *DataSyncStub) CompareChecksums(p0 context.Context, p1 uint32, p2 map[uint32]string) ([]uint32, error) {
	return *new([]uint32), ErrNotSupported
}

func (s *DeviceStruct) NodeID(p0 context.Context) (string, error) {
	if s.Internal.NodeID == nil {
		return "", ErrNotSupported
	}
	return s.Internal.NodeID(p0)
}

func (s *DeviceStub) NodeID(p0 context.Context) (string, error) {
	return "", ErrNotSupported
}

func (s *DeviceStruct) NodeInfo(p0 context.Context) (types.NodeInfo, error) {
	if s.Internal.NodeInfo == nil {
		return *new(types.NodeInfo), ErrNotSupported
	}
	return s.Internal.NodeInfo(p0)
}

func (s *DeviceStub) NodeInfo(p0 context.Context) (types.NodeInfo, error) {
	return *new(types.NodeInfo), ErrNotSupported
}

func (s *EdgeStruct) ExternalServiceAddress(p0 context.Context, p1 string) (string, error) {
	if s.Internal.ExternalServiceAddress == nil {
		return "", ErrNotSupported
	}
	return s.Internal.ExternalServiceAddress(p0, p1)
}

func (s *EdgeStub) ExternalServiceAddress(p0 context.Context, p1 string) (string, error) {
	return "", ErrNotSupported
}

func (s *EdgeStruct) UserNATTravel(p0 context.Context, p1 string) error {
	if s.Internal.UserNATTravel == nil {
		return ErrNotSupported
	}
	return s.Internal.UserNATTravel(p0, p1)
}

func (s *EdgeStub) UserNATTravel(p0 context.Context, p1 string) error {
	return ErrNotSupported
}

func (s *EdgeStruct) WaitQuiet(p0 context.Context) error {
	if s.Internal.WaitQuiet == nil {
		return ErrNotSupported
	}
	return s.Internal.WaitQuiet(p0)
}

func (s *EdgeStub) WaitQuiet(p0 context.Context) error {
	return ErrNotSupported
}

func (s *LocatorStruct) AddAccessPoint(p0 context.Context, p1 string, p2 string, p3 int, p4 string) error {
	if s.Internal.AddAccessPoint == nil {
		return ErrNotSupported
	}
	return s.Internal.AddAccessPoint(p0, p1, p2, p3, p4)
}

func (s *LocatorStub) AddAccessPoint(p0 context.Context, p1 string, p2 string, p3 int, p4 string) error {
	return ErrNotSupported
}

func (s *LocatorStruct) AllocateNodes(p0 context.Context, p1 string, p2 types.NodeType, p3 int) ([]*types.NodeAllocateInfo, error) {
	if s.Internal.AllocateNodes == nil {
		return *new([]*types.NodeAllocateInfo), ErrNotSupported
	}
	return s.Internal.AllocateNodes(p0, p1, p2, p3)
}

func (s *LocatorStub) AllocateNodes(p0 context.Context, p1 string, p2 types.NodeType, p3 int) ([]*types.NodeAllocateInfo, error) {
	return *new([]*types.NodeAllocateInfo), ErrNotSupported
}

func (s *LocatorStruct) EdgeDownloadInfos(p0 context.Context, p1 string) ([]*types.DownloadInfo, error) {
	if s.Internal.EdgeDownloadInfos == nil {
		return *new([]*types.DownloadInfo), ErrNotSupported
	}
	return s.Internal.EdgeDownloadInfos(p0, p1)
}

func (s *LocatorStub) EdgeDownloadInfos(p0 context.Context, p1 string) ([]*types.DownloadInfo, error) {
	return *new([]*types.DownloadInfo), ErrNotSupported
}

func (s *LocatorStruct) GetAccessPoints(p0 context.Context, p1 string) ([]string, error) {
	if s.Internal.GetAccessPoints == nil {
		return *new([]string), ErrNotSupported
	}
	return s.Internal.GetAccessPoints(p0, p1)
}

func (s *LocatorStub) GetAccessPoints(p0 context.Context, p1 string) ([]string, error) {
	return *new([]string), ErrNotSupported
}

func (s *LocatorStruct) ListAreaIDs(p0 context.Context) ([]string, error) {
	if s.Internal.ListAreaIDs == nil {
		return *new([]string), ErrNotSupported
	}
	return s.Internal.ListAreaIDs(p0)
}

func (s *LocatorStub) ListAreaIDs(p0 context.Context) ([]string, error) {
	return *new([]string), ErrNotSupported
}

func (s *LocatorStruct) LoadAccessPointsForWeb(p0 context.Context) ([]AccessPoint, error) {
	if s.Internal.LoadAccessPointsForWeb == nil {
		return *new([]AccessPoint), ErrNotSupported
	}
	return s.Internal.LoadAccessPointsForWeb(p0)
}

func (s *LocatorStub) LoadAccessPointsForWeb(p0 context.Context) ([]AccessPoint, error) {
	return *new([]AccessPoint), ErrNotSupported
}

func (s *LocatorStruct) LoadUserAccessPoint(p0 context.Context, p1 string) (AccessPoint, error) {
	if s.Internal.LoadUserAccessPoint == nil {
		return *new(AccessPoint), ErrNotSupported
	}
	return s.Internal.LoadUserAccessPoint(p0, p1)
}

func (s *LocatorStub) LoadUserAccessPoint(p0 context.Context, p1 string) (AccessPoint, error) {
	return *new(AccessPoint), ErrNotSupported
}

func (s *LocatorStruct) RemoveAccessPoints(p0 context.Context, p1 string) error {
	if s.Internal.RemoveAccessPoints == nil {
		return ErrNotSupported
	}
	return s.Internal.RemoveAccessPoints(p0, p1)
}

func (s *LocatorStub) RemoveAccessPoints(p0 context.Context, p1 string) error {
	return ErrNotSupported
}

func (s *LocatorStruct) SetNodeOnlineStatus(p0 context.Context, p1 string, p2 bool) error {
	if s.Internal.SetNodeOnlineStatus == nil {
		return ErrNotSupported
	}
	return s.Internal.SetNodeOnlineStatus(p0, p1, p2)
}

func (s *LocatorStub) SetNodeOnlineStatus(p0 context.Context, p1 string, p2 bool) error {
	return ErrNotSupported
}

func (s *LocatorStruct) ShowAccessPoint(p0 context.Context, p1 string) (AccessPoint, error) {
	if s.Internal.ShowAccessPoint == nil {
		return *new(AccessPoint), ErrNotSupported
	}
	return s.Internal.ShowAccessPoint(p0, p1)
}

func (s *LocatorStub) ShowAccessPoint(p0 context.Context, p1 string) (AccessPoint, error) {
	return *new(AccessPoint), ErrNotSupported
}

func (s *LocatorStruct) UserDownloadBlockResults(p0 context.Context, p1 []types.UserBlockDownloadResult) error {
	if s.Internal.UserDownloadBlockResults == nil {
		return ErrNotSupported
	}
	return s.Internal.UserDownloadBlockResults(p0, p1)
}

func (s *LocatorStub) UserDownloadBlockResults(p0 context.Context, p1 []types.UserBlockDownloadResult) error {
	return ErrNotSupported
}

func (s *SchedulerStruct) CacheCarfiles(p0 context.Context, p1 *types.CacheCarfileInfo) error {
	if s.Internal.CacheCarfiles == nil {
		return ErrNotSupported
	}
	return s.Internal.CacheCarfiles(p0, p1)
}

func (s *SchedulerStub) CacheCarfiles(p0 context.Context, p1 *types.CacheCarfileInfo) error {
	return ErrNotSupported
}

func (s *SchedulerStruct) CandidateNodeConnect(p0 context.Context, p1 string) error {
	if s.Internal.CandidateNodeConnect == nil {
		return ErrNotSupported
	}
	return s.Internal.CandidateNodeConnect(p0, p1)
}

func (s *SchedulerStub) CandidateNodeConnect(p0 context.Context, p1 string) error {
	return ErrNotSupported
}

func (s *SchedulerStruct) CarfileRecord(p0 context.Context, p1 string) (*types.CarfileRecordInfo, error) {
	if s.Internal.CarfileRecord == nil {
		return nil, ErrNotSupported
	}
	return s.Internal.CarfileRecord(p0, p1)
}

func (s *SchedulerStub) CarfileRecord(p0 context.Context, p1 string) (*types.CarfileRecordInfo, error) {
	return nil, ErrNotSupported
}

func (s *SchedulerStruct) CarfileRecords(p0 context.Context, p1 int, p2 []string) (*types.ListCarfileRecordRsp, error) {
	if s.Internal.CarfileRecords == nil {
		return nil, ErrNotSupported
	}
	return s.Internal.CarfileRecords(p0, p1, p2)
}

func (s *SchedulerStub) CarfileRecords(p0 context.Context, p1 int, p2 []string) (*types.ListCarfileRecordRsp, error) {
	return nil, ErrNotSupported
}

func (s *SchedulerStruct) CarfileReplicaList(p0 context.Context, p1 types.ListCacheInfosReq) (*types.ListCarfileReplicaRsp, error) {
	if s.Internal.CarfileReplicaList == nil {
		return nil, ErrNotSupported
	}
	return s.Internal.CarfileReplicaList(p0, p1)
}

func (s *SchedulerStub) CarfileReplicaList(p0 context.Context, p1 types.ListCacheInfosReq) (*types.ListCarfileReplicaRsp, error) {
	return nil, ErrNotSupported
}

func (s *SchedulerStruct) DeleteEdgeUpdateInfo(p0 context.Context, p1 int) error {
	if s.Internal.DeleteEdgeUpdateInfo == nil {
		return ErrNotSupported
	}
	return s.Internal.DeleteEdgeUpdateInfo(p0, p1)
}

func (s *SchedulerStub) DeleteEdgeUpdateInfo(p0 context.Context, p1 int) error {
	return ErrNotSupported
}

func (s *SchedulerStruct) DeleteNodeLogFile(p0 context.Context, p1 string) error {
	if s.Internal.DeleteNodeLogFile == nil {
		return ErrNotSupported
	}
	return s.Internal.DeleteNodeLogFile(p0, p1)
}

func (s *SchedulerStub) DeleteNodeLogFile(p0 context.Context, p1 string) error {
	return ErrNotSupported
}

func (s *SchedulerStruct) EdgeDownloadInfos(p0 context.Context, p1 string) ([]*types.DownloadInfo, error) {
	if s.Internal.EdgeDownloadInfos == nil {
		return *new([]*types.DownloadInfo), ErrNotSupported
	}
	return s.Internal.EdgeDownloadInfos(p0, p1)
}

func (s *SchedulerStub) EdgeDownloadInfos(p0 context.Context, p1 string) ([]*types.DownloadInfo, error) {
	return *new([]*types.DownloadInfo), ErrNotSupported
}

func (s *SchedulerStruct) EdgeExternalServiceAddress(p0 context.Context, p1 string, p2 string) (string, error) {
	if s.Internal.EdgeExternalServiceAddress == nil {
		return "", ErrNotSupported
	}
	return s.Internal.EdgeExternalServiceAddress(p0, p1, p2)
}

func (s *SchedulerStub) EdgeExternalServiceAddress(p0 context.Context, p1 string, p2 string) (string, error) {
	return "", ErrNotSupported
}

func (s *SchedulerStruct) EdgeNodeConnect(p0 context.Context, p1 string) error {
	if s.Internal.EdgeNodeConnect == nil {
		return ErrNotSupported
	}
	return s.Internal.EdgeNodeConnect(p0, p1)
}

func (s *SchedulerStub) EdgeNodeConnect(p0 context.Context, p1 string) error {
	return ErrNotSupported
}

func (s *SchedulerStruct) EdgeUpdateInfos(p0 context.Context) (map[int]*EdgeUpdateInfo, error) {
	if s.Internal.EdgeUpdateInfos == nil {
		return *new(map[int]*EdgeUpdateInfo), ErrNotSupported
	}
	return s.Internal.EdgeUpdateInfos(p0)
}

func (s *SchedulerStub) EdgeUpdateInfos(p0 context.Context) (map[int]*EdgeUpdateInfo, error) {
	return *new(map[int]*EdgeUpdateInfo), ErrNotSupported
}

func (s *SchedulerStruct) GetNodeAppUpdateInfos(p0 context.Context) (map[int]*EdgeUpdateInfo, error) {
	if s.Internal.GetNodeAppUpdateInfos == nil {
		return *new(map[int]*EdgeUpdateInfo), ErrNotSupported
	}
	return s.Internal.GetNodeAppUpdateInfos(p0)
}

func (s *SchedulerStub) GetNodeAppUpdateInfos(p0 context.Context) (map[int]*EdgeUpdateInfo, error) {
	return *new(map[int]*EdgeUpdateInfo), ErrNotSupported
}

func (s *SchedulerStruct) IsBehindFullConeNAT(p0 context.Context, p1 string) (bool, error) {
	if s.Internal.IsBehindFullConeNAT == nil {
		return false, ErrNotSupported
	}
	return s.Internal.IsBehindFullConeNAT(p0, p1)
}

func (s *SchedulerStub) IsBehindFullConeNAT(p0 context.Context, p1 string) (bool, error) {
	return false, ErrNotSupported
}

func (s *SchedulerStruct) LocatorConnect(p0 context.Context, p1 string, p2 string) error {
	if s.Internal.LocatorConnect == nil {
		return ErrNotSupported
	}
	return s.Internal.LocatorConnect(p0, p1, p2)
}

func (s *SchedulerStub) LocatorConnect(p0 context.Context, p1 string, p2 string) error {
	return ErrNotSupported
}

func (s *SchedulerStruct) NodeAuthNew(p0 context.Context, p1 string, p2 string) (string, error) {
	if s.Internal.NodeAuthNew == nil {
		return "", ErrNotSupported
	}
	return s.Internal.NodeAuthNew(p0, p1, p2)
}

func (s *SchedulerStub) NodeAuthNew(p0 context.Context, p1 string, p2 string) (string, error) {
	return "", ErrNotSupported
}

func (s *SchedulerStruct) NodeAuthVerify(p0 context.Context, p1 string) ([]auth.Permission, error) {
	if s.Internal.NodeAuthVerify == nil {
		return *new([]auth.Permission), ErrNotSupported
	}
	return s.Internal.NodeAuthVerify(p0, p1)
}

func (s *SchedulerStub) NodeAuthVerify(p0 context.Context, p1 string) ([]auth.Permission, error) {
	return *new([]auth.Permission), ErrNotSupported
}

func (s *SchedulerStruct) NodeExternalServiceAddress(p0 context.Context) (string, error) {
	if s.Internal.NodeExternalServiceAddress == nil {
		return "", ErrNotSupported
	}
	return s.Internal.NodeExternalServiceAddress(p0)
}

func (s *SchedulerStub) NodeExternalServiceAddress(p0 context.Context) (string, error) {
	return "", ErrNotSupported
}

func (s *SchedulerStruct) NodeInfo(p0 context.Context, p1 string) (types.NodeInfo, error) {
	if s.Internal.NodeInfo == nil {
		return *new(types.NodeInfo), ErrNotSupported
	}
	return s.Internal.NodeInfo(p0, p1)
}

func (s *SchedulerStub) NodeInfo(p0 context.Context, p1 string) (types.NodeInfo, error) {
	return *new(types.NodeInfo), ErrNotSupported
}

func (s *SchedulerStruct) NodeList(p0 context.Context, p1 int, p2 int) (*types.ListNodesRsp, error) {
	if s.Internal.NodeList == nil {
		return nil, ErrNotSupported
	}
	return s.Internal.NodeList(p0, p1, p2)
}

func (s *SchedulerStub) NodeList(p0 context.Context, p1 int, p2 int) (*types.ListNodesRsp, error) {
	return nil, ErrNotSupported
}

func (s *SchedulerStruct) NodeLogFile(p0 context.Context, p1 string) ([]byte, error) {
	if s.Internal.NodeLogFile == nil {
		return *new([]byte), ErrNotSupported
	}
	return s.Internal.NodeLogFile(p0, p1)
}

func (s *SchedulerStub) NodeLogFile(p0 context.Context, p1 string) ([]byte, error) {
	return *new([]byte), ErrNotSupported
}

func (s *SchedulerStruct) NodeLogFileInfo(p0 context.Context, p1 string) (*LogFile, error) {
	if s.Internal.NodeLogFileInfo == nil {
		return nil, ErrNotSupported
	}
	return s.Internal.NodeLogFileInfo(p0, p1)
}

func (s *SchedulerStub) NodeLogFileInfo(p0 context.Context, p1 string) (*LogFile, error) {
	return nil, ErrNotSupported
}

func (s *SchedulerStruct) NodeNatType(p0 context.Context, p1 string) (types.NatType, error) {
	if s.Internal.NodeNatType == nil {
		return *new(types.NatType), ErrNotSupported
	}
	return s.Internal.NodeNatType(p0, p1)
}

func (s *SchedulerStub) NodeNatType(p0 context.Context, p1 string) (types.NatType, error) {
	return *new(types.NatType), ErrNotSupported
}

func (s *SchedulerStruct) NodeQuit(p0 context.Context, p1 string) error {
	if s.Internal.NodeQuit == nil {
		return ErrNotSupported
	}
	return s.Internal.NodeQuit(p0, p1)
}

func (s *SchedulerStub) NodeQuit(p0 context.Context, p1 string) error {
	return ErrNotSupported
}

func (s *SchedulerStruct) NodeValidatedResult(p0 context.Context, p1 ValidatedResult) error {
	if s.Internal.NodeValidatedResult == nil {
		return ErrNotSupported
	}
	return s.Internal.NodeValidatedResult(p0, p1)
}

func (s *SchedulerStub) NodeValidatedResult(p0 context.Context, p1 ValidatedResult) error {
	return ErrNotSupported
}

func (s *SchedulerStruct) OnlineNodeList(p0 context.Context, p1 types.NodeType) ([]string, error) {
	if s.Internal.OnlineNodeList == nil {
		return *new([]string), ErrNotSupported
	}
	return s.Internal.OnlineNodeList(p0, p1)
}

func (s *SchedulerStub) OnlineNodeList(p0 context.Context, p1 types.NodeType) ([]string, error) {
	return *new([]string), ErrNotSupported
}

func (s *SchedulerStruct) PublicKey(p0 context.Context) (string, error) {
	if s.Internal.PublicKey == nil {
		return "", ErrNotSupported
	}
	return s.Internal.PublicKey(p0)
}

func (s *SchedulerStub) PublicKey(p0 context.Context) (string, error) {
	return "", ErrNotSupported
}

func (s *SchedulerStruct) RegisterNode(p0 context.Context, p1 string, p2 string, p3 types.NodeType) error {
	if s.Internal.RegisterNode == nil {
		return ErrNotSupported
	}
	return s.Internal.RegisterNode(p0, p1, p2, p3)
}

func (s *SchedulerStub) RegisterNode(p0 context.Context, p1 string, p2 string, p3 types.NodeType) error {
	return ErrNotSupported
}

func (s *SchedulerStruct) RemoveCarfile(p0 context.Context, p1 string) error {
	if s.Internal.RemoveCarfile == nil {
		return ErrNotSupported
	}
	return s.Internal.RemoveCarfile(p0, p1)
}

func (s *SchedulerStub) RemoveCarfile(p0 context.Context, p1 string) error {
	return ErrNotSupported
}

func (s *SchedulerStruct) RemoveCarfileResult(p0 context.Context, p1 types.RemoveCarfileResult) error {
	if s.Internal.RemoveCarfileResult == nil {
		return ErrNotSupported
	}
	return s.Internal.RemoveCarfileResult(p0, p1)
}

func (s *SchedulerStub) RemoveCarfileResult(p0 context.Context, p1 types.RemoveCarfileResult) error {
	return ErrNotSupported
}

func (s *SchedulerStruct) ResetCarfileExpiration(p0 context.Context, p1 string, p2 time.Time) error {
	if s.Internal.ResetCarfileExpiration == nil {
		return ErrNotSupported
	}
	return s.Internal.ResetCarfileExpiration(p0, p1, p2)
}

func (s *SchedulerStub) ResetCarfileExpiration(p0 context.Context, p1 string, p2 time.Time) error {
	return ErrNotSupported
}

func (s *SchedulerStruct) RestartFailedCarfiles(p0 context.Context, p1 []types.CarfileHash) error {
	if s.Internal.RestartFailedCarfiles == nil {
		return ErrNotSupported
	}
	return s.Internal.RestartFailedCarfiles(p0, p1)
}

func (s *SchedulerStub) RestartFailedCarfiles(p0 context.Context, p1 []types.CarfileHash) error {
	return ErrNotSupported
}

func (s *SchedulerStruct) SetEdgeUpdateInfo(p0 context.Context, p1 *EdgeUpdateInfo) error {
	if s.Internal.SetEdgeUpdateInfo == nil {
		return ErrNotSupported
	}
	return s.Internal.SetEdgeUpdateInfo(p0, p1)
}

func (s *SchedulerStub) SetEdgeUpdateInfo(p0 context.Context, p1 *EdgeUpdateInfo) error {
	return ErrNotSupported
}

func (s *SchedulerStruct) SetEnableValidation(p0 context.Context, p1 bool) error {
	if s.Internal.SetEnableValidation == nil {
		return ErrNotSupported
	}
	return s.Internal.SetEnableValidation(p0, p1)
}

func (s *SchedulerStub) SetEnableValidation(p0 context.Context, p1 bool) error {
	return ErrNotSupported
}

func (s *SchedulerStruct) SetNodePort(p0 context.Context, p1 string, p2 string) error {
	if s.Internal.SetNodePort == nil {
		return ErrNotSupported
	}
	return s.Internal.SetNodePort(p0, p1, p2)
}

func (s *SchedulerStub) SetNodePort(p0 context.Context, p1 string, p2 string) error {
	return ErrNotSupported
}

func (s *SchedulerStruct) StartOnceElection(p0 context.Context) error {
	if s.Internal.StartOnceElection == nil {
		return ErrNotSupported
	}
	return s.Internal.StartOnceElection(p0)
}

func (s *SchedulerStub) StartOnceElection(p0 context.Context) error {
	return ErrNotSupported
}

func (s *SchedulerStruct) UserDownloadBlockResults(p0 context.Context, p1 []types.UserBlockDownloadResult) error {
	if s.Internal.UserDownloadBlockResults == nil {
		return ErrNotSupported
	}
	return s.Internal.UserDownloadBlockResults(p0, p1)
}

func (s *SchedulerStub) UserDownloadBlockResults(p0 context.Context, p1 []types.UserBlockDownloadResult) error {
	return ErrNotSupported
}

func (s *SchedulerStruct) UserDownloadResult(p0 context.Context, p1 types.UserDownloadResult) error {
	if s.Internal.UserDownloadResult == nil {
		return ErrNotSupported
	}
	return s.Internal.UserDownloadResult(p0, p1)
}

func (s *SchedulerStub) UserDownloadResult(p0 context.Context, p1 types.UserDownloadResult) error {
	return ErrNotSupported
}

func (s *SchedulerStruct) ValidatedResultList(p0 context.Context, p1 time.Time, p2 time.Time, p3 int, p4 int) (*types.ListValidatedResultRsp, error) {
	if s.Internal.ValidatedResultList == nil {
		return nil, ErrNotSupported
	}
	return s.Internal.ValidatedResultList(p0, p1, p2, p3, p4)
}

func (s *SchedulerStub) ValidatedResultList(p0 context.Context, p1 time.Time, p2 time.Time, p3 int, p4 int) (*types.ListValidatedResultRsp, error) {
	return nil, ErrNotSupported
}

func (s *ValidateStruct) BeValidate(p0 context.Context, p1 ReqValidate, p2 string) error {
	if s.Internal.BeValidate == nil {
		return ErrNotSupported
	}
	return s.Internal.BeValidate(p0, p1, p2)
}

func (s *ValidateStub) BeValidate(p0 context.Context, p1 ReqValidate, p2 string) error {
	return ErrNotSupported
}

var _ Candidate = new(CandidateStruct)
var _ CarfileOperation = new(CarfileOperationStruct)
var _ Common = new(CommonStruct)
var _ DataSync = new(DataSyncStruct)
var _ Device = new(DeviceStruct)
var _ Edge = new(EdgeStruct)
var _ Locator = new(LocatorStruct)
var _ Scheduler = new(SchedulerStruct)
var _ Validate = new(ValidateStruct)
