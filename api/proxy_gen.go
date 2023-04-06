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
		GetBlocksOfCarfile func(p0 context.Context, p1 string, p2 int64, p3 int) (map[int]string, error) `perm:"read"`

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

		CachedProgresses func(p0 context.Context, p1 []string) (*types.PullResult, error) `perm:"write"`

		DeleteCarfile func(p0 context.Context, p1 string) error `perm:"write"`

		QueryCacheStat func(p0 context.Context) (*types.CacheStat, error) `perm:"write"`

		QueryCachingCarfile func(p0 context.Context) (*types.CachingAsset, error) `perm:"write"`
	}
}

type CarfileOperationStub struct {
}

type CommonStruct struct {
	Internal struct {
		AuthNew func(p0 context.Context, p1 []auth.Permission) (string, error) `perm:"admin"`

		AuthVerify func(p0 context.Context, p1 string) ([]auth.Permission, error) `perm:"read"`

		Closing func(p0 context.Context) (<-chan struct{}, error) `perm:"read"`

		Discover func(p0 context.Context) (types.OpenRPCDocument, error) `perm:"read"`

		LogAlerts func(p0 context.Context) ([]alerting.Alert, error) `perm:"admin"`

		LogList func(p0 context.Context) ([]string, error) `perm:"write"`

		LogSetLevel func(p0 context.Context, p1 string, p2 string) error `perm:"write"`

		Session func(p0 context.Context) (uuid.UUID, error) `perm:"read"`

		Shutdown func(p0 context.Context) error `perm:"admin"`

		Version func(p0 context.Context) (APIVersion, error) `perm:"read"`
	}
}

type CommonStub struct {
}

type DataSyncStruct struct {
	Internal struct {
		CompareBucketHashes func(p0 context.Context, p1 map[uint32]string) ([]uint32, error) `perm:"write"`

		CompareTopHash func(p0 context.Context, p1 string) (bool, error) `perm:"write"`
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

		UserNATTravel func(p0 context.Context, p1 string, p2 *types.NatTravelReq) error `perm:"write"`

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

		EdgeDownloadInfos func(p0 context.Context, p1 string) ([]*types.DownloadInfo, error) `perm:"read"`

		GetAccessPoints func(p0 context.Context, p1 string) ([]string, error) `perm:"read"`

		ListAreaIDs func(p0 context.Context) ([]string, error) `perm:"admin"`

		LoadAccessPointsForWeb func(p0 context.Context) ([]AccessPoint, error) `perm:"admin"`

		LoadUserAccessPoint func(p0 context.Context, p1 string) (AccessPoint, error) `perm:"admin"`

		RegisterNewNode func(p0 context.Context, p1 string, p2 string, p3 string, p4 types.NodeType) error `perm:"admin"`

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
		GetAssetListForBucket func(p0 context.Context, p1 string) ([]string, error) `perm:"write"`

		AssetRecord func(p0 context.Context, p1 string) (*types.AssetRecord, error) `perm:"read"`

		AssetRecords func(p0 context.Context, p1 int, p2 int, p3 []string) ([]*types.AssetRecord, error) `perm:"read"`

		AssetReplicaList func(p0 context.Context, p1 types.ListReplicaInfosReq) (*types.ListReplicaInfosRsp, error) `perm:"read"`

		CacheAsset func(p0 context.Context, p1 *types.PullAssetReq) error `perm:"admin"`

		CheckNetworkConnectivity func(p0 context.Context, p1 string, p2 string) error `perm:"read"`

		ConnectCandidateNode func(p0 context.Context, p1 *types.ConnectOptions) error `perm:"write"`

		ConnectEdgeNode func(p0 context.Context, p1 *types.ConnectOptions) error `perm:"write"`

		ConnectLocator func(p0 context.Context, p1 string, p2 string) error `perm:"write"`

		CreateNodeAuthToken func(p0 context.Context, p1 string, p2 string) (string, error) `perm:"read"`

		DeleteEdgeUpdateInfo func(p0 context.Context, p1 int) error `perm:"admin"`

		EdgeDownloadInfos func(p0 context.Context, p1 string) ([]*types.DownloadInfo, error) `perm:"read"`

		EdgeExternalServiceAddress func(p0 context.Context, p1 string, p2 string) (string, error) `perm:"write"`

		EdgeUpdateInfos func(p0 context.Context) (map[int]*EdgeUpdateInfo, error) `perm:"read"`

		RetrieveCandidateDownloadSources func(p0 context.Context, p1 string) ([]*types.DownloadSource, error) `perm:"read"`

		GetNodeAppUpdateInfos func(p0 context.Context) (map[int]*EdgeUpdateInfo, error) `perm:"read"`

		GetNodeExternalAddress func(p0 context.Context) (string, error) `perm:"read"`

		GetNodeList func(p0 context.Context, p1 int, p2 int) (*types.ListNodesRsp, error) `perm:"read"`

		GetOnlineNodeList func(p0 context.Context, p1 types.NodeType) ([]string, error) `perm:"read"`

		GetServerPublicKey func(p0 context.Context) (string, error) `perm:"write"`

		GetValidationResultList func(p0 context.Context, p1 time.Time, p2 time.Time, p3 int, p4 int) (*types.ListValidateResultRsp, error) `perm:"read"`

		NatTravel func(p0 context.Context, p1 *types.NatTravelReq) error `perm:"read"`

		NodeNatType func(p0 context.Context, p1 string) (types.NatType, error) `perm:"write"`

		NodeQuit func(p0 context.Context, p1 string) error `perm:"admin"`

		ProcessNodeValidationResult func(p0 context.Context, p1 ValidateResult) error `perm:"write"`

		RegisterNewNode func(p0 context.Context, p1 string, p2 string, p3 types.NodeType) error `perm:"admin"`

		RemoveAsset func(p0 context.Context, p1 string) error `perm:"admin"`

		RemoveAssetResult func(p0 context.Context, p1 types.RemoveAssetResult) error `perm:"write"`

		ResetAssetExpiration func(p0 context.Context, p1 string, p2 time.Time) error `perm:"admin"`

		RestartFailedAssets func(p0 context.Context, p1 []types.AssetHash) error `perm:"admin"`

		RetrieveNodeInfo func(p0 context.Context, p1 string) (types.NodeInfo, error) `perm:"read"`

		SetEdgeUpdateInfo func(p0 context.Context, p1 *EdgeUpdateInfo) error `perm:"admin"`

		IgnoreProofOfWork func(p0 context.Context, p1 []*types.NodeWorkloadProof) error `perm:"read"`

		TriggerElection func(p0 context.Context) error `perm:"admin"`

		UpdateNodePort func(p0 context.Context, p1 string, p2 string) error `perm:"admin"`

		UserDownloadBlockResults func(p0 context.Context, p1 []types.UserBlockDownloadResult) error `perm:"read"`

		UserDownloadResult func(p0 context.Context, p1 types.UserDownloadResult) error `perm:"write"`

		VerifyNodeAuthToken func(p0 context.Context, p1 string) ([]auth.Permission, error) `perm:"read"`
	}
}

type SchedulerStub struct {
	CommonStub
}

type ValidateStruct struct {
	Internal struct {
		BeValidate func(p0 context.Context, p1 *BeValidateReq) error `perm:"read"`
	}
}

type ValidateStub struct {
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

func (s *CarfileOperationStruct) CachedProgresses(p0 context.Context, p1 []string) (*types.PullResult, error) {
	if s.Internal.CachedProgresses == nil {
		return nil, ErrNotSupported
	}
	return s.Internal.CachedProgresses(p0, p1)
}

func (s *CarfileOperationStub) CachedProgresses(p0 context.Context, p1 []string) (*types.PullResult, error) {
	return nil, ErrNotSupported
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

func (s *CarfileOperationStruct) QueryCachingCarfile(p0 context.Context) (*types.CachingAsset, error) {
	if s.Internal.QueryCachingCarfile == nil {
		return nil, ErrNotSupported
	}
	return s.Internal.QueryCachingCarfile(p0)
}

func (s *CarfileOperationStub) QueryCachingCarfile(p0 context.Context) (*types.CachingAsset, error) {
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

func (s *CommonStruct) Discover(p0 context.Context) (types.OpenRPCDocument, error) {
	if s.Internal.Discover == nil {
		return *new(types.OpenRPCDocument), ErrNotSupported
	}
	return s.Internal.Discover(p0)
}

func (s *CommonStub) Discover(p0 context.Context) (types.OpenRPCDocument, error) {
	return *new(types.OpenRPCDocument), ErrNotSupported
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

func (s *DataSyncStruct) CompareBucketHashes(p0 context.Context, p1 map[uint32]string) ([]uint32, error) {
	if s.Internal.CompareBucketHashes == nil {
		return *new([]uint32), ErrNotSupported
	}
	return s.Internal.CompareBucketHashes(p0, p1)
}

func (s *DataSyncStub) CompareBucketHashes(p0 context.Context, p1 map[uint32]string) ([]uint32, error) {
	return *new([]uint32), ErrNotSupported
}

func (s *DataSyncStruct) CompareTopHash(p0 context.Context, p1 string) (bool, error) {
	if s.Internal.CompareTopHash == nil {
		return false, ErrNotSupported
	}
	return s.Internal.CompareTopHash(p0, p1)
}

func (s *DataSyncStub) CompareTopHash(p0 context.Context, p1 string) (bool, error) {
	return false, ErrNotSupported
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

func (s *EdgeStruct) UserNATTravel(p0 context.Context, p1 string, p2 *types.NatTravelReq) error {
	if s.Internal.UserNATTravel == nil {
		return ErrNotSupported
	}
	return s.Internal.UserNATTravel(p0, p1, p2)
}

func (s *EdgeStub) UserNATTravel(p0 context.Context, p1 string, p2 *types.NatTravelReq) error {
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

func (s *LocatorStruct) RegisterNewNode(p0 context.Context, p1 string, p2 string, p3 string, p4 types.NodeType) error {
	if s.Internal.RegisterNewNode == nil {
		return ErrNotSupported
	}
	return s.Internal.RegisterNewNode(p0, p1, p2, p3, p4)
}

func (s *LocatorStub) RegisterNewNode(p0 context.Context, p1 string, p2 string, p3 string, p4 types.NodeType) error {
	return ErrNotSupported
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

func (s *SchedulerStruct) GetAssetListForBucket(p0 context.Context, p1 string) ([]string, error) {
	if s.Internal.GetAssetListForBucket == nil {
		return *new([]string), ErrNotSupported
	}
	return s.Internal.GetAssetListForBucket(p0, p1)
}

func (s *SchedulerStub) GetAssetListForBucket(p0 context.Context, p1 string) ([]string, error) {
	return *new([]string), ErrNotSupported
}

func (s *SchedulerStruct) AssetRecord(p0 context.Context, p1 string) (*types.AssetRecord, error) {
	if s.Internal.AssetRecord == nil {
		return nil, ErrNotSupported
	}
	return s.Internal.AssetRecord(p0, p1)
}

func (s *SchedulerStub) AssetRecord(p0 context.Context, p1 string) (*types.AssetRecord, error) {
	return nil, ErrNotSupported
}

func (s *SchedulerStruct) AssetRecords(p0 context.Context, p1 int, p2 int, p3 []string) ([]*types.AssetRecord, error) {
	if s.Internal.AssetRecords == nil {
		return *new([]*types.AssetRecord), ErrNotSupported
	}
	return s.Internal.AssetRecords(p0, p1, p2, p3)
}

func (s *SchedulerStub) AssetRecords(p0 context.Context, p1 int, p2 int, p3 []string) ([]*types.AssetRecord, error) {
	return *new([]*types.AssetRecord), ErrNotSupported
}

func (s *SchedulerStruct) AssetReplicaList(p0 context.Context, p1 types.ListReplicaInfosReq) (*types.ListReplicaInfosRsp, error) {
	if s.Internal.AssetReplicaList == nil {
		return nil, ErrNotSupported
	}
	return s.Internal.AssetReplicaList(p0, p1)
}

func (s *SchedulerStub) AssetReplicaList(p0 context.Context, p1 types.ListReplicaInfosReq) (*types.ListReplicaInfosRsp, error) {
	return nil, ErrNotSupported
}

func (s *SchedulerStruct) CacheAsset(p0 context.Context, p1 *types.PullAssetReq) error {
	if s.Internal.CacheAsset == nil {
		return ErrNotSupported
	}
	return s.Internal.CacheAsset(p0, p1)
}

func (s *SchedulerStub) CacheAsset(p0 context.Context, p1 *types.PullAssetReq) error {
	return ErrNotSupported
}

func (s *SchedulerStruct) CheckNetworkConnectivity(p0 context.Context, p1 string, p2 string) error {
	if s.Internal.CheckNetworkConnectivity == nil {
		return ErrNotSupported
	}
	return s.Internal.CheckNetworkConnectivity(p0, p1, p2)
}

func (s *SchedulerStub) CheckNetworkConnectivity(p0 context.Context, p1 string, p2 string) error {
	return ErrNotSupported
}

func (s *SchedulerStruct) ConnectCandidateNode(p0 context.Context, p1 *types.ConnectOptions) error {
	if s.Internal.ConnectCandidateNode == nil {
		return ErrNotSupported
	}
	return s.Internal.ConnectCandidateNode(p0, p1)
}

func (s *SchedulerStub) ConnectCandidateNode(p0 context.Context, p1 *types.ConnectOptions) error {
	return ErrNotSupported
}

func (s *SchedulerStruct) ConnectEdgeNode(p0 context.Context, p1 *types.ConnectOptions) error {
	if s.Internal.ConnectEdgeNode == nil {
		return ErrNotSupported
	}
	return s.Internal.ConnectEdgeNode(p0, p1)
}

func (s *SchedulerStub) ConnectEdgeNode(p0 context.Context, p1 *types.ConnectOptions) error {
	return ErrNotSupported
}

func (s *SchedulerStruct) ConnectLocator(p0 context.Context, p1 string, p2 string) error {
	if s.Internal.ConnectLocator == nil {
		return ErrNotSupported
	}
	return s.Internal.ConnectLocator(p0, p1, p2)
}

func (s *SchedulerStub) ConnectLocator(p0 context.Context, p1 string, p2 string) error {
	return ErrNotSupported
}

func (s *SchedulerStruct) CreateNodeAuthToken(p0 context.Context, p1 string, p2 string) (string, error) {
	if s.Internal.CreateNodeAuthToken == nil {
		return "", ErrNotSupported
	}
	return s.Internal.CreateNodeAuthToken(p0, p1, p2)
}

func (s *SchedulerStub) CreateNodeAuthToken(p0 context.Context, p1 string, p2 string) (string, error) {
	return "", ErrNotSupported
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

func (s *SchedulerStruct) EdgeUpdateInfos(p0 context.Context) (map[int]*EdgeUpdateInfo, error) {
	if s.Internal.EdgeUpdateInfos == nil {
		return *new(map[int]*EdgeUpdateInfo), ErrNotSupported
	}
	return s.Internal.EdgeUpdateInfos(p0)
}

func (s *SchedulerStub) EdgeUpdateInfos(p0 context.Context) (map[int]*EdgeUpdateInfo, error) {
	return *new(map[int]*EdgeUpdateInfo), ErrNotSupported
}

func (s *SchedulerStruct) RetrieveCandidateDownloadSources(p0 context.Context, p1 string) ([]*types.DownloadSource, error) {
	if s.Internal.RetrieveCandidateDownloadSources == nil {
		return *new([]*types.DownloadSource), ErrNotSupported
	}
	return s.Internal.RetrieveCandidateDownloadSources(p0, p1)
}

func (s *SchedulerStub) RetrieveCandidateDownloadSources(p0 context.Context, p1 string) ([]*types.DownloadSource, error) {
	return *new([]*types.DownloadSource), ErrNotSupported
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

func (s *SchedulerStruct) GetNodeExternalAddress(p0 context.Context) (string, error) {
	if s.Internal.GetNodeExternalAddress == nil {
		return "", ErrNotSupported
	}
	return s.Internal.GetNodeExternalAddress(p0)
}

func (s *SchedulerStub) GetNodeExternalAddress(p0 context.Context) (string, error) {
	return "", ErrNotSupported
}

func (s *SchedulerStruct) GetNodeList(p0 context.Context, p1 int, p2 int) (*types.ListNodesRsp, error) {
	if s.Internal.GetNodeList == nil {
		return nil, ErrNotSupported
	}
	return s.Internal.GetNodeList(p0, p1, p2)
}

func (s *SchedulerStub) GetNodeList(p0 context.Context, p1 int, p2 int) (*types.ListNodesRsp, error) {
	return nil, ErrNotSupported
}

func (s *SchedulerStruct) GetOnlineNodeList(p0 context.Context, p1 types.NodeType) ([]string, error) {
	if s.Internal.GetOnlineNodeList == nil {
		return *new([]string), ErrNotSupported
	}
	return s.Internal.GetOnlineNodeList(p0, p1)
}

func (s *SchedulerStub) GetOnlineNodeList(p0 context.Context, p1 types.NodeType) ([]string, error) {
	return *new([]string), ErrNotSupported
}

func (s *SchedulerStruct) GetServerPublicKey(p0 context.Context) (string, error) {
	if s.Internal.GetServerPublicKey == nil {
		return "", ErrNotSupported
	}
	return s.Internal.GetServerPublicKey(p0)
}

func (s *SchedulerStub) GetServerPublicKey(p0 context.Context) (string, error) {
	return "", ErrNotSupported
}

func (s *SchedulerStruct) GetValidationResultList(p0 context.Context, p1 time.Time, p2 time.Time, p3 int, p4 int) (*types.ListValidateResultRsp, error) {
	if s.Internal.GetValidationResultList == nil {
		return nil, ErrNotSupported
	}
	return s.Internal.GetValidationResultList(p0, p1, p2, p3, p4)
}

func (s *SchedulerStub) GetValidationResultList(p0 context.Context, p1 time.Time, p2 time.Time, p3 int, p4 int) (*types.ListValidateResultRsp, error) {
	return nil, ErrNotSupported
}

func (s *SchedulerStruct) NatTravel(p0 context.Context, p1 *types.NatTravelReq) error {
	if s.Internal.NatTravel == nil {
		return ErrNotSupported
	}
	return s.Internal.NatTravel(p0, p1)
}

func (s *SchedulerStub) NatTravel(p0 context.Context, p1 *types.NatTravelReq) error {
	return ErrNotSupported
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

func (s *SchedulerStruct) ProcessNodeValidationResult(p0 context.Context, p1 ValidateResult) error {
	if s.Internal.ProcessNodeValidationResult == nil {
		return ErrNotSupported
	}
	return s.Internal.ProcessNodeValidationResult(p0, p1)
}

func (s *SchedulerStub) ProcessNodeValidationResult(p0 context.Context, p1 ValidateResult) error {
	return ErrNotSupported
}

func (s *SchedulerStruct) RegisterNewNode(p0 context.Context, p1 string, p2 string, p3 types.NodeType) error {
	if s.Internal.RegisterNewNode == nil {
		return ErrNotSupported
	}
	return s.Internal.RegisterNewNode(p0, p1, p2, p3)
}

func (s *SchedulerStub) RegisterNewNode(p0 context.Context, p1 string, p2 string, p3 types.NodeType) error {
	return ErrNotSupported
}

func (s *SchedulerStruct) RemoveAsset(p0 context.Context, p1 string) error {
	if s.Internal.RemoveAsset == nil {
		return ErrNotSupported
	}
	return s.Internal.RemoveAsset(p0, p1)
}

func (s *SchedulerStub) RemoveAsset(p0 context.Context, p1 string) error {
	return ErrNotSupported
}

func (s *SchedulerStruct) RemoveAssetResult(p0 context.Context, p1 types.RemoveAssetResult) error {
	if s.Internal.RemoveAssetResult == nil {
		return ErrNotSupported
	}
	return s.Internal.RemoveAssetResult(p0, p1)
}

func (s *SchedulerStub) RemoveAssetResult(p0 context.Context, p1 types.RemoveAssetResult) error {
	return ErrNotSupported
}

func (s *SchedulerStruct) ResetAssetExpiration(p0 context.Context, p1 string, p2 time.Time) error {
	if s.Internal.ResetAssetExpiration == nil {
		return ErrNotSupported
	}
	return s.Internal.ResetAssetExpiration(p0, p1, p2)
}

func (s *SchedulerStub) ResetAssetExpiration(p0 context.Context, p1 string, p2 time.Time) error {
	return ErrNotSupported
}

func (s *SchedulerStruct) RestartFailedAssets(p0 context.Context, p1 []types.AssetHash) error {
	if s.Internal.RestartFailedAssets == nil {
		return ErrNotSupported
	}
	return s.Internal.RestartFailedAssets(p0, p1)
}

func (s *SchedulerStub) RestartFailedAssets(p0 context.Context, p1 []types.AssetHash) error {
	return ErrNotSupported
}

func (s *SchedulerStruct) RetrieveNodeInfo(p0 context.Context, p1 string) (types.NodeInfo, error) {
	if s.Internal.RetrieveNodeInfo == nil {
		return *new(types.NodeInfo), ErrNotSupported
	}
	return s.Internal.RetrieveNodeInfo(p0, p1)
}

func (s *SchedulerStub) RetrieveNodeInfo(p0 context.Context, p1 string) (types.NodeInfo, error) {
	return *new(types.NodeInfo), ErrNotSupported
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

func (s *SchedulerStruct) IgnoreProofOfWork(p0 context.Context, p1 []*types.NodeWorkloadProof) error {
	if s.Internal.IgnoreProofOfWork == nil {
		return ErrNotSupported
	}
	return s.Internal.IgnoreProofOfWork(p0, p1)
}

func (s *SchedulerStub) IgnoreProofOfWork(p0 context.Context, p1 []*types.NodeWorkloadProof) error {
	return ErrNotSupported
}

func (s *SchedulerStruct) TriggerElection(p0 context.Context) error {
	if s.Internal.TriggerElection == nil {
		return ErrNotSupported
	}
	return s.Internal.TriggerElection(p0)
}

func (s *SchedulerStub) TriggerElection(p0 context.Context) error {
	return ErrNotSupported
}

func (s *SchedulerStruct) UpdateNodePort(p0 context.Context, p1 string, p2 string) error {
	if s.Internal.UpdateNodePort == nil {
		return ErrNotSupported
	}
	return s.Internal.UpdateNodePort(p0, p1, p2)
}

func (s *SchedulerStub) UpdateNodePort(p0 context.Context, p1 string, p2 string) error {
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

func (s *SchedulerStruct) VerifyNodeAuthToken(p0 context.Context, p1 string) ([]auth.Permission, error) {
	if s.Internal.VerifyNodeAuthToken == nil {
		return *new([]auth.Permission), ErrNotSupported
	}
	return s.Internal.VerifyNodeAuthToken(p0, p1)
}

func (s *SchedulerStub) VerifyNodeAuthToken(p0 context.Context, p1 string) ([]auth.Permission, error) {
	return *new([]auth.Permission), ErrNotSupported
}

func (s *ValidateStruct) BeValidate(p0 context.Context, p1 *BeValidateReq) error {
	if s.Internal.BeValidate == nil {
		return ErrNotSupported
	}
	return s.Internal.BeValidate(p0, p1)
}

func (s *ValidateStub) BeValidate(p0 context.Context, p1 *BeValidateReq) error {
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
