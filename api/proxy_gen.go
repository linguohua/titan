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

type AssetStruct struct {
	Internal struct {
		DeleteAsset func(p0 context.Context, p1 string) error `perm:"write"`

		GetAssetProgresses func(p0 context.Context, p1 []string) (*types.PullResult, error) `perm:"write"`

		GetAssetStats func(p0 context.Context) (*types.AssetStats, error) `perm:"write"`

		GetPullingAssetInfo func(p0 context.Context) (*types.InProgressAsset, error) `perm:"write"`

		PullAsset func(p0 context.Context, p1 string, p2 []*types.CandidateDownloadInfo) error `perm:"write"`
	}
}

type AssetStub struct {
}

type CandidateStruct struct {
	CommonStruct

	DeviceStruct

	ValidationStruct

	DataSyncStruct

	AssetStruct

	Internal struct {
		GetBlocksWithAssetCID func(p0 context.Context, p1 string, p2 int64, p3 int) (map[int]string, error) `perm:"read"`

		WaitQuiet func(p0 context.Context) error `perm:"read"`
	}
}

type CandidateStub struct {
	CommonStub

	DeviceStub

	ValidationStub

	DataSyncStub

	AssetStub
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

		Session func(p0 context.Context) (uuid.UUID, error) `perm:"write"`

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
		GetNodeID func(p0 context.Context) (string, error) `perm:"read"`

		GetNodeInfo func(p0 context.Context) (types.NodeInfo, error) `perm:"read"`
	}
}

type DeviceStub struct {
}

type EdgeStruct struct {
	CommonStruct

	DeviceStruct

	ValidationStruct

	DataSyncStruct

	AssetStruct

	Internal struct {
		ExternalServiceAddress func(p0 context.Context, p1 string) (string, error) `perm:"write"`

		UserNATPunch func(p0 context.Context, p1 string, p2 *types.NatPunchReq) error `perm:"write"`

		WaitQuiet func(p0 context.Context) error `perm:"read"`
	}
}

type EdgeStub struct {
	CommonStub

	DeviceStub

	ValidationStub

	DataSyncStub

	AssetStub
}

type LocatorStruct struct {
	CommonStruct

	Internal struct {
		AddAccessPoint func(p0 context.Context, p1 string, p2 string, p3 int, p4 string) error `perm:"admin"`

		EdgeDownloadInfos func(p0 context.Context, p1 string) (*types.EdgeDownloadInfoList, error) `perm:"read"`

		GetAccessPoint func(p0 context.Context, p1 string) (AccessPoint, error) `perm:"admin"`

		GetAccessPoints func(p0 context.Context, p1 string) ([]string, error) `perm:"read"`

		GetUserAccessPoint func(p0 context.Context, p1 string) (AccessPoint, error) `perm:"admin"`

		GetWebAccessPoints func(p0 context.Context) ([]AccessPoint, error) `perm:"admin"`

		ListAreaIDs func(p0 context.Context) ([]string, error) `perm:"admin"`

		RegisterNode func(p0 context.Context, p1 string, p2 string, p3 string, p4 types.NodeType) error `perm:"admin"`

		RemoveAccessPoints func(p0 context.Context, p1 string) error `perm:"admin"`

		UpdateNodeOnlineStatus func(p0 context.Context, p1 string, p2 bool) error `perm:"write"`
	}
}

type LocatorStub struct {
	CommonStub
}

type SchedulerStruct struct {
	CommonStruct

	Internal struct {
		CandidateConnect func(p0 context.Context, p1 *types.ConnectOptions) error `perm:"write"`

		CheckNetworkConnectivity func(p0 context.Context, p1 string, p2 string) error `perm:"read"`

		NodeLogin func(p0 context.Context, p1 string, p2 string) (string, error) `perm:"read"`

		DeleteEdgeUpdateConfig func(p0 context.Context, p1 int) error `perm:"admin"`

		EdgeConnect func(p0 context.Context, p1 *types.ConnectOptions) error `perm:"write"`

		GetAssetListForBucket func(p0 context.Context, p1 string) ([]string, error) `perm:"write"`

		GetAssetRecord func(p0 context.Context, p1 string) (*types.AssetRecord, error) `perm:"read"`

		GetAssetRecords func(p0 context.Context, p1 int, p2 int, p3 []string) ([]*types.AssetRecord, error) `perm:"read"`

		GetAssetReplicaInfos func(p0 context.Context, p1 types.ListReplicaInfosReq) (*types.ListReplicaInfosRsp, error) `perm:"read"`

		GetCandidateDownloadInfos func(p0 context.Context, p1 string) ([]*types.CandidateDownloadInfo, error) `perm:"read"`

		GetEdgeDownloadInfos func(p0 context.Context, p1 string) (*types.EdgeDownloadInfoList, error) `perm:"read"`

		GetEdgeExternalServiceAddress func(p0 context.Context, p1 string, p2 string) (string, error) `perm:"write"`

		GetEdgeUpdateConfigs func(p0 context.Context) (map[int]*EdgeUpdateConfig, error) `perm:"read"`

		GetExternalAddress func(p0 context.Context) (string, error) `perm:"read"`

		GetNodeInfo func(p0 context.Context, p1 string) (types.NodeInfo, error) `perm:"read"`

		GetNodeList func(p0 context.Context, p1 int, p2 int) (*types.ListNodesRsp, error) `perm:"read"`

		GetNodeNATType func(p0 context.Context, p1 string) (types.NatType, error) `perm:"write"`

		GetOnlineNodeCount func(p0 context.Context, p1 types.NodeType) (int, error) `perm:"read"`

		GetSchedulerPublicKey func(p0 context.Context) (string, error) `perm:"write"`

		GetValidationResults func(p0 context.Context, p1 time.Time, p2 time.Time, p3 int, p4 int) (*types.ListValidationResultRsp, error) `perm:"read"`

		NatPunch func(p0 context.Context, p1 *types.NatPunchReq) error `perm:"read"`

		NodeRemoveAssetResult func(p0 context.Context, p1 types.RemoveAssetResult) error `perm:"write"`

		NodeValidationResult func(p0 context.Context, p1 ValidationResult) error `perm:"write"`

		PullAsset func(p0 context.Context, p1 *types.PullAssetReq) error `perm:"admin"`

		RePullFailedAssets func(p0 context.Context, p1 []types.AssetHash) error `perm:"admin"`

		RegisterNode func(p0 context.Context, p1 string, p2 string, p3 types.NodeType) error `perm:"admin"`

		RemoveAssetRecord func(p0 context.Context, p1 string) error `perm:"admin"`

		RemoveAssetReplica func(p0 context.Context, p1 string, p2 string) error `perm:"admin"`

		SetEdgeUpdateConfig func(p0 context.Context, p1 *EdgeUpdateConfig) error `perm:"admin"`

		SubmitUserProofsOfWork func(p0 context.Context, p1 []*types.UserProofOfWork) error `perm:"read"`

		TriggerElection func(p0 context.Context) error `perm:"admin"`

		UnregisterNode func(p0 context.Context, p1 string) error `perm:"admin"`

		UpdateAssetExpiration func(p0 context.Context, p1 string, p2 time.Time) error `perm:"admin"`

		UpdateNodePort func(p0 context.Context, p1 string, p2 string) error `perm:"admin"`

		VerifyNodeAuthToken func(p0 context.Context, p1 string) ([]auth.Permission, error) `perm:"read"`
	}
}

type SchedulerStub struct {
	CommonStub
}

type ValidationStruct struct {
	Internal struct {
		Validate func(p0 context.Context, p1 *ValidateReq) error `perm:"read"`
	}
}

type ValidationStub struct {
}

func (s *AssetStruct) DeleteAsset(p0 context.Context, p1 string) error {
	if s.Internal.DeleteAsset == nil {
		return ErrNotSupported
	}
	return s.Internal.DeleteAsset(p0, p1)
}

func (s *AssetStub) DeleteAsset(p0 context.Context, p1 string) error {
	return ErrNotSupported
}

func (s *AssetStruct) GetAssetProgresses(p0 context.Context, p1 []string) (*types.PullResult, error) {
	if s.Internal.GetAssetProgresses == nil {
		return nil, ErrNotSupported
	}
	return s.Internal.GetAssetProgresses(p0, p1)
}

func (s *AssetStub) GetAssetProgresses(p0 context.Context, p1 []string) (*types.PullResult, error) {
	return nil, ErrNotSupported
}

func (s *AssetStruct) GetAssetStats(p0 context.Context) (*types.AssetStats, error) {
	if s.Internal.GetAssetStats == nil {
		return nil, ErrNotSupported
	}
	return s.Internal.GetAssetStats(p0)
}

func (s *AssetStub) GetAssetStats(p0 context.Context) (*types.AssetStats, error) {
	return nil, ErrNotSupported
}

func (s *AssetStruct) GetPullingAssetInfo(p0 context.Context) (*types.InProgressAsset, error) {
	if s.Internal.GetPullingAssetInfo == nil {
		return nil, ErrNotSupported
	}
	return s.Internal.GetPullingAssetInfo(p0)
}

func (s *AssetStub) GetPullingAssetInfo(p0 context.Context) (*types.InProgressAsset, error) {
	return nil, ErrNotSupported
}

func (s *AssetStruct) PullAsset(p0 context.Context, p1 string, p2 []*types.CandidateDownloadInfo) error {
	if s.Internal.PullAsset == nil {
		return ErrNotSupported
	}
	return s.Internal.PullAsset(p0, p1, p2)
}

func (s *AssetStub) PullAsset(p0 context.Context, p1 string, p2 []*types.CandidateDownloadInfo) error {
	return ErrNotSupported
}

func (s *CandidateStruct) GetBlocksWithAssetCID(p0 context.Context, p1 string, p2 int64, p3 int) (map[int]string, error) {
	if s.Internal.GetBlocksWithAssetCID == nil {
		return *new(map[int]string), ErrNotSupported
	}
	return s.Internal.GetBlocksWithAssetCID(p0, p1, p2, p3)
}

func (s *CandidateStub) GetBlocksWithAssetCID(p0 context.Context, p1 string, p2 int64, p3 int) (map[int]string, error) {
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

func (s *DeviceStruct) GetNodeID(p0 context.Context) (string, error) {
	if s.Internal.GetNodeID == nil {
		return "", ErrNotSupported
	}
	return s.Internal.GetNodeID(p0)
}

func (s *DeviceStub) GetNodeID(p0 context.Context) (string, error) {
	return "", ErrNotSupported
}

func (s *DeviceStruct) GetNodeInfo(p0 context.Context) (types.NodeInfo, error) {
	if s.Internal.GetNodeInfo == nil {
		return *new(types.NodeInfo), ErrNotSupported
	}
	return s.Internal.GetNodeInfo(p0)
}

func (s *DeviceStub) GetNodeInfo(p0 context.Context) (types.NodeInfo, error) {
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

func (s *EdgeStruct) UserNATPunch(p0 context.Context, p1 string, p2 *types.NatPunchReq) error {
	if s.Internal.UserNATPunch == nil {
		return ErrNotSupported
	}
	return s.Internal.UserNATPunch(p0, p1, p2)
}

func (s *EdgeStub) UserNATPunch(p0 context.Context, p1 string, p2 *types.NatPunchReq) error {
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

func (s *LocatorStruct) EdgeDownloadInfos(p0 context.Context, p1 string) (*types.EdgeDownloadInfoList, error) {
	if s.Internal.EdgeDownloadInfos == nil {
		return nil, ErrNotSupported
	}
	return s.Internal.EdgeDownloadInfos(p0, p1)
}

func (s *LocatorStub) EdgeDownloadInfos(p0 context.Context, p1 string) (*types.EdgeDownloadInfoList, error) {
	return nil, ErrNotSupported
}

func (s *LocatorStruct) GetAccessPoint(p0 context.Context, p1 string) (AccessPoint, error) {
	if s.Internal.GetAccessPoint == nil {
		return *new(AccessPoint), ErrNotSupported
	}
	return s.Internal.GetAccessPoint(p0, p1)
}

func (s *LocatorStub) GetAccessPoint(p0 context.Context, p1 string) (AccessPoint, error) {
	return *new(AccessPoint), ErrNotSupported
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

func (s *LocatorStruct) GetUserAccessPoint(p0 context.Context, p1 string) (AccessPoint, error) {
	if s.Internal.GetUserAccessPoint == nil {
		return *new(AccessPoint), ErrNotSupported
	}
	return s.Internal.GetUserAccessPoint(p0, p1)
}

func (s *LocatorStub) GetUserAccessPoint(p0 context.Context, p1 string) (AccessPoint, error) {
	return *new(AccessPoint), ErrNotSupported
}

func (s *LocatorStruct) GetWebAccessPoints(p0 context.Context) ([]AccessPoint, error) {
	if s.Internal.GetWebAccessPoints == nil {
		return *new([]AccessPoint), ErrNotSupported
	}
	return s.Internal.GetWebAccessPoints(p0)
}

func (s *LocatorStub) GetWebAccessPoints(p0 context.Context) ([]AccessPoint, error) {
	return *new([]AccessPoint), ErrNotSupported
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

func (s *LocatorStruct) RegisterNode(p0 context.Context, p1 string, p2 string, p3 string, p4 types.NodeType) error {
	if s.Internal.RegisterNode == nil {
		return ErrNotSupported
	}
	return s.Internal.RegisterNode(p0, p1, p2, p3, p4)
}

func (s *LocatorStub) RegisterNode(p0 context.Context, p1 string, p2 string, p3 string, p4 types.NodeType) error {
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

func (s *LocatorStruct) UpdateNodeOnlineStatus(p0 context.Context, p1 string, p2 bool) error {
	if s.Internal.UpdateNodeOnlineStatus == nil {
		return ErrNotSupported
	}
	return s.Internal.UpdateNodeOnlineStatus(p0, p1, p2)
}

func (s *LocatorStub) UpdateNodeOnlineStatus(p0 context.Context, p1 string, p2 bool) error {
	return ErrNotSupported
}

func (s *SchedulerStruct) CandidateConnect(p0 context.Context, p1 *types.ConnectOptions) error {
	if s.Internal.CandidateConnect == nil {
		return ErrNotSupported
	}
	return s.Internal.CandidateConnect(p0, p1)
}

func (s *SchedulerStub) CandidateConnect(p0 context.Context, p1 *types.ConnectOptions) error {
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

func (s *SchedulerStruct) NodeLogin(p0 context.Context, p1 string, p2 string) (string, error) {
	if s.Internal.NodeLogin == nil {
		return "", ErrNotSupported
	}
	return s.Internal.NodeLogin(p0, p1, p2)
}

func (s *SchedulerStub) NodeLogin(p0 context.Context, p1 string, p2 string) (string, error) {
	return "", ErrNotSupported
}

func (s *SchedulerStruct) DeleteEdgeUpdateConfig(p0 context.Context, p1 int) error {
	if s.Internal.DeleteEdgeUpdateConfig == nil {
		return ErrNotSupported
	}
	return s.Internal.DeleteEdgeUpdateConfig(p0, p1)
}

func (s *SchedulerStub) DeleteEdgeUpdateConfig(p0 context.Context, p1 int) error {
	return ErrNotSupported
}

func (s *SchedulerStruct) EdgeConnect(p0 context.Context, p1 *types.ConnectOptions) error {
	if s.Internal.EdgeConnect == nil {
		return ErrNotSupported
	}
	return s.Internal.EdgeConnect(p0, p1)
}

func (s *SchedulerStub) EdgeConnect(p0 context.Context, p1 *types.ConnectOptions) error {
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

func (s *SchedulerStruct) GetAssetRecord(p0 context.Context, p1 string) (*types.AssetRecord, error) {
	if s.Internal.GetAssetRecord == nil {
		return nil, ErrNotSupported
	}
	return s.Internal.GetAssetRecord(p0, p1)
}

func (s *SchedulerStub) GetAssetRecord(p0 context.Context, p1 string) (*types.AssetRecord, error) {
	return nil, ErrNotSupported
}

func (s *SchedulerStruct) GetAssetRecords(p0 context.Context, p1 int, p2 int, p3 []string) ([]*types.AssetRecord, error) {
	if s.Internal.GetAssetRecords == nil {
		return *new([]*types.AssetRecord), ErrNotSupported
	}
	return s.Internal.GetAssetRecords(p0, p1, p2, p3)
}

func (s *SchedulerStub) GetAssetRecords(p0 context.Context, p1 int, p2 int, p3 []string) ([]*types.AssetRecord, error) {
	return *new([]*types.AssetRecord), ErrNotSupported
}

func (s *SchedulerStruct) GetAssetReplicaInfos(p0 context.Context, p1 types.ListReplicaInfosReq) (*types.ListReplicaInfosRsp, error) {
	if s.Internal.GetAssetReplicaInfos == nil {
		return nil, ErrNotSupported
	}
	return s.Internal.GetAssetReplicaInfos(p0, p1)
}

func (s *SchedulerStub) GetAssetReplicaInfos(p0 context.Context, p1 types.ListReplicaInfosReq) (*types.ListReplicaInfosRsp, error) {
	return nil, ErrNotSupported
}

func (s *SchedulerStruct) GetCandidateDownloadInfos(p0 context.Context, p1 string) ([]*types.CandidateDownloadInfo, error) {
	if s.Internal.GetCandidateDownloadInfos == nil {
		return *new([]*types.CandidateDownloadInfo), ErrNotSupported
	}
	return s.Internal.GetCandidateDownloadInfos(p0, p1)
}

func (s *SchedulerStub) GetCandidateDownloadInfos(p0 context.Context, p1 string) ([]*types.CandidateDownloadInfo, error) {
	return *new([]*types.CandidateDownloadInfo), ErrNotSupported
}

func (s *SchedulerStruct) GetEdgeDownloadInfos(p0 context.Context, p1 string) (*types.EdgeDownloadInfoList, error) {
	if s.Internal.GetEdgeDownloadInfos == nil {
		return nil, ErrNotSupported
	}
	return s.Internal.GetEdgeDownloadInfos(p0, p1)
}

func (s *SchedulerStub) GetEdgeDownloadInfos(p0 context.Context, p1 string) (*types.EdgeDownloadInfoList, error) {
	return nil, ErrNotSupported
}

func (s *SchedulerStruct) GetEdgeExternalServiceAddress(p0 context.Context, p1 string, p2 string) (string, error) {
	if s.Internal.GetEdgeExternalServiceAddress == nil {
		return "", ErrNotSupported
	}
	return s.Internal.GetEdgeExternalServiceAddress(p0, p1, p2)
}

func (s *SchedulerStub) GetEdgeExternalServiceAddress(p0 context.Context, p1 string, p2 string) (string, error) {
	return "", ErrNotSupported
}

func (s *SchedulerStruct) GetEdgeUpdateConfigs(p0 context.Context) (map[int]*EdgeUpdateConfig, error) {
	if s.Internal.GetEdgeUpdateConfigs == nil {
		return *new(map[int]*EdgeUpdateConfig), ErrNotSupported
	}
	return s.Internal.GetEdgeUpdateConfigs(p0)
}

func (s *SchedulerStub) GetEdgeUpdateConfigs(p0 context.Context) (map[int]*EdgeUpdateConfig, error) {
	return *new(map[int]*EdgeUpdateConfig), ErrNotSupported
}

func (s *SchedulerStruct) GetExternalAddress(p0 context.Context) (string, error) {
	if s.Internal.GetExternalAddress == nil {
		return "", ErrNotSupported
	}
	return s.Internal.GetExternalAddress(p0)
}

func (s *SchedulerStub) GetExternalAddress(p0 context.Context) (string, error) {
	return "", ErrNotSupported
}

func (s *SchedulerStruct) GetNodeInfo(p0 context.Context, p1 string) (types.NodeInfo, error) {
	if s.Internal.GetNodeInfo == nil {
		return *new(types.NodeInfo), ErrNotSupported
	}
	return s.Internal.GetNodeInfo(p0, p1)
}

func (s *SchedulerStub) GetNodeInfo(p0 context.Context, p1 string) (types.NodeInfo, error) {
	return *new(types.NodeInfo), ErrNotSupported
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

func (s *SchedulerStruct) GetNodeNATType(p0 context.Context, p1 string) (types.NatType, error) {
	if s.Internal.GetNodeNATType == nil {
		return *new(types.NatType), ErrNotSupported
	}
	return s.Internal.GetNodeNATType(p0, p1)
}

func (s *SchedulerStub) GetNodeNATType(p0 context.Context, p1 string) (types.NatType, error) {
	return *new(types.NatType), ErrNotSupported
}

func (s *SchedulerStruct) GetOnlineNodeCount(p0 context.Context, p1 types.NodeType) (int, error) {
	if s.Internal.GetOnlineNodeCount == nil {
		return 0, ErrNotSupported
	}
	return s.Internal.GetOnlineNodeCount(p0, p1)
}

func (s *SchedulerStub) GetOnlineNodeCount(p0 context.Context, p1 types.NodeType) (int, error) {
	return 0, ErrNotSupported
}

func (s *SchedulerStruct) GetSchedulerPublicKey(p0 context.Context) (string, error) {
	if s.Internal.GetSchedulerPublicKey == nil {
		return "", ErrNotSupported
	}
	return s.Internal.GetSchedulerPublicKey(p0)
}

func (s *SchedulerStub) GetSchedulerPublicKey(p0 context.Context) (string, error) {
	return "", ErrNotSupported
}

func (s *SchedulerStruct) GetValidationResults(p0 context.Context, p1 time.Time, p2 time.Time, p3 int, p4 int) (*types.ListValidationResultRsp, error) {
	if s.Internal.GetValidationResults == nil {
		return nil, ErrNotSupported
	}
	return s.Internal.GetValidationResults(p0, p1, p2, p3, p4)
}

func (s *SchedulerStub) GetValidationResults(p0 context.Context, p1 time.Time, p2 time.Time, p3 int, p4 int) (*types.ListValidationResultRsp, error) {
	return nil, ErrNotSupported
}

func (s *SchedulerStruct) NatPunch(p0 context.Context, p1 *types.NatPunchReq) error {
	if s.Internal.NatPunch == nil {
		return ErrNotSupported
	}
	return s.Internal.NatPunch(p0, p1)
}

func (s *SchedulerStub) NatPunch(p0 context.Context, p1 *types.NatPunchReq) error {
	return ErrNotSupported
}

func (s *SchedulerStruct) NodeRemoveAssetResult(p0 context.Context, p1 types.RemoveAssetResult) error {
	if s.Internal.NodeRemoveAssetResult == nil {
		return ErrNotSupported
	}
	return s.Internal.NodeRemoveAssetResult(p0, p1)
}

func (s *SchedulerStub) NodeRemoveAssetResult(p0 context.Context, p1 types.RemoveAssetResult) error {
	return ErrNotSupported
}

func (s *SchedulerStruct) NodeValidationResult(p0 context.Context, p1 ValidationResult) error {
	if s.Internal.NodeValidationResult == nil {
		return ErrNotSupported
	}
	return s.Internal.NodeValidationResult(p0, p1)
}

func (s *SchedulerStub) NodeValidationResult(p0 context.Context, p1 ValidationResult) error {
	return ErrNotSupported
}

func (s *SchedulerStruct) PullAsset(p0 context.Context, p1 *types.PullAssetReq) error {
	if s.Internal.PullAsset == nil {
		return ErrNotSupported
	}
	return s.Internal.PullAsset(p0, p1)
}

func (s *SchedulerStub) PullAsset(p0 context.Context, p1 *types.PullAssetReq) error {
	return ErrNotSupported
}

func (s *SchedulerStruct) RePullFailedAssets(p0 context.Context, p1 []types.AssetHash) error {
	if s.Internal.RePullFailedAssets == nil {
		return ErrNotSupported
	}
	return s.Internal.RePullFailedAssets(p0, p1)
}

func (s *SchedulerStub) RePullFailedAssets(p0 context.Context, p1 []types.AssetHash) error {
	return ErrNotSupported
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

func (s *SchedulerStruct) RemoveAssetRecord(p0 context.Context, p1 string) error {
	if s.Internal.RemoveAssetRecord == nil {
		return ErrNotSupported
	}
	return s.Internal.RemoveAssetRecord(p0, p1)
}

func (s *SchedulerStub) RemoveAssetRecord(p0 context.Context, p1 string) error {
	return ErrNotSupported
}

func (s *SchedulerStruct) RemoveAssetReplica(p0 context.Context, p1 string, p2 string) error {
	if s.Internal.RemoveAssetReplica == nil {
		return ErrNotSupported
	}
	return s.Internal.RemoveAssetReplica(p0, p1, p2)
}

func (s *SchedulerStub) RemoveAssetReplica(p0 context.Context, p1 string, p2 string) error {
	return ErrNotSupported
}

func (s *SchedulerStruct) SetEdgeUpdateConfig(p0 context.Context, p1 *EdgeUpdateConfig) error {
	if s.Internal.SetEdgeUpdateConfig == nil {
		return ErrNotSupported
	}
	return s.Internal.SetEdgeUpdateConfig(p0, p1)
}

func (s *SchedulerStub) SetEdgeUpdateConfig(p0 context.Context, p1 *EdgeUpdateConfig) error {
	return ErrNotSupported
}

func (s *SchedulerStruct) SubmitUserProofsOfWork(p0 context.Context, p1 []*types.UserProofOfWork) error {
	if s.Internal.SubmitUserProofsOfWork == nil {
		return ErrNotSupported
	}
	return s.Internal.SubmitUserProofsOfWork(p0, p1)
}

func (s *SchedulerStub) SubmitUserProofsOfWork(p0 context.Context, p1 []*types.UserProofOfWork) error {
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

func (s *SchedulerStruct) UnregisterNode(p0 context.Context, p1 string) error {
	if s.Internal.UnregisterNode == nil {
		return ErrNotSupported
	}
	return s.Internal.UnregisterNode(p0, p1)
}

func (s *SchedulerStub) UnregisterNode(p0 context.Context, p1 string) error {
	return ErrNotSupported
}

func (s *SchedulerStruct) UpdateAssetExpiration(p0 context.Context, p1 string, p2 time.Time) error {
	if s.Internal.UpdateAssetExpiration == nil {
		return ErrNotSupported
	}
	return s.Internal.UpdateAssetExpiration(p0, p1, p2)
}

func (s *SchedulerStub) UpdateAssetExpiration(p0 context.Context, p1 string, p2 time.Time) error {
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

func (s *SchedulerStruct) VerifyNodeAuthToken(p0 context.Context, p1 string) ([]auth.Permission, error) {
	if s.Internal.VerifyNodeAuthToken == nil {
		return *new([]auth.Permission), ErrNotSupported
	}
	return s.Internal.VerifyNodeAuthToken(p0, p1)
}

func (s *SchedulerStub) VerifyNodeAuthToken(p0 context.Context, p1 string) ([]auth.Permission, error) {
	return *new([]auth.Permission), ErrNotSupported
}

func (s *ValidationStruct) Validate(p0 context.Context, p1 *ValidateReq) error {
	if s.Internal.Validate == nil {
		return ErrNotSupported
	}
	return s.Internal.Validate(p0, p1)
}

func (s *ValidationStub) Validate(p0 context.Context, p1 *ValidateReq) error {
	return ErrNotSupported
}

var _ Asset = new(AssetStruct)
var _ Candidate = new(CandidateStruct)
var _ Common = new(CommonStruct)
var _ DataSync = new(DataSyncStruct)
var _ Device = new(DeviceStruct)
var _ Edge = new(EdgeStruct)
var _ Locator = new(LocatorStruct)
var _ Scheduler = new(SchedulerStruct)
var _ Validation = new(ValidationStruct)
