package assets

import (
	"context"
	"crypto"
	"database/sql"
	"sort"
	"sync"
	"time"

	"github.com/filecoin-project/go-statemachine"
	"github.com/ipfs/go-datastore"
	"github.com/linguohua/titan/api/types"

	"github.com/linguohua/titan/node/modules/dtypes"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/node/cidutil"
	titanrsa "github.com/linguohua/titan/node/rsa"
	"github.com/linguohua/titan/node/scheduler/node"
	"golang.org/x/xerrors"
)

var log = logging.Logger("asset")

const (
	nodePullAssetTimeout         = 60 * time.Second // Pull asset timeout (Unit:Second)
	checkExpirationTimerInterval = 60 * 30          // Check for expired asset interval (Unit:Second)
	maxPullingAssets             = 10               // Maximum number of asset pull
	maxNodeDiskUsage             = 95.0             // If the node disk size is greater than this value, pulling will not continue
	seedReplicaCount             = 1                // The number of pull replica in the first stage
	getProgressInterval          = 20 * time.Second // Get asset pull progress interval from node (Unit:Second)
)

// Manager asset replica manager
type Manager struct {
	nodeMgr       *node.Manager
	minExpiration time.Time // Minimum expiration time for asset

	statemachineWait   sync.WaitGroup
	assetStateMachines *statemachine.StateGroup

	lock      sync.Mutex
	apTickers map[string]*assetTicker // timeout timer for asset pulling

	schedulerConfig dtypes.GetSchedulerConfigFunc
}

type assetTicker struct {
	ticker *time.Ticker
	close  chan struct{}
}

func (t *assetTicker) run(job func() error) {
	for {
		select {
		case <-t.ticker.C:
			err := job()
			if err != nil {
				log.Error(err.Error())
				break
			}
			return
		case <-t.close:
			return
		}
	}
}

// NewManager return new manager instance
func NewManager(nodeManager *node.Manager, ds datastore.Batching, configFunc dtypes.GetSchedulerConfigFunc) *Manager {
	m := &Manager{
		nodeMgr:         nodeManager,
		minExpiration:   time.Now(),
		apTickers:       make(map[string]*assetTicker),
		schedulerConfig: configFunc,
	}

	m.statemachineWait.Add(1)
	m.assetStateMachines = statemachine.New(ds, m, AssetPullingInfo{})

	return m
}

// Run start asset statemachine and start ticker
func (m *Manager) Run(ctx context.Context) {
	if err := m.restartStateMachines(ctx); err != nil {
		log.Errorf("failed load sector states: %+v", err)
	}
	go m.checkExpirationTicker(ctx)
	go m.pullProgressTicker(ctx)
}

// Stop stop statemachine
func (m *Manager) Stop(ctx context.Context) error {
	if err := m.assetStateMachines.Stop(ctx); err != nil {
		return err
	}
	return nil
}

// check asset expiration
func (m *Manager) checkExpirationTicker(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(checkExpirationTimerInterval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.checkAssetsExpiration()
		case <-ctx.Done():
			return
		}
	}
}

// get asset pull progress timer
func (m *Manager) pullProgressTicker(ctx context.Context) {
	ticker := time.NewTicker(getProgressInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.nodesPullProgresses()
		case <-ctx.Done():
			return
		}
	}
}

func (m *Manager) nodesPullProgresses() {
	nodePulls := make(map[string][]string)

	// pulling assets
	for hash := range m.apTickers {
		cid, err := cidutil.HashString2CIDString(hash)
		if err != nil {
			log.Errorf("%s HashString2CIDString err:%s", hash, err.Error())
			continue
		}

		nodes, err := m.nodeMgr.LoadPullingNodes(hash)
		if err != nil {
			log.Errorf("%s UnDoneNodes err:%s", hash, err.Error())
			continue
		}

		for _, nodeID := range nodes {
			list := nodePulls[nodeID]
			nodePulls[nodeID] = append(list, cid)
		}
	}

	getCP := func(nodeID string, cids []string) {
		// request node
		result, err := m.nodePullProgresses(nodeID, cids)
		if err != nil {
			log.Errorf("%s nodePullProgresses err:%s", nodeID, err.Error())
			return
		}

		// update asset info
		m.pullAssetsResult(nodeID, result)
	}

	for nodeID, cids := range nodePulls {
		go getCP(nodeID, cids)
	}
}

func (m *Manager) nodePullProgresses(nodeID string, cids []string) (result *types.CacheResult, err error) {
	log.Debugf("nodeID:%s, %v", nodeID, cids)

	cNode := m.nodeMgr.GetCandidateNode(nodeID)
	if cNode != nil {
		result, err = cNode.API().CachedProgresses(context.Background(), cids)
	} else {
		eNode := m.nodeMgr.GetEdgeNode(nodeID)
		if eNode != nil {
			result, err = eNode.API().CachedProgresses(context.Background(), cids)
		} else {
			err = xerrors.Errorf("node %s offline", nodeID)
		}
	}

	if err != nil {
		return
	}

	return
}

// PullAssets create a new pull asset task
func (m *Manager) PullAssets(info *types.PullAssetReq) error {
	m.statemachineWait.Wait()

	if len(m.apTickers) >= maxPullingAssets {
		return xerrors.Errorf("The asset in the pulling exceeds the limit %d, please wait", maxPullingAssets)
	}

	log.Debugf("asset event: %s, add asset replica: %d,expiration: %s", info.CID, info.Replicas, info.Expiration.String())

	cInfo, err := m.nodeMgr.LoadAssetRecord(info.Hash)
	if err != nil && err != sql.ErrNoRows {
		return err
	}

	if cInfo == nil {
		// create asset task
		return m.assetStateMachines.Send(AssetHash(info.Hash), AssetStartPulls{
			ID:                info.CID,
			Hash:              AssetHash(info.Hash),
			Replicas:          info.Replicas,
			ServerID:          info.ServerID,
			CreatedAt:         time.Now().Unix(),
			Expiration:        info.Expiration.Unix(),
			CandidateReplicas: m.GetCandidateReplicaCount(),
		})
	}

	return xerrors.New("asset exists")
}

// RestartPullAssets restart pull assets
func (m *Manager) RestartPullAssets(hashes []types.AssetHash) error {
	for _, hash := range hashes {
		err := m.assetStateMachines.Send(hash, PullAssetRestart{})
		if err != nil {
			log.Errorf("RestartPullAssets send err:%s", err.Error())
		}
	}

	return nil
}

// RemoveAsset remove a asset
func (m *Manager) RemoveAsset(cid, hash string) error {
	cInfos, err := m.nodeMgr.LoadAssetReplicas(hash)
	if err != nil {
		return xerrors.Errorf("LoadAssetReplicaInfos: %s,err:%s", cid, err.Error())
	}

	defer func() {
		err = m.nodeMgr.RemoveAssetRecord(hash)
		if err != nil {
			log.Errorf("%s RemoveAssetRecord db err: %s", hash, err.Error())
		}
	}()

	// remove asset
	err = m.assetStateMachines.Send(AssetHash(hash), AssetRemove{})
	if err != nil {
		return xerrors.Errorf("send to state machine err: %s ", err.Error())
	}

	log.Infof("asset event %s , remove asset", cid)

	go func() {
		for _, cInfo := range cInfos {
			err = m.deleteAssetRequest(cInfo.NodeID, cid)
			if err != nil {
				log.Errorf("deleteAssetRequest err: %s ", err.Error())
			}
		}
	}()

	return nil
}

// pullAssetsResult pull result
func (m *Manager) pullAssetsResult(nodeID string, result *types.CacheResult) {
	isCandidate := false
	pullingCount := 0

	nodeInfo := m.nodeMgr.GetNode(nodeID)
	if nodeInfo != nil {
		isCandidate = nodeInfo.NodeType == types.NodeCandidate
		// update node info
		nodeInfo.DiskUsage = result.DiskUsage
		defer nodeInfo.SetCurPullingCount(pullingCount)
	}

	for _, progress := range result.Progresses {
		log.Debugf("pullAssetsResult node_id: %s, status: %d, size: %d/%d, cid: %s ", nodeID, progress.Status, progress.DoneSize, progress.Size, progress.CID)

		hash, err := cidutil.CIDString2HashString(progress.CID)
		if err != nil {
			log.Errorf("%s cid to hash err:%s", progress.CID, err.Error())
			continue
		}

		{
			m.lock.Lock()
			tickerC, ok := m.apTickers[hash]
			if ok {
				tickerC.ticker.Reset(nodePullAssetTimeout)
			}
			m.lock.Unlock()
		}

		if progress.Status == types.ReplicaStatusWaiting {
			pullingCount++
			continue
		}

		// save replica info to db
		cInfo := &types.ReplicaInfo{
			Status:   progress.Status,
			DoneSize: progress.DoneSize,
			Hash:     hash,
			NodeID:   nodeID,
		}

		err = m.nodeMgr.UpdateUnfinishedReplica(cInfo)
		if err != nil {
			log.Errorf("pullAssetsResult %s UpdateReplicaInfo err:%s", nodeID, err.Error())
			continue
		}

		if progress.Status == types.ReplicaStatusPulling {
			pullingCount++

			err = m.assetStateMachines.Send(AssetHash(hash), InfoUpdate{
				ResultInfo: &NodePulledResult{
					BlocksCount: int64(progress.BlocksCount),
					Size:        progress.Size,
				},
			})
			if err != nil {
				log.Errorf("pullAssetsResult %s statemachine send err:%s", nodeID, err.Error())
			}

			continue
		}

		err = m.assetStateMachines.Send(AssetHash(hash), PulledResult{
			ResultInfo: &NodePulledResult{
				NodeID:      nodeID,
				Status:      int64(progress.Status),
				BlocksCount: int64(progress.BlocksCount),
				Size:        progress.Size,
				IsCandidate: isCandidate,
			},
		})
		if err != nil {
			log.Errorf("pullAssetsResult %s statemachine send err:%s", nodeID, err.Error())
			continue
		}
	}
}

func (m *Manager) addOrResetAssetTicker(hash string) {
	m.lock.Lock()
	defer m.lock.Unlock()

	fn := func() error {
		// update replicas status
		err := m.nodeMgr.UpdateStatusOfUnfinishedReplicas(hash, types.ReplicaStatusFailed)
		if err != nil {
			return xerrors.Errorf("addOrResetAssetTicker %s UpdateStatusOfUnfinishedReplicas err:%s", hash, err.Error())
		}

		err = m.assetStateMachines.Send(AssetHash(hash), PullFailed{error: xerrors.New("node pull asset response timeout")})
		if err != nil {
			return xerrors.Errorf("addOrResetAssetTicker %s send time out err:%s", hash, err.Error())
		}

		return nil
	}

	t, ok := m.apTickers[hash]
	if ok {
		t.ticker.Reset(nodePullAssetTimeout)
		return
	}

	m.apTickers[hash] = &assetTicker{
		ticker: time.NewTicker(nodePullAssetTimeout),
		close:  make(chan struct{}),
	}

	go m.apTickers[hash].run(fn)
}

func (m *Manager) removeAssetTicker(key string) {
	m.lock.Lock()
	defer m.lock.Unlock()

	t, ok := m.apTickers[key]
	if !ok {
		return
	}

	t.ticker.Stop()
	close(t.close)
	delete(m.apTickers, key)
}

// ResetAssetRecordExpiration reset the asset expiration
func (m *Manager) ResetAssetRecordExpiration(cid string, t time.Time) error {
	hash, err := cidutil.CIDString2HashString(cid)
	if err != nil {
		return err
	}

	log.Infof("asset event %s, reset asset expiration:%s", cid, t.String())

	err = m.nodeMgr.UpdateAssetRecordExpiration(hash, t)
	if err != nil {
		return err
	}

	m.resetLatestExpiration(t)

	return nil
}

// check assets expiration
func (m *Manager) checkAssetsExpiration() {
	if m.minExpiration.After(time.Now()) {
		return
	}

	records, err := m.nodeMgr.LoadExpiredAssetRecords()
	if err != nil {
		log.Errorf("LoadExpiredAssetRecords err:%s", err.Error())
		return
	}

	for _, record := range records {
		// do remove
		err = m.RemoveAsset(record.CID, record.Hash)
		log.Infof("the asset cid(%s) has expired, being removed, err: %v", record.CID, err)
	}

	// reset expiration
	expiration, err := m.nodeMgr.LoadMinExpirationOfAssetRecords()
	if err != nil {
		return
	}

	m.resetLatestExpiration(expiration)
}

func (m *Manager) resetLatestExpiration(t time.Time) {
	if m.minExpiration.After(t) {
		m.minExpiration = t
	}
}

// Notify node to delete asset
func (m *Manager) deleteAssetRequest(nodeID, cid string) error {
	edge := m.nodeMgr.GetEdgeNode(nodeID)
	if edge != nil {
		return edge.API().DeleteCarfile(context.Background(), cid)
	}

	candidate := m.nodeMgr.GetCandidateNode(nodeID)
	if candidate != nil {
		return candidate.API().DeleteCarfile(context.Background(), cid)
	}

	return nil
}

// GetCandidateReplicaCount get candidate replica count
func (m *Manager) GetCandidateReplicaCount() int {
	cfg, err := m.schedulerConfig()
	if err != nil {
		log.Errorf("schedulerConfig err:%s", err.Error())
		return 0
	}

	return cfg.CandidateReplicas
}

// GetAssetRecordInfo get asset record info of cid
func (m *Manager) GetAssetRecordInfo(cid string) (*types.AssetRecord, error) {
	hash, err := cidutil.CIDString2HashString(cid)
	if err != nil {
		return nil, err
	}

	dInfo, err := m.nodeMgr.LoadAssetRecord(hash)
	if err != nil {
		return nil, err
	}

	dInfo.ReplicaInfos, err = m.nodeMgr.LoadAssetReplicas(hash)
	if err != nil {
		log.Errorf("loadData hash:%s, LoadAssetReplicaInfos err:%s", hash, err.Error())
	}

	return dInfo, err
}

// Find edges that meet the full criteria
func (m *Manager) findEdges(count int, filterNodes []string) []*node.Edge {
	list := make([]*node.Edge, 0)

	if count <= 0 {
		return list
	}

	m.nodeMgr.EdgeNodes.Range(func(key, value interface{}) bool {
		edgeNode := value.(*node.Edge)

		for _, nodeID := range filterNodes {
			if nodeID == edgeNode.NodeID {
				return true
			}
		}

		if edgeNode.DiskUsage > maxNodeDiskUsage {
			return true
		}

		list = append(list, edgeNode)
		return true
	})

	sort.Slice(list, func(i, j int) bool {
		return list[i].CurPullingCount() < list[j].CurPullingCount()
	})

	if count > len(list) {
		count = len(list)
	}

	return list[:count]
}

// Find candidates that meet the pull criteria
func (m *Manager) findCandidates(count int, filterNodes []string) []*node.Candidate {
	list := make([]*node.Candidate, 0)

	if count <= 0 {
		return list
	}

	m.nodeMgr.CandidateNodes.Range(func(key, value interface{}) bool {
		candidateNode := value.(*node.Candidate)

		for _, nodeID := range filterNodes {
			if nodeID == candidateNode.NodeID {
				return true
			}
		}

		if candidateNode.DiskUsage > maxNodeDiskUsage {
			return true
		}

		list = append(list, candidateNode)
		return true
	})

	sort.Slice(list, func(i, j int) bool {
		return list[i].CurPullingCount() < list[j].CurPullingCount()
	})

	if count > len(list) {
		count = len(list)
	}

	return list[:count]
}

func (m *Manager) saveCandidateReplicaInfos(nodes []*node.Candidate, hash string) error {
	// save replica info
	replicaInfos := make([]*types.ReplicaInfo, 0)

	for _, node := range nodes {
		replicaInfos = append(replicaInfos, &types.ReplicaInfo{
			NodeID:      node.NodeID,
			Status:      types.ReplicaStatusWaiting,
			Hash:        hash,
			IsCandidate: true,
		})
	}

	return m.nodeMgr.BatchUpsertReplicas(replicaInfos)
}

func (m *Manager) saveEdgeReplicaInfos(nodes []*node.Edge, hash string) error {
	// save replica info
	replicaInfos := make([]*types.ReplicaInfo, 0)

	for _, node := range nodes {
		replicaInfos = append(replicaInfos, &types.ReplicaInfo{
			NodeID:      node.NodeID,
			Status:      types.ReplicaStatusWaiting,
			Hash:        hash,
			IsCandidate: false,
		})
	}

	return m.nodeMgr.BatchUpsertReplicas(replicaInfos)
}

// Sources get download sources
func (m *Manager) Sources(cid string, nodes []string) []*types.DownloadSource {
	titanRsa := titanrsa.New(crypto.SHA256, crypto.SHA256.New())
	sources := make([]*types.DownloadSource, 0)
	for _, nodeID := range nodes {
		cNode := m.nodeMgr.GetCandidateNode(nodeID)
		if cNode == nil {
			continue
		}

		credentials, err := cNode.Credentials(cid, titanRsa, m.nodeMgr.PrivateKey)
		if err != nil {
			continue
		}
		source := &types.DownloadSource{
			CandidateAddr: cNode.DownloadAddr(),
			Credentials:   credentials,
		}

		sources = append(sources, source)
	}

	return sources
}
