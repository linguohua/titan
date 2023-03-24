package caching

import (
	"context"
	"crypto"
	"database/sql"
	"fmt"
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

var log = logging.Logger("cache")

const (
	cacheTimeout                 = 30 * time.Second // Carfile cache timeout (Unit:Second)
	checkExpirationTimerInterval = 60 * 30          // Check for expired carfile record interval (Unit:Second)
	maxCachingCarfileCount       = 10               // Maximum number of carfile caches
	maxNodeDiskUsage             = 90.0             // If the node disk size is greater than this value, caching will not continue
	seedCacheCount               = 1                // The number of caches in the first stage
	getProgressInterval          = 10 * time.Second // Get cache progress interval from node (Unit:Second)
)

// Manager cache manager
type Manager struct {
	nodeManager          *node.Manager
	carfileMinExpiration time.Time

	startupWait sync.WaitGroup
	carfiles    *statemachine.StateGroup

	lock           sync.Mutex
	carfileTickers map[string]*carfileTicker

	getSchedulerConfigFunc dtypes.GetSchedulerConfigFunc
}

type carfileTicker struct {
	ticker *time.Ticker
	close  chan struct{}
}

func (t *carfileTicker) run(job func() error) {
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

// NewManager return new cache manager instance
func NewManager(nodeManager *node.Manager, ds datastore.Batching, configFunc dtypes.GetSchedulerConfigFunc) *Manager {
	m := &Manager{
		nodeManager:            nodeManager,
		carfileMinExpiration:   time.Now(),
		carfileTickers:         make(map[string]*carfileTicker),
		getSchedulerConfigFunc: configFunc,
	}

	m.startupWait.Add(1)
	m.carfiles = statemachine.New(ds, m, CarfileCacheInfo{})

	return m
}

// Run start carfile statemachine and start ticker
func (m *Manager) Run(ctx context.Context) {
	if err := m.restartCarfiles(ctx); err != nil {
		log.Errorf("failed load sector states: %+v", err)
	}
	go m.checkExpirationTicker(ctx)
	go m.cachedProgressTicker(ctx)
}

// Stop stop statemachine
func (m *Manager) Stop(ctx context.Context) error {
	if err := m.carfiles.Stop(ctx); err != nil {
		return err
	}
	return nil
}

func (m *Manager) checkExpirationTicker(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(checkExpirationTimerInterval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.checkCachesExpiration()
		case <-ctx.Done():
			return
		}
	}
}

func (m *Manager) cachedProgressTicker(ctx context.Context) {
	ticker := time.NewTicker(getProgressInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.nodesCacheProgresses()
		case <-ctx.Done():
			return
		}
	}
}

func (m *Manager) nodesCacheProgresses() {
	nodeCaches := make(map[string][]string)

	// caching carfiles
	for hash := range m.carfileTickers {
		cid, err := cidutil.HashString2CIDString(hash)
		if err != nil {
			log.Errorf("%s HashString2CIDString err:%s", hash, err.Error())
			continue
		}

		nodes, err := m.nodeManager.LoadCachingNodes(hash)
		if err != nil {
			log.Errorf("%s UnDoneNodes err:%s", hash, err.Error())
			continue
		}

		for _, nodeID := range nodes {
			list := nodeCaches[nodeID]
			if list == nil {
				list = make([]string, 0)
			}

			nodeCaches[nodeID] = append(list, cid)
		}
	}

	f := func(nodeID string, cids []string) {
		// request node
		result, err := m.nodeCachedProgresses(nodeID, cids)
		if err != nil {
			log.Errorf("%s nodeCachedProgresses err:%s", nodeID, err.Error())
			return
		}

		// update carfile info
		m.cacheCarfileResult(nodeID, result)
	}

	for nodeID, cids := range nodeCaches {
		go f(nodeID, cids)
	}
}

func (m *Manager) nodeCachedProgresses(nodeID string, carfileCIDs []string) (result *types.CacheResult, err error) {
	log.Debugf("nodeID:%s, %v", nodeID, carfileCIDs)

	cNode := m.nodeManager.GetCandidateNode(nodeID)
	if cNode != nil {
		result, err = cNode.API().CachedProgresses(context.Background(), carfileCIDs)
	} else {
		eNode := m.nodeManager.GetEdgeNode(nodeID)
		if eNode != nil {
			result, err = eNode.API().CachedProgresses(context.Background(), carfileCIDs)
		} else {
			err = xerrors.Errorf("node %s offline", nodeID)
		}
	}

	if err != nil {
		return
	}

	return
}

// CacheCarfile create a new cache carfile task
func (m *Manager) CacheCarfile(info *types.CacheCarfileInfo) error {
	m.startupWait.Wait()

	if len(m.carfileTickers) >= maxCachingCarfileCount {
		return xerrors.Errorf("The carfile in the cache exceeds the limit %d, please wait", maxCachingCarfileCount)
	}

	log.Debugf("carfile event: %s, add carfile replica: %d,expiration: %s", info.CarfileCid, info.Replicas, info.Expiration.String())

	cInfo, err := m.nodeManager.LoadCarfileRecordInfo(info.CarfileHash)
	if err != nil && err != sql.ErrNoRows {
		return err
	}

	if cInfo.CarfileHash == "" {
		// create carfile task
		return m.carfiles.Send(CarfileHash(info.CarfileHash), CarfileStartCaches{
			ID:                          info.CarfileCid,
			CarfileHash:                 CarfileHash(info.CarfileHash),
			Replicas:                    info.Replicas,
			ServerID:                    info.ServerID,
			CreatedAt:                   time.Now().Unix(),
			Expiration:                  info.Expiration.Unix(),
			CandidateReplicaCachesCount: m.GetCandidateReplicaCount(),
		})
	}

	return xerrors.New("carfile exists")
}

// FailedCarfilesRestart restart failed carfiles
func (m *Manager) FailedCarfilesRestart(hashes []types.CarfileHash) error {
	for _, hash := range hashes {
		err := m.carfiles.Send(hash, CarfileRestart{})
		if err != nil {
			log.Errorf("FailedCarfilesRestart send err:%s", err.Error())
		}
	}

	return nil
}

// RemoveCarfileRecord remove a carfile record
func (m *Manager) RemoveCarfileRecord(carfileCid, hash string) error {
	cInfos, err := m.nodeManager.LoadReplicaInfosOfCarfile(hash, false)
	if err != nil {
		return xerrors.Errorf("GetCarfileReplicaInfosByHash: %s,err:%s", carfileCid, err.Error())
	}

	defer func() {
		err = m.nodeManager.RemoveCarfileRecord(hash)
		if err != nil {
			log.Errorf("%s RemoveCarfileRecord db err: %s", hash, err.Error())
		}
	}()

	// remove carfile
	err = m.carfiles.Send(CarfileHash(hash), CarfileRemove{})
	if err != nil {
		return xerrors.Errorf("RemoveCarfileRecord send to state machine err: %s ", err.Error())
	}

	log.Infof("carfile event %s , remove carfile record", carfileCid)

	go func() {
		for _, cInfo := range cInfos {
			err = m.sendCacheRequest(cInfo.NodeID, carfileCid)
			if err != nil {
				log.Errorf("sendCacheRequest err: %s ", err.Error())
			}
		}
	}()

	return nil
}

// cacheCarfileResult block cache result
func (m *Manager) cacheCarfileResult(nodeID string, result *types.CacheResult) {
	isCandidate := false
	cacheCount := 0

	nodeInfo := m.nodeManager.GetNode(nodeID)
	if nodeInfo != nil {
		isCandidate = nodeInfo.NodeType == types.NodeCandidate
		// update node info
		nodeInfo.DiskUsage = result.DiskUsage
		defer nodeInfo.SetCurCacheCount(cacheCount)
	}

	for _, progress := range result.Progresses {
		log.Debugf("CacheCarfileResult node_id: %s, status: %d, size: %d/%d, cid: %s ", nodeID, progress.Status, progress.DoneSize, progress.CarfileSize, progress.CarfileCid)

		hash, err := cidutil.CIDString2HashString(progress.CarfileCid)
		if err != nil {
			log.Errorf("%s cid to hash err:%s", progress.CarfileCid, err.Error())
			continue
		}

		{
			m.lock.Lock()
			tickerC, ok := m.carfileTickers[hash]
			if ok {
				tickerC.ticker.Reset(cacheTimeout)
			}
			m.lock.Unlock()
		}

		if progress.Status == types.CacheStatusWaiting {
			cacheCount++
			continue
		}

		// save to db
		cInfo := &types.ReplicaInfo{
			ID:       replicaID(hash, nodeID),
			Status:   progress.Status,
			DoneSize: progress.DoneSize,
		}

		err = m.nodeManager.UpdateUnfinishedReplicaInfo(cInfo)
		if err != nil {
			log.Errorf("CacheCarfileResult %s UpdateReplicaInfo err:%s", nodeID, err.Error())
			continue
		}

		if progress.Status == types.CacheStatusCaching {
			cacheCount++

			err = m.carfiles.Send(CarfileHash(hash), CarfileInfoUpdate{
				ResultInfo: &NodeCacheResultInfo{
					CarfileBlocksCount: int64(progress.CarfileBlocksCount),
					CarfileSize:        progress.CarfileSize,
				},
			})
			if err != nil {
				log.Errorf("CacheCarfileResult %s statemachine send err:%s", nodeID, err.Error())
			}

			continue
		}

		err = m.carfiles.Send(CarfileHash(hash), CacheResult{
			ResultInfo: &NodeCacheResultInfo{
				NodeID:             nodeID,
				Status:             int64(progress.Status),
				CarfileBlocksCount: int64(progress.CarfileBlocksCount),
				CarfileSize:        progress.CarfileSize,
				IsCandidate:        isCandidate,
			},
		})
		if err != nil {
			log.Errorf("CacheCarfileResult %s statemachine send err:%s", nodeID, err.Error())
			continue
		}
	}
}

func (m *Manager) addOrResetCarfileTicker(hash string) {
	m.lock.Lock()
	defer m.lock.Unlock()

	fn := func() error {
		// update replicas status
		err := m.nodeManager.UpdateStatusOfUnfinishedReplicas(hash, types.CacheStatusFailed)
		if err != nil {
			return xerrors.Errorf("carfileHash %s SetCarfileReplicasTimeout err:%s", hash, err.Error())
		}

		err = m.carfiles.Send(CarfileHash(hash), CacheFailed{error: xerrors.New("waiting cache response timeout")})
		if err != nil {
			return xerrors.Errorf("carfileHash %s send time out err:%s", hash, err.Error())
		}

		return nil
	}

	t, ok := m.carfileTickers[hash]
	if ok {
		t.ticker.Reset(cacheTimeout)
		return
	}

	m.carfileTickers[hash] = &carfileTicker{
		ticker: time.NewTicker(cacheTimeout),
		close:  make(chan struct{}),
	}

	go m.carfileTickers[hash].run(fn)
}

func (m *Manager) removeCarfileTicker(key string) {
	m.lock.Lock()
	defer m.lock.Unlock()

	t, ok := m.carfileTickers[key]
	if !ok {
		return
	}

	t.ticker.Stop()
	close(t.close)
	delete(m.carfileTickers, key)
}

// ResetCarfileExpiration reset the carfile expiration
func (m *Manager) ResetCarfileExpiration(cid string, t time.Time) error {
	hash, err := cidutil.CIDString2HashString(cid)
	if err != nil {
		return err
	}

	log.Infof("carfile event %s, reset carfile expiration:%s", cid, t.String())

	err = m.nodeManager.UpdateCarfileRecordExpiration(hash, t)
	if err != nil {
		return err
	}

	m.resetLatestExpiration(t)

	return nil
}

// check caches expiration
func (m *Manager) checkCachesExpiration() {
	if m.carfileMinExpiration.After(time.Now()) {
		return
	}

	carfileRecords, err := m.nodeManager.LoadExpiredCarfileRecords()
	if err != nil {
		log.Errorf("ExpiredCarfiles err:%s", err.Error())
		return
	}

	for _, carfileRecord := range carfileRecords {
		// do remove
		err = m.RemoveCarfileRecord(carfileRecord.CarfileCID, carfileRecord.CarfileHash)
		log.Infof("the carfile cid(%s) has expired, being removed, err: %v", carfileRecord.CarfileCID, err)
	}

	// reset expiration
	latestExpiration, err := m.nodeManager.LoadMinExpirationOfCarfileRecords()
	if err != nil {
		return
	}

	m.resetLatestExpiration(latestExpiration)
}

func (m *Manager) resetLatestExpiration(t time.Time) {
	if m.carfileMinExpiration.After(t) {
		m.carfileMinExpiration = t
	}
}

// Notify node to delete carfile
func (m *Manager) sendCacheRequest(nodeID, cid string) error {
	edge := m.nodeManager.GetEdgeNode(nodeID)
	if edge != nil {
		return edge.API().DeleteCarfile(context.Background(), cid)
	}

	candidate := m.nodeManager.GetCandidateNode(nodeID)
	if candidate != nil {
		return candidate.API().DeleteCarfile(context.Background(), cid)
	}

	return nil
}

// GetCandidateReplicaCount get candidate replica count
func (m *Manager) GetCandidateReplicaCount() int {
	cfg, err := m.getSchedulerConfigFunc()
	if err != nil {
		log.Errorf("getSchedulerConfigFunc err:%s", err.Error())
		return 0
	}

	return cfg.CandidateReplicaCachesCount
}

func replicaID(hash, nodeID string) string {
	return fmt.Sprintf("%s_%s", hash, nodeID)
}

// GetCarfileRecordInfo get carfile record info of cid
func (m *Manager) GetCarfileRecordInfo(cid string) (*types.CarfileRecordInfo, error) {
	hash, err := cidutil.CIDString2HashString(cid)
	if err != nil {
		return nil, err
	}

	dInfo, err := m.nodeManager.LoadCarfileRecordInfo(hash)
	if err != nil {
		return nil, err
	}

	dInfo.ReplicaInfos, err = m.nodeManager.LoadReplicaInfosOfCarfile(hash, false)
	if err != nil {
		log.Errorf("loadData hash:%s, GetCarfileReplicaInfosByHash err:%s", hash, err.Error())
	}

	return dInfo, err
}

// Find edges that meet the cache criteria
func (m *Manager) findEdges(count int, filterNodes []string) []*node.Edge {
	list := make([]*node.Edge, 0)

	if count <= 0 {
		return list
	}

	m.nodeManager.EdgeNodes.Range(func(key, value interface{}) bool {
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
		return list[i].CurCacheCount() < list[j].CurCacheCount()
	})

	if count > len(list) {
		count = len(list)
	}

	return list[:count]
}

// Find candidates that meet the cache criteria
func (m *Manager) findCandidates(count int, filterNodes []string) []*node.Candidate {
	list := make([]*node.Candidate, 0)

	if count <= 0 {
		return list
	}

	m.nodeManager.CandidateNodes.Range(func(key, value interface{}) bool {
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
		return list[i].CurCacheCount() < list[j].CurCacheCount()
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
			ID:          replicaID(hash, node.NodeID),
			NodeID:      node.NodeID,
			Status:      types.CacheStatusWaiting,
			CarfileHash: hash,
			IsCandidate: true,
		})
	}

	return m.nodeManager.BatchUpsertReplicas(replicaInfos)
}

func (m *Manager) saveEdgeReplicaInfos(nodes []*node.Edge, hash string) error {
	// save replica info
	replicaInfos := make([]*types.ReplicaInfo, 0)

	for _, node := range nodes {
		replicaInfos = append(replicaInfos, &types.ReplicaInfo{
			ID:          replicaID(hash, node.NodeID),
			NodeID:      node.NodeID,
			Status:      types.CacheStatusWaiting,
			CarfileHash: hash,
			IsCandidate: false,
		})
	}

	return m.nodeManager.BatchUpsertReplicas(replicaInfos)
}

// Sources get download sources
func (m *Manager) Sources(cid string, nodes []string) []*types.DownloadSource {
	titanRsa := titanrsa.New(crypto.SHA256, crypto.SHA256.New())
	sources := make([]*types.DownloadSource, 0)
	for _, nodeID := range nodes {
		cNode := m.nodeManager.GetCandidateNode(nodeID)
		if cNode == nil {
			continue
		}

		credentials, err := cNode.Credentials(cid, titanRsa, m.nodeManager.PrivateKey)
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
