package storage

import (
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
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
	"github.com/linguohua/titan/node/scheduler/node"
	"golang.org/x/xerrors"
)

var log = logging.Logger("storage")

const (
	cachingTimeout               = 30 * time.Second // node caching keepalive (Unit:Second)
	checkExpirationTimerInterval = 60 * 30          // time interval (Unit:Second)
	downloadingCarfileMaxCount   = 10               // It needs to be changed to the number of caches
	maxDiskUsage                 = 90.0             // If the node disk size is greater than this value, caching will not continue
	seedCacheCount               = 1                // The number of caches in the first stage

	cachedProgressTimerInterval = 10 * time.Second // time interval (Unit:Second)
)

// Manager storage
type Manager struct {
	nodeManager      *node.Manager
	latestExpiration time.Time
	writeToken       string

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

// NewManager return new storage manager instance
func NewManager(nodeManager *node.Manager, writeToken dtypes.PermissionWriteToken, ds datastore.Batching, configFunc dtypes.GetSchedulerConfigFunc) *Manager {
	m := &Manager{
		nodeManager:            nodeManager,
		latestExpiration:       time.Now(),
		writeToken:             string(writeToken),
		carfileTickers:         make(map[string]*carfileTicker),
		getSchedulerConfigFunc: configFunc,
	}

	m.startupWait.Add(1)
	m.carfiles = statemachine.New(ds, m, CarfileInfo{})

	return m
}

func (m *Manager) Run(ctx context.Context) {
	if err := m.restartCarfiles(ctx); err != nil {
		log.Errorf("failed load sector states: %+v", err)
	}
	go m.checkExpirationTicker(ctx)
	go m.cachedProgressTicker(ctx)
}

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
	ticker := time.NewTicker(cachedProgressTimerInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.nodeCacheProgresses()
		case <-ctx.Done():
			return
		}
	}
}

func (m *Manager) nodeCacheProgresses() {
	nodeCaches := make(map[string][]string)

	for hash := range m.carfileTickers {
		cid, err := cidutil.HashString2CIDString(hash)
		if err != nil {
			log.Errorf("HashString2CIDString err:%s", err.Error())
			continue
		}

		nodes, err := m.nodeManager.CarfileDB.UnDoneNodes(hash)
		if err != nil {
			log.Errorf("UnDoneNodes err:%s", err.Error())
			continue
		}

		for _, node := range nodes {
			list := nodeCaches[node]
			if list == nil {
				list = make([]string, 0)
			}

			nodeCaches[node] = append(list, cid)
		}
	}

	// request node
	for node, cids := range nodeCaches {
		err := m.nodeCachedProgresses(node, cids)
		if err != nil {
			log.Errorf("cachedProgresses err:%s", err.Error())
		}
	}
}

func (m *Manager) nodeCachedProgresses(nodeID string, carfileCIDs []string) error {
	log.Warnf("nodeID:%s, %v", nodeID, carfileCIDs)
	var progresses []*types.CarfileProgress
	var err error
	isCandidate := false

	cNode := m.nodeManager.GetCandidateNode(nodeID)
	if cNode != nil {
		progresses, err = cNode.API().CachedProgresses(context.Background(), carfileCIDs)
		isCandidate = true
	} else {
		eNode := m.nodeManager.GetEdgeNode(nodeID)
		if eNode != nil {
			progresses, err = eNode.API().CachedProgresses(context.Background(), carfileCIDs)
		} else {
			err = xerrors.Errorf("node %s offline", nodeID)
		}
	}

	if err != nil {
		return err
	}

	return m.cacheCarfileResult(nodeID, progresses, isCandidate)
}

// CacheCarfile create a new carfile storing task
func (m *Manager) CacheCarfile(info *types.CacheCarfileInfo) error {
	log.Debugf("carfile event: %s, add carfile replica: %d,expiration: %s", info.CarfileCid, info.Replicas, info.Expiration.String())

	cInfo, err := m.nodeManager.CarfileDB.CarfileInfo(info.CarfileHash)
	if err != nil && err != sql.ErrNoRows {
		return err
	}

	if cInfo == nil {
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
		m.carfiles.Send(hash, CarfileRestart{})
	}

	return nil
}

// RemoveCarfileRecord remove a storage
func (m *Manager) RemoveCarfileRecord(carfileCid, hash string) error {
	cInfos, err := m.nodeManager.CarfileDB.ReplicaInfosByCarfile(hash, false)
	if err != nil {
		return xerrors.Errorf("GetCarfileReplicaInfosByHash: %s,err:%s", carfileCid, err.Error())
	}

	defer func() {
		err = m.nodeManager.CarfileDB.RemoveCarfileRecord(hash)
		if err != nil {
			log.Errorf("%s RemoveCarfileRecord db err: %s", hash, err.Error())
		}
	}()

	// remove carfile
	err = m.carfiles.Send(CarfileHash(hash), CarfileRemove{})
	if err != nil {
		return xerrors.Errorf("RemoveCarfileRecord send to state machine err: %s ", err.Error())
	}

	log.Infof("storage event %s , remove storage record", carfileCid)

	for _, cInfo := range cInfos {
		go m.sendCacheRequest(cInfo.NodeID, carfileCid)
	}

	return nil
}

// cacheCarfileResult block cache result
func (m *Manager) cacheCarfileResult(nodeID string, progresses []*types.CarfileProgress, isCandidate bool) (err error) {
	for _, progress := range progresses {
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
				tickerC.ticker.Reset(cachingTimeout)
			}
			m.lock.Unlock()
		}

		if progress.Status == types.CacheStatusWaiting {
			continue
		}

		// save to db
		cInfo := &types.ReplicaInfo{
			ID:       replicaID(hash, nodeID),
			Status:   progress.Status,
			DoneSize: int64(progress.DoneSize),
		}

		err = m.nodeManager.CarfileDB.UpdateReplicaInfo(cInfo)
		if err != nil {
			log.Errorf("CacheCarfileResult %s UpdateReplicaInfo err:%s", nodeID, err.Error())
			continue
		}

		if progress.Status == types.CacheStatusDownloading {
			err = m.carfiles.Send(CarfileHash(hash), CarfileInfoUpdate{
				ResultInfo: &CacheResultInfo{
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
			ResultInfo: &CacheResultInfo{
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

	return nil
}

func (m *Manager) addOrResetCarfileTicker(hash string) {
	m.lock.Lock()
	defer m.lock.Unlock()

	fn := func() error {
		// update replicas status
		err := m.nodeManager.CarfileDB.SetCarfileReplicasTimeout(hash)
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
		t.ticker.Reset(cachingTimeout)
		return
	}

	m.carfileTickers[hash] = &carfileTicker{
		ticker: time.NewTicker(cachingTimeout),
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

	log.Infof("storage event %s, reset storage expiration:%s", cid, t.String())

	err = m.nodeManager.CarfileDB.ResetCarfileExpiration(hash, t)
	if err != nil {
		return err
	}

	m.resetLatestExpiration(t)

	return nil
}

// check caches expiration
func (m *Manager) checkCachesExpiration() {
	if m.latestExpiration.After(time.Now()) {
		return
	}

	carfileRecords, err := m.nodeManager.CarfileDB.ExpiredCarfiles()
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
	latestExpiration, err := m.nodeManager.CarfileDB.MinExpiration()
	if err != nil {
		return
	}

	m.resetLatestExpiration(latestExpiration)
}

func (m *Manager) resetLatestExpiration(t time.Time) {
	if m.latestExpiration.After(t) {
		m.latestExpiration = t
	}
}

// Notify node to delete all carfiles
func (m *Manager) sendRemoveRequest(nodeID string) error {
	edge := m.nodeManager.GetEdgeNode(nodeID)
	if edge != nil {
		return edge.API().DeleteAllCarfiles(context.Background())
	}

	candidate := m.nodeManager.GetCandidateNode(nodeID)
	if candidate != nil {
		return candidate.API().DeleteAllCarfiles(context.Background())
	}

	return nil
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
	input := fmt.Sprintf("%s%s", hash, nodeID)

	c := sha1.New()
	c.Write([]byte(input))
	bytes := c.Sum(nil)
	return hex.EncodeToString(bytes)
}

// GetCarfileRecordInfo get storage record info of cid
func (m *Manager) GetCarfileRecordInfo(cid string) (*types.CarfileRecordInfo, error) {
	hash, err := cidutil.CIDString2HashString(cid)
	if err != nil {
		return nil, err
	}

	dInfo, err := m.nodeManager.CarfileDB.CarfileInfo(hash)
	if err != nil {
		return nil, err
	}

	dInfo.ReplicaInfos, err = m.nodeManager.CarfileDB.ReplicaInfosByCarfile(hash, false)
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

		if filterNodes != nil {
			for _, nodeID := range filterNodes {
				if nodeID == edgeNode.NodeID {
					return true
				}
			}
		}

		if edgeNode.DiskUsage > maxDiskUsage {
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

		if filterNodes != nil {
			for _, nodeID := range filterNodes {
				if nodeID == candidateNode.NodeID {
					return true
				}
			}
		}

		if candidateNode.DiskUsage > maxDiskUsage {
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

	return m.nodeManager.CarfileDB.InsertOrUpdateReplicaInfo(replicaInfos)
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

	return m.nodeManager.CarfileDB.InsertOrUpdateReplicaInfo(replicaInfos)
}

// Sources get download sources
func (m *Manager) Sources(hash string, nodes []string) []*types.DownloadSource {
	sources := make([]*types.DownloadSource, 0)

	for _, nodeID := range nodes {
		cNode := m.nodeManager.GetCandidateNode(nodeID)
		if cNode != nil {
			source := &types.DownloadSource{
				CandidateURL:   cNode.RPCURL(),
				CandidateToken: m.writeToken,
			}

			sources = append(sources, source)
		}
	}

	return sources
}
