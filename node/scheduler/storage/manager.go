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
	nodeCachingKeepalive         = 30      // node caching keepalive (Unit:Second)
	checkExpirationTimerInterval = 60 * 30 // time interval (Unit:Second)
	downloadingCarfileMaxCount   = 10      // It needs to be changed to the number of caches
	maxDiskUsage                 = 90.0    // If the node disk size is greater than this value, caching will not continue
	rootCachesCount              = 1       // The number of caches in the first stage
)

var candidateReplicaCachesCount = 0 // nodeMgrCache to the number of candidate nodes （does not contain 'rootCachesCount'）

// CacheEvent carfile cache event
type CacheEvent int

const (
	// EventStop stop
	EventStop CacheEvent = iota
	// EventReset status
	EventReset
)

// Manager storage
type Manager struct {
	nodeManager          *node.Manager
	latelyExpirationTime time.Time
	writeToken           []byte
	downloadingTaskCount int

	startupWait sync.WaitGroup
	carfiles    *statemachine.StateGroup

	carfileTickers map[string]chan CacheEvent
	lock           sync.Mutex
}

// NewManager return new storage manager instance
func NewManager(nodeManager *node.Manager, writeToken dtypes.PermissionWriteToken, ds datastore.Batching) *Manager {
	m := &Manager{
		nodeManager:          nodeManager,
		latelyExpirationTime: time.Now(),
		writeToken:           writeToken,
		carfileTickers:       map[string]chan CacheEvent{},
	}

	m.startupWait.Add(1)
	m.carfiles = statemachine.New(ds, m, CarfileInfo{})

	// m.initCarfileMap()
	// go m.carfileTaskTicker()
	go m.checkExpirationTicker()

	return m
}

func (m *Manager) Run(ctx context.Context) {
	if err := m.restartCarfiles(ctx); err != nil {
		log.Errorf("failed load sector states: %+v", err)
	}
}

func (m *Manager) Stop(ctx context.Context) error {
	if err := m.carfiles.Stop(ctx); err != nil {
		return err
	}
	return nil
}

func (m *Manager) checkExpirationTicker() {
	ticker := time.NewTicker(time.Duration(checkExpirationTimerInterval) * time.Second)
	defer ticker.Stop()

	for {
		<-ticker.C
		m.checkCachesExpiration()
	}
}

// CacheCarfile create a new carfile storing task
func (m *Manager) CacheCarfile(info *types.CacheCarfileInfo) error {
	log.Infof("carfile event %s , add carfile,replica:%d,expiration:%s", info.CarfileCid, info.Replicas, info.Expiration.String())

	cInfo, err := m.nodeManager.CarfileDB.CarfileInfo(info.CarfileHash)
	if err != nil && err != sql.ErrNoRows {
		return err
	}

	if cInfo == nil {
		// create carfile task
		return m.carfiles.Send(CarfileHash(info.CarfileHash), CarfileStartCaches{
			ID:          info.CarfileCid,
			CarfileHash: CarfileHash(info.CarfileHash),
			Replicas:    info.Replicas,
			ServerID:    info.ServerID,
			CreatedAt:   time.Now().Unix(),
			Expiration:  info.Expiration.Unix(),
		})
	}

	return xerrors.New("carfile is exist")
}

// FailedCarfilesRestart restart failed carfiles
func (m *Manager) FailedCarfilesRestart() error {
	list, err := m.ListCarfiles()
	if err != nil {
		return err
	}

	for _, c := range list {
		if c.State == CacheSeedFailed || c.State == CacheCandidatesFailed || c.State == CacheEdgesFailed {
			return m.carfiles.Send(c.CarfileHash, CarfileRestart{})
		}
	}

	return nil
}

// RemoveCarfileRecord remove a storage
func (m *Manager) RemoveCarfileRecord(carfileCid, hash string) error {
	cInfos, err := m.nodeManager.CarfileDB.ReplicaInfosByCarfile(hash, false)
	if err != nil {
		return xerrors.Errorf("GetCarfileReplicaInfosByHash: %s,err:%s", carfileCid, err.Error())
	}

	// remove carfile
	err = m.carfiles.Send(CarfileHash(hash), CarfileRemove{})
	if err != nil {
		return xerrors.Errorf("RemoveCarfileRecord send to statemachine err:%s ", err.Error())
	}

	err = m.nodeManager.CarfileDB.RemoveCarfileRecord(hash)
	if err != nil {
		return xerrors.Errorf("RemoveCarfileRecord db err:%s ", err.Error())
	}

	log.Infof("storage event %s , remove storage record", carfileCid)

	for _, cInfo := range cInfos {
		go m.sendCacheRequest(cInfo.NodeID, carfileCid)
	}

	return nil
}

// CacheCarfileResult block cache result
func (m *Manager) CacheCarfileResult(nodeID string, info *types.CacheResult) (err error) {
	log.Debugf("carfileCacheResult :%s , %d , %s", nodeID, info.Status, info.CarfileHash)

	m.lock.Lock()
	defer m.lock.Unlock()

	tickerC := m.carfileTickers[info.CarfileHash]
	if tickerC != nil {
		tickerC <- EventReset
	}

	if info.Status == types.CacheStatusDownloading {
		return nil
	}

	t, err := m.nodeManager.NodeMgrDB.NodeType(nodeID)
	if err != nil {
		return err
	}

	// save to db
	cInfo := &types.ReplicaInfo{
		ID:     replicaID(info.CarfileHash, nodeID),
		Status: info.Status,
	}

	err = m.nodeManager.CarfileDB.UpdateReplicaInfo(cInfo)
	if err != nil {
		return err
	}

	return m.carfiles.Send(CarfileHash(info.CarfileHash), CacheResult{
		ResultInfo: &CacheResultInfo{
			NodeID:             nodeID,
			Status:             int64(info.Status),
			CarfileBlocksCount: int64(info.CarfileBlocksCount),
			CarfileSize:        info.CarfileSize,
			IsCandidate:        t == types.NodeCandidate,
		},
	})
}

func (m *Manager) resetTimeoutTimer(carfileHash string) {
	m.lock.Lock()
	defer m.lock.Unlock()

	tickerC := m.carfileTickers[carfileHash]
	if tickerC != nil {
		tickerC <- EventReset
	} else {
		m.carfileTickers[carfileHash] = m.startTicker(carfileHash)
	}
}

func (m *Manager) startTicker(carfileHash string) chan CacheEvent {
	ticker := time.NewTicker(time.Duration(nodeCachingKeepalive) * time.Second)

	tChan := make(chan CacheEvent)
	go func(ticker *time.Ticker) {
		defer func() {
			ticker.Stop()
			close(tChan)

			m.lock.Lock()
			defer m.lock.Unlock()

			delete(m.carfileTickers, carfileHash)
		}()

		for {
			select {
			case <-ticker.C:
				err := m.nodeManager.CarfileDB.SetReplicasFailed(carfileHash)
				if err != nil {
					log.Errorf("carfileHash %s SetReplicasFailed err:%s", carfileHash, err.Error())
					break
				}

				err = m.carfiles.Send(CarfileHash(carfileHash), CacheFailed{error: xerrors.New("waiting cache response timeout")})
				if err != nil {
					log.Errorf("carfileHash %s send time out err:%s", carfileHash, err.Error())
				}
				return
			case event := <-tChan:
				if event == EventStop {
					return
				}
				if event == EventReset {
					ticker.Reset(time.Duration(nodeCachingKeepalive) * time.Second)
				}
			}
		}
	}(ticker)

	return tChan
}

func (m *Manager) stopTimeoutTimer(carfileHash string) {
	m.lock.Lock()
	defer m.lock.Unlock()

	tickerC := m.carfileTickers[carfileHash]
	if tickerC != nil {
		tickerC <- EventStop
	}
}

// ResetCarfileExpiration reset the carfile expiration
func (m *Manager) ResetCarfileExpiration(cid string, t time.Time) error {
	hash, err := cidutil.CIDString2HashString(cid)
	if err != nil {
		return err
	}

	log.Infof("storage event %s , reset storage expiration time:%s", cid, t.String())

	err = m.nodeManager.CarfileDB.ResetCarfileExpiration(hash, t)
	if err != nil {
		return err
	}

	m.resetLatelyExpirationTime(t)

	return nil
}

// check caches expiration
func (m *Manager) checkCachesExpiration() {
	if m.latelyExpirationTime.After(time.Now()) {
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
		log.Infof("cid:%s, expired,remove it ; %v", carfileRecord.CarfileCID, err)
	}

	// reset expiration time
	latelyExpirationTime, err := m.nodeManager.CarfileDB.MinExpiration()
	if err != nil {
		return
	}

	m.resetLatelyExpirationTime(latelyExpirationTime)
}

func (m *Manager) resetLatelyExpirationTime(t time.Time) {
	if m.latelyExpirationTime.After(t) {
		m.latelyExpirationTime = t
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

// ResetReplicaCount reset candidate replica count
func (m *Manager) ResetReplicaCount(count int) {
	candidateReplicaCachesCount = count
}

// GetCandidateReplicaCount get candidate replica count
func (m *Manager) GetCandidateReplicaCount() int {
	return candidateReplicaCachesCount
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

func (m *Manager) CarfileStatus(ctx context.Context, hash types.CarfileHash) (types.CarfileRecordInfo, error) {
	info, err := m.GetCarfileFromStatemachine(CarfileHash(hash))
	if err != nil {
		return types.CarfileRecordInfo{}, err
	}

	cInfo := types.CarfileRecordInfo{
		CarfileCID:            hash.String(),
		State:                 info.State.String(),
		CarfileHash:           info.CarfileHash.String(),
		NeedEdgeReplica:       info.EdgeReplicas,
		NeedCandidateReplicas: info.CandidateReplicas,
		TotalSize:             info.Size,
		TotalBlocks:           info.Blocks,
		Expiration:            time.Unix(info.Expiration, 0),
	}

	return cInfo, nil
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
			Status:      types.CacheStatusDownloading,
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
			Status:      types.CacheStatusDownloading,
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
				CandidateToken: string(m.writeToken),
			}

			sources = append(sources, source)
		}
	}

	return sources
}
