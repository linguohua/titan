package storage

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/filecoin-project/go-statemachine"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	"github.com/linguohua/titan/api/types"

	"github.com/linguohua/titan/node/modules/dtypes"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/node/cidutil"
	"github.com/linguohua/titan/node/scheduler/node"
	"golang.org/x/xerrors"
)

const CarfileStorePrefix = "/carfile"

var log = logging.Logger("storage")

const (
	nodoCachingKeepalive         = 60      // node caching keepalive (Unit:Second)
	nodeCacheTimeoutTime         = 65      // expiration set to redis (Unit:Second)
	startTaskInterval            = 10      //  time interval (Unit:Second)
	checkExpirationTimerInterval = 60 * 30 //  time interval (Unit:Second)
	downloadingCarfileMaxCount   = 10      // It needs to be changed to the number of caches
	diskUsageMax                 = 90.0    // If the node disk size is greater than this value, caching will not continue
	rootCacheCount               = 1       // The number of caches in the first stage
)

var candidateReplicaCacheCount = 0 // nodeMgrCache to the number of candidate nodes （does not contain 'rootCacheCount'）

// Manager storage
type Manager struct {
	nodeManager               *node.Manager
	DownloadingCarfileRecords sync.Map // caching storage map
	latelyExpirationTime      time.Time
	writeToken                []byte
	downloadingTaskCount      int

	startupWait sync.WaitGroup
	carfiles    *statemachine.StateGroup
}

// NewManager return new storage manager instance
func NewManager(nodeManager *node.Manager, writeToken dtypes.PermissionWriteToken, ds datastore.Batching) *Manager {
	m := &Manager{
		nodeManager:          nodeManager,
		latelyExpirationTime: time.Now(),
		writeToken:           writeToken,
	}

	m.startupWait.Add(1)
	m.carfiles = statemachine.New(namespace.Wrap(ds, datastore.NewKey(CarfileStorePrefix)), m, CarfileInfo{})

	m.initCarfileMap()
	go m.carfileTaskTicker()
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

func (m *Manager) initCarfileMap() {
	hashes, err := m.nodeManager.CarfileDB.GetCachingCarfiles(m.nodeManager.ServerID)
	if err != nil {
		log.Errorf("initCacheMap GetCachingCarfiles err:%s", err.Error())
		return
	}

	for _, hash := range hashes {
		cr, err := m.loadCarfileRecord(hash, m)
		if err != nil {
			log.Errorf("initCacheMap loadCarfileRecord hash:%s , err:%s", hash, err.Error())
			continue
		}
		cr.initStep()

		m.carfileCacheStart(cr)

		isDownloading := false
		// start timout check
		cr.Replicas.Range(func(key, value interface{}) bool {
			ra := value.(*Replica)
			if ra.status != types.CacheStatusDownloading {
				return true
			}

			ra.countDown = nodeCacheTimeoutTime
			isDownloading = true
			go ra.startTimeoutTimer()

			return true
		})

		if !isDownloading {
			m.carfileCacheEnd(cr, nil)
		}
	}
}

func (m *Manager) carfileTaskTicker() {
	ticker := time.NewTicker(time.Duration(startTaskInterval) * time.Second)
	defer ticker.Stop()

	for {
		<-ticker.C
		m.startCarfileReplicaTasks()
	}
}

func (m *Manager) checkExpirationTicker() {
	ticker := time.NewTicker(time.Duration(checkExpirationTimerInterval) * time.Second)
	defer ticker.Stop()

	for {
		<-ticker.C
		m.checkCachesExpiration()
	}
}

// GetCarfileRecord get a carfileRecord from map or db
func (m *Manager) GetCarfileRecord(hash string) (*CarfileRecord, error) {
	dI, exist := m.DownloadingCarfileRecords.Load(hash)
	if exist && dI != nil {
		return dI.(*CarfileRecord), nil
	}

	return m.loadCarfileRecord(hash, m)
}

func (m *Manager) cacheCarfileToNode(info *types.CacheCarfileInfo) error {
	carfileRecord, err := m.loadCarfileRecord(info.CarfileHash, m)
	if err != nil {
		return err
	}

	// only execute once
	carfileRecord.step = cachedStep

	m.carfileCacheStart(carfileRecord)

	err = carfileRecord.dispatchCache(info.NodeID)
	if err != nil {
		m.carfileCacheEnd(carfileRecord, err)
	}

	return nil
}

func (m *Manager) doCarfileReplicaTask(info *types.CacheCarfileInfo) error {
	exist, err := m.nodeManager.CarfileDB.CarfileRecordExisted(info.CarfileHash)
	if err != nil {
		log.Errorf("%s CarfileRecordExist err:%s", info.CarfileCid, err.Error())
		return err
	}

	var carfileRecord *CarfileRecord
	if exist {
		carfileRecord, err = m.loadCarfileRecord(info.CarfileHash, m)
		if err != nil {
			return err
		}

		carfileRecord.replica = info.Replicas
		carfileRecord.expirationTime = info.Expiration

		carfileRecord.initStep()
	} else {
		carfileRecord = newCarfileRecord(m, info.CarfileCid, info.CarfileHash)
		carfileRecord.replica = info.Replicas
		carfileRecord.expirationTime = info.Expiration
	}

	err = m.nodeManager.CarfileDB.CreateOrUpdateCarfileRecordInfo(&types.CarfileRecordInfo{
		CarfileCid:  carfileRecord.carfileCid,
		Replica:     carfileRecord.replica,
		Expiration:  carfileRecord.expirationTime,
		CarfileHash: carfileRecord.carfileHash,
	})
	if err != nil {
		return xerrors.Errorf("cid:%s,CreateOrUpdateCarfileRecordInfo err:%s", carfileRecord.carfileCid, err.Error())
	}

	m.carfileCacheStart(carfileRecord)

	err = carfileRecord.dispatchCaches()
	if err != nil {
		m.carfileCacheEnd(carfileRecord, err)
	}

	return nil
}

// CacheCarfile new storage task
func (m *Manager) CacheCarfile(info *types.CacheCarfileInfo) error {
	if info.NodeID == "" {
		log.Infof("carfile event %s , add carfile,replica:%d,expiration:%s", info.CarfileCid, info.Replicas, info.Expiration.String())
	} else {
		log.Infof("carfile event %s , add carfile,nodeID:%s", info.CarfileCid, info.NodeID)
	}

	// info.ServerID = string(m.nodeManager.ServerID)
	// err := m.nodeManager.CarfileDB.PushCarfileToWaitList(info)
	// if err != nil {
	// 	log.Errorf("push carfile to wait list: %v", err)
	// }
	err := m.nodeManager.CarfileDB.CreateOrUpdateCarfileRecordInfo(&types.CarfileRecordInfo{
		CarfileCid:  info.CarfileCid,
		Replica:     info.Replicas,
		Expiration:  info.Expiration,
		CarfileHash: info.CarfileHash,
	})
	if err != nil {
		return xerrors.Errorf("cid:%s,CreateOrUpdateCarfileRecordInfo err:%s", info.CarfileCid, err.Error())
	}

	return m.carfiles.Send(CarfileID(info.CarfileHash), CarfileStartCache{
		ID:          info.CarfileCid,
		CarfileHash: CarfileID(info.CarfileHash),
		Replicas:    int64(info.Replicas),
		ServerID:    info.ServerID,
		CreatedAt:   time.Now().Unix(),
		Expiration:  info.Expiration.Unix(),
	})
}

// RemoveCarfileRecord remove a storage
func (m *Manager) RemoveCarfileRecord(carfileCid, hash string) error {
	cInfos, err := m.nodeManager.CarfileDB.CarfileReplicaInfosWithHash(hash, false)
	if err != nil {
		return xerrors.Errorf("GetCarfileReplicaInfosWithHash: %s,err:%s", carfileCid, err.Error())
	}

	err = m.nodeManager.CarfileDB.RemoveCarfileRecord(hash)
	if err != nil {
		return xerrors.Errorf("RemoveCarfileRecord err:%s ", err.Error())
	}

	log.Infof("storage event %s , remove storage record", carfileCid)

	for _, cInfo := range cInfos {
		go m.notifyNodeRemoveCarfile(cInfo.NodeID, carfileCid)
	}

	return nil
}

// RemoveCache remove a cache
func (m *Manager) RemoveCache(carfileCid, nodeID string) error {
	hash, err := cidutil.CIDString2HashString(carfileCid)
	if err != nil {
		return err
	}

	dI, exist := m.DownloadingCarfileRecords.Load(hash)
	if exist && dI != nil {
		return xerrors.Errorf("task %s is downloading, please wait", carfileCid)
	}

	cacheInfo, err := m.nodeManager.CarfileDB.LoadReplicaInfo(replicaID(hash, nodeID))
	if err != nil {
		return xerrors.Errorf("GetReplicaInfo: %s,err:%s", carfileCid, err.Error())
	}

	// delete cache and update carfile info
	err = m.nodeManager.CarfileDB.RemoveCarfileReplica(cacheInfo.NodeID, cacheInfo.CarfileHash)
	if err != nil {
		return err
	}

	log.Infof("carfile event %s , remove cache task:%s", carfileCid, nodeID)

	go m.notifyNodeRemoveCarfile(cacheInfo.NodeID, carfileCid)

	return nil
}

// CacheCarfileResult block cache result
func (m *Manager) CacheCarfileResult(nodeID string, info *types.CacheResult) (err error) {
	log.Info("carfileCacheResult :%s , %d , %s", nodeID, info.Status, info.CarfileHash)
	// log.Debugf("carfileCacheResult :%v", info)

	// var carfileRecord *CarfileRecord
	// dI, exist := m.DownloadingCarfileRecords.Load(info.CarfileHash)
	// if exist && dI != nil {
	// 	carfileRecord = dI.(*CarfileRecord)
	// } else {
	// 	err = xerrors.Errorf("task not downloading : %s,%s ,err:%v", nodeID, info.CarfileHash, err)
	// 	return
	// }

	// if carfileRecord.step == rootCandidateCacheStep {
	// 	carfileRecord.totalSize = info.CarfileSize
	// 	carfileRecord.totalBlocks = info.CarfileBlockCount
	// }

	// err = carfileRecord.carfileCacheResult(nodeID, info)

	if info.Status == types.CacheStatusSucceeded {
		t, err := m.nodeManager.NodeMgrDB.NodeType(nodeID)
		if err != nil {
			return err
		}

		var source *types.DownloadSource
		if t == types.NodeCandidate {
			// TODO if node offline
			cNode := m.nodeManager.GetCandidateNode(nodeID)
			if cNode != nil {
				source = &types.DownloadSource{
					CandidateURL:   cNode.RPCURL(),
					CandidateToken: string(m.writeToken),
				}
			}
		}

		err = m.carfiles.Send(CarfileID(info.CarfileHash), CarfileCacheCompleted{
			ResultInfo: &NodeCacheResult{
				NodeID:            nodeID,
				IsCandidate:       t == types.NodeCandidate,
				Status:            int64(info.Status),
				CarfileBlockCount: int64(info.CarfileBlockCount),
				CarfileSize:       info.CarfileSize,
				Source:            source,
			},
		})
	}

	return
}

func (m *Manager) startCarfileReplicaTasks() {
	doLen := downloadingCarfileMaxCount - m.downloadingTaskCount
	if doLen <= 0 {
		return
	}

	for i := 0; i < doLen; i++ {
		info, err := m.nodeManager.CarfileDB.LoadWaitCarfiles(m.nodeManager.ServerID)
		if err != nil {
			// if cache.IsNilErr(err) {
			// 	return
			// }
			// log.Errorf("GetWaitCarfile err:%s", err.Error())
			continue
		}

		if _, exist := m.DownloadingCarfileRecords.Load(info.CarfileHash); exist {
			log.Errorf("carfileRecord %s is downloading, please wait", info.CarfileCid)
			continue
		}

		if info.NodeID != "" {
			err = m.cacheCarfileToNode(info)
		} else {
			err = m.doCarfileReplicaTask(info)
		}
		if err != nil {
			log.Errorf("storage %s do caches err:%s", info.CarfileCid, err.Error())
		}
	}
}

func (m *Manager) carfileCacheStart(cr *CarfileRecord) {
	_, exist := m.DownloadingCarfileRecords.LoadOrStore(cr.carfileHash, cr)
	if !exist {
		m.downloadingTaskCount++
	}

	log.Infof("storage %s cache task start ----- cur downloading count : %d", cr.carfileCid, m.downloadingTaskCount)
}

func (m *Manager) carfileCacheEnd(cr *CarfileRecord, err error) {
	_, exist := m.DownloadingCarfileRecords.LoadAndDelete(cr.carfileHash)
	if exist {
		m.downloadingTaskCount--
	}

	log.Infof("storage %s cache task end ----- cur downloading count : %d", cr.carfileCid, m.downloadingTaskCount)

	m.resetLatelyExpirationTime(cr.expirationTime)

	// save result msg
	info := &types.CarfileRecordCacheResult{
		NodeErrs:             cr.nodeCacheErrs,
		EdgeNodeCacheSummary: cr.findNodesDetails,
	}
	if err != nil {
		info.ErrMsg = err.Error()
	}

	// err = cache.SetCarfileRecordCacheResult(cr.carfileHash, info)
	// if err != nil {
	// 	log.Errorf("SetCarfileRecordCacheResult err:%s", err.Error())
	// }
}

// ResetCarfileRecordExpiration reset expiration time
func (m *Manager) ResetCarfileRecordExpiration(cid string, t time.Time) error {
	hash, err := cidutil.CIDString2HashString(cid)
	if err != nil {
		return err
	}

	log.Infof("storage event %s , reset storage expiration time:%s", cid, t.String())

	dI, exist := m.DownloadingCarfileRecords.Load(hash)
	if exist && dI != nil {
		carfileRecord := dI.(*CarfileRecord)
		carfileRecord.expirationTime = t
	}

	err = m.nodeManager.CarfileDB.ResetCarfileRecordExpiration(hash, t)
	if err != nil {
		return err
	}

	m.resetLatelyExpirationTime(t)

	return nil
}

// check expiration caches
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
		err = m.RemoveCarfileRecord(carfileRecord.CarfileCid, carfileRecord.CarfileHash)
		log.Infof("cid:%s, expired,remove it ; %v", carfileRecord.CarfileCid, err)
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

// Notify node to delete all carfile
func (m *Manager) notifyNodeRemoveCarfiles(nodeID string) error {
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
func (m *Manager) notifyNodeRemoveCarfile(nodeID, cid string) error {
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
	candidateReplicaCacheCount = count
}

// GetCandidateReplicaCount get candidta replica count
func (m *Manager) GetCandidateReplicaCount() int {
	return candidateReplicaCacheCount
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

	cr, err := m.GetCarfileRecord(hash)
	if err != nil {
		return nil, err
	}

	dInfo := carfileRecord2Info(cr)

	// result, err := cache.GetCarfileRecordCacheResult(hash)
	// if err == nil {
	// 	dInfo.ResultInfo = result
	// }

	return dInfo, nil
}

// GetDownloadingCarfileInfos get all downloading carfiles
func (m *Manager) GetDownloadingCarfileInfos() []*types.CarfileRecordInfo {
	infos := make([]*types.CarfileRecordInfo, 0)

	m.DownloadingCarfileRecords.Range(func(key, value interface{}) bool {
		if value != nil {
			data := value.(*CarfileRecord)
			if data != nil {
				cInfo := carfileRecord2Info(data)
				infos = append(infos, cInfo)
			}
		}

		return true
	})

	return infos
}

func carfileRecord2Info(cr *CarfileRecord) *types.CarfileRecordInfo {
	info := &types.CarfileRecordInfo{}
	if cr != nil {
		info.CarfileCid = cr.carfileCid
		info.CarfileHash = cr.carfileHash
		info.TotalSize = cr.totalSize
		info.Replica = cr.replica
		info.EdgeReplica = cr.edgeReplica
		info.TotalBlocks = cr.totalBlocks
		info.Expiration = cr.expirationTime

		raInfos := make([]*types.ReplicaInfo, 0)

		cr.Replicas.Range(func(key, value interface{}) bool {
			ra := value.(*Replica)

			raInfo := &types.ReplicaInfo{
				Status:      ra.status,
				DoneSize:    ra.doneSize,
				DoneBlocks:  ra.doneBlocks,
				IsCandidate: ra.isCandidate,
				NodeID:      ra.nodeID,
				EndTime:     ra.endTime,
			}

			raInfos = append(raInfos, raInfo)
			return true
		})

		info.ReplicaInfos = raInfos
	}

	return info
}

func (m *Manager) CarfilesStatus(ctx context.Context, cid types.CarfileID) (types.CarfileInfo, error) {
	info, err := m.GetCarfileInfo(CarfileID(cid))
	if err != nil {
		return types.CarfileInfo{}, err
	}

	cLog := make([]types.Log, len(info.Log))
	for i, l := range info.Log {
		cLog[i] = types.Log{
			Kind:      l.Kind,
			Timestamp: l.Timestamp,
			Trace:     l.Trace,
			Message:   l.Message,
		}
	}

	cInfo := types.CarfileInfo{
		CarfileCID:  cid.String(),
		State:       types.CarfileState(info.State),
		CarfileHash: types.CarfileID(info.CarfileHash),
		Replicas:    info.Replicas,
		ServerID:    info.ServerID,
		Size:        info.Size,
		Blocks:      info.Blocks,
		CreatedAt:   time.Unix(info.CreatedAt, 0),
		Expiration:  time.Unix(info.Expiration, 0),
		Log:         cLog,
	}

	return cInfo, nil
}

// Find edges that meet the cache criteria
func (m *Manager) findEdges(count int, filterNodes map[string]struct{}) []*node.Edge {
	list := make([]*node.Edge, 0)

	if count <= 0 {
		return list
	}

	m.nodeManager.EdgeNodes.Range(func(key, value interface{}) bool {
		edgeNode := value.(*node.Edge)

		if _, exist := filterNodes[edgeNode.NodeID]; exist {
			return true
		}

		node := value.(*node.Edge)
		if node.DiskUsage > diskUsageMax {
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
func (m *Manager) findCandidates(count int, filterNodes map[string]struct{}) []*node.Candidate {
	list := make([]*node.Candidate, 0)

	if count <= 0 {
		return list
	}

	m.nodeManager.CandidateNodes.Range(func(key, value interface{}) bool {
		candidateNode := value.(*node.Candidate)

		if _, exist := filterNodes[candidateNode.NodeID]; exist {
			return true
		}

		node := value.(*node.Candidate)
		if node.DiskUsage > diskUsageMax {
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
			IsCandidate: true,
		})
	}

	return m.nodeManager.CarfileDB.CreateCarfileReplicaInfos(replicaInfos)
}

func (m *Manager) saveEdgeReplicaInfos(nodes []*node.Edge, hash string) error {
	// save replica info
	replicaInfos := make([]*types.ReplicaInfo, 0)

	for _, node := range nodes {
		replicaInfos = append(replicaInfos, &types.ReplicaInfo{
			ID:          replicaID(hash, node.NodeID),
			NodeID:      node.NodeID,
			Status:      types.CacheStatusDownloading,
			IsCandidate: false,
		})
	}

	return m.nodeManager.CarfileDB.CreateCarfileReplicaInfos(replicaInfos)
}
