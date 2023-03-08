package storage

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/linguohua/titan/api/types"

	"github.com/linguohua/titan/node/scheduler/node"
	"golang.org/x/xerrors"
)

const (
	rootCandidateCacheStep    = iota // step 1: Pull storage from ipfs to titan
	candidateReplicaCacheStep        // step 2: Pull storage from root candidate to othe candidates
	edgeReplicaCacheStep             // step 3: Pull storage from candidates to edges
	cachedStep                       // step 4: Caches end
)

// CarfileRecord CarfileRecord
type CarfileRecord struct {
	nodeManager    *node.Manager
	carfileManager *Manager

	carfileCid     string
	carfileHash    string
	replica        int
	totalSize      int64
	totalBlocks    int
	expirationTime time.Time

	downloadSources  []*types.DownloadSource
	candidateReploca int
	Replicas         sync.Map

	lock sync.RWMutex

	edgeReplica      int // An edge node represents a reliability
	step             int
	nodeCacheErrs    map[string]string // Node cache error info
	findNodesDetails string            // Find the details of nodes that meet the cache conditions
}

func newCarfileRecord(manager *Manager, cid, hash string) *CarfileRecord {
	return &CarfileRecord{
		nodeManager:     manager.nodeManager,
		carfileManager:  manager,
		carfileCid:      cid,
		carfileHash:     hash,
		downloadSources: make([]*types.DownloadSource, 0),
		nodeCacheErrs:   make(map[string]string),
	}
}

func (m *Manager) loadCarfileRecord(hash string, manager *Manager) (*CarfileRecord, error) {
	dInfo, err := m.nodeManager.CarfileDB.LoadCarfileInfo(hash)
	if err != nil {
		return nil, err
	}

	cr := &CarfileRecord{}
	cr.carfileCid = dInfo.CarfileCid
	cr.nodeManager = manager.nodeManager
	cr.carfileManager = manager
	cr.totalSize = dInfo.TotalSize
	cr.replica = dInfo.Replica
	cr.totalBlocks = dInfo.TotalBlocks
	cr.expirationTime = dInfo.Expiration
	cr.carfileHash = dInfo.CarfileHash
	cr.downloadSources = make([]*types.DownloadSource, 0)
	cr.nodeCacheErrs = make(map[string]string)

	raInfos, err := m.nodeManager.CarfileDB.CarfileReplicaInfosWithHash(hash, false)
	if err != nil {
		log.Errorf("loadData hash:%s, GetCarfileReplicaInfosWithHash err:%s", hash, err.Error())
		return cr, err
	}

	for _, raInfo := range raInfos {
		if raInfo == nil {
			continue
		}

		ra := &Replica{
			id:            raInfo.ID,
			nodeID:        raInfo.NodeID,
			carfileRecord: cr,
			status:        raInfo.Status,
			isCandidate:   raInfo.IsCandidate,
			carfileHash:   raInfo.CarfileHash,
			nodeManager:   cr.nodeManager,
			createTime:    raInfo.CreateTime,
			endTime:       raInfo.EndTime,
		}

		if ra.status == types.CacheStatusSucceeded {
			ra.doneBlocks = cr.totalBlocks
			ra.doneSize = cr.totalSize

			if ra.isCandidate {
				cr.candidateReploca++

				cNode := cr.nodeManager.GetCandidateNode(ra.nodeID)
				if cNode != nil {
					cr.downloadSources = append(cr.downloadSources, &types.DownloadSource{
						CandidateURL:   cNode.RPCURL(),
						CandidateToken: string(cr.carfileManager.writeToken),
					})
				}
			} else {
				cr.edgeReplica++
			}
		}

		cr.Replicas.Store(raInfo.NodeID, ra)
	}

	return cr, nil
}

func (cr *CarfileRecord) startCacheReplicas(nodes []string, isCandidate bool) (downloading bool) {
	downloading = false

	// init replicas status
	err := cr.nodeManager.CarfileDB.UpdateCarfileReplicaStatus(cr.carfileHash, nodes, types.CacheStatusDownloading)
	if err != nil {
		log.Errorf("startCacheReplicas %s , UpdateCarfileReplicaStatus err:%s", cr.carfileHash, err.Error())
		return
	}

	err = cr.nodeManager.CarfileDB.ReplicaTasksStart(cr.nodeManager.ServerID, cr.carfileHash, nodes)
	if err != nil {
		log.Errorf("startCacheReplicas %s , ReplicaTasksStart err:%s", cr.carfileHash, err.Error())
		return
	}

	errorList := make([]string, 0)

	for _, nodeID := range nodes {
		// find or create cache task
		var ra *Replica
		cI, exist := cr.Replicas.Load(nodeID)
		if !exist || cI == nil {
			ra, err = cr.newReplica(cr, nodeID, isCandidate)
			if err != nil {
				log.Errorf("newReplica %s , node:%s,err:%s", cr.carfileCid, nodeID, err.Error())
				errorList = append(errorList, nodeID)
				continue
			}
			cr.Replicas.Store(nodeID, ra)
		} else {
			ra = cI.(*Replica)
		}

		// do cache
		err = ra.cacheCarfile(nodeCacheTimeoutTime)
		if err != nil {
			log.Errorf("cacheCarfile %s , node:%s,err:%s", cr.carfileCid, ra.nodeID, err.Error())
			errorList = append(errorList, nodeID)
			continue
		}

		downloading = true
	}

	if len(errorList) > 0 {
		// set caches status
		err := cr.nodeManager.CarfileDB.UpdateCarfileReplicaStatus(cr.carfileHash, errorList, types.CacheStatusFailed)
		if err != nil {
			log.Errorf("startReplicaTasks %s , UpdateCarfileReplicaStatus err:%s", cr.carfileHash, err.Error())
		}

		_, err = cr.nodeManager.CarfileDB.ReplicaTasksEnd(cr.nodeManager.ServerID, cr.carfileHash, errorList)
		if err != nil {
			log.Errorf("startReplicaTasks %s , ReplicaTasksEnd err:%s", cr.carfileHash, err.Error())
		}
	}

	return
}

func (cr *CarfileRecord) cacheToCandidates(needCount int) error {
	result := cr.findCandidates(needCount)
	cr.findNodesDetails = fmt.Sprintf("totalCandidates:%d,cachedCount:%d,insufficientDiskCount:%d,need:%d",
		result.totalCount, result.cachedCount, result.insufficientDiskCount, needCount)
	if len(result.list) <= 0 {
		return xerrors.New("not found candidate")
	}

	if !cr.startCacheReplicas(result.list, true) {
		return xerrors.New("running err")
	}

	return nil
}

func (cr *CarfileRecord) cacheToEdges(needCount int) error {
	if len(cr.downloadSources) <= 0 {
		return xerrors.New("not found cache sources")
	}

	result := cr.findEdges(needCount)
	cr.findNodesDetails = fmt.Sprintf("totalEdges:%d,cachedCount:%d,insufficientDiskCount:%d,need:%d",
		result.totalCount, result.cachedCount, result.insufficientDiskCount, needCount)
	if len(result.list) <= 0 {
		return xerrors.New("not found edge")
	}

	if !cr.startCacheReplicas(result.list, false) {
		return xerrors.New("running err")
	}

	return nil
}

func (cr *CarfileRecord) initStep() {
	cr.step = cachedStep

	if cr.candidateReploca <= 0 {
		cr.step = rootCandidateCacheStep
		return
	}

	if cr.candidateReploca < rootCacheCount+candidateReplicaCacheCount {
		cr.step = candidateReplicaCacheStep
		return
	}

	if cr.edgeReplica < cr.replica {
		cr.step = edgeReplicaCacheStep
	}
}

func (cr *CarfileRecord) nextStep() {
	cr.step++

	if cr.step == candidateReplicaCacheStep {
		needCacdidateCount := (rootCacheCount + candidateReplicaCacheCount) - cr.candidateReploca
		if needCacdidateCount <= 0 {
			// no need to cache to candidate , skip this step
			cr.step++
		}
	}
}

// cache a carfile to the node
func (cr *CarfileRecord) dispatchCache(nodeID string) error {
	cNode := cr.nodeManager.GetCandidateNode(nodeID)
	if cNode != nil {
		if !cr.startCacheReplicas([]string{nodeID}, true) {
			return xerrors.New("running err")
		}

		return nil
	}

	eNode := cr.nodeManager.GetEdgeNode(nodeID)
	if eNode != nil {
		if len(cr.downloadSources) <= 0 {
			return xerrors.New("not found cache sources")
		}

		if !cr.startCacheReplicas([]string{nodeID}, false) {
			return xerrors.New("running err")
		}

		return nil
	}

	return xerrors.Errorf("node %s not found", nodeID)
}

func (cr *CarfileRecord) dispatchCaches() error {
	switch cr.step {
	case rootCandidateCacheStep:
		return cr.cacheToCandidates(rootCacheCount)
	case candidateReplicaCacheStep:
		if cr.candidateReploca == 0 {
			return xerrors.New("no root reploca")
		}
		needCacdidateCount := (rootCacheCount + candidateReplicaCacheCount) - cr.candidateReploca
		if needCacdidateCount <= 0 {
			return xerrors.New("no caching required to candidate node")
		}
		return cr.cacheToCandidates(needCacdidateCount)
	case edgeReplicaCacheStep:
		needEdgeCount := cr.replica - cr.edgeReplica
		if needEdgeCount <= 0 {
			return xerrors.New("no caching required to edge node")
		}
		return cr.cacheToEdges(needEdgeCount)
	}

	return xerrors.New("steps completed")
}

func (cr *CarfileRecord) replicaCacheEnd(ra *Replica, errMsg string) error {
	cr.lock.Lock()
	defer cr.lock.Unlock()

	if ra.status == types.CacheStatusSucceeded {
		if ra.isCandidate {
			cr.candidateReploca++

			cNode := cr.nodeManager.GetCandidateNode(ra.nodeID)
			if cNode != nil {
				cr.downloadSources = append(cr.downloadSources, &types.DownloadSource{
					CandidateURL:   cNode.RPCURL(),
					CandidateToken: string(cr.carfileManager.writeToken),
				})
			}
		} else {
			cr.edgeReplica++
		}
	} else if ra.status == types.CacheStatusFailed {
		// node err msg
		cr.nodeCacheErrs[ra.nodeID] = errMsg
	}

	// Carfile caches end
	info := &types.CarfileRecordInfo{
		CarfileHash: cr.carfileHash,
		TotalSize:   cr.totalSize,
		TotalBlocks: cr.totalBlocks,
		Replica:     cr.replica,
		Expiration:  cr.expirationTime,
	}
	return cr.nodeManager.CarfileDB.UpdateCarfileRecordCachesInfo(info)
}

func (cr *CarfileRecord) carfileCacheResult(nodeID string, info *types.CacheResult) error {
	rI, exist := cr.Replicas.Load(nodeID)
	if !exist {
		return xerrors.Errorf("cacheCarfileResult not found nodeID:%s,cid:%s", nodeID, cr.carfileCid)
	}
	ra := rI.(*Replica)

	ra.status = info.Status
	ra.doneBlocks = info.DoneBlockCount
	ra.doneSize = info.DoneSize

	if ra.status == types.CacheStatusDownloading {
		// update cache task timeout
		ra.countDown = nodeCacheTimeoutTime
		return nil
	}

	// update node info
	node := cr.nodeManager.GetNode(ra.nodeID)
	if node != nil {
		node.IncrCurCacheCount(-1)
	}

	err := ra.updateInfo()
	if err != nil {
		return xerrors.Errorf("endCache %s , updateReplicaInfo err:%s", ra.carfileHash, err.Error())
	}

	err = cr.replicaCacheEnd(ra, info.Msg)
	if err != nil {
		return xerrors.Errorf("endCache %s , updateCarfileRecordInfo err:%s", ra.carfileHash, err.Error())
	}

	cachesDone, err := cr.nodeManager.CarfileDB.ReplicaTasksEnd(cr.nodeManager.ServerID, ra.carfileHash, []string{ra.nodeID})
	if err != nil {
		return xerrors.Errorf("endCache %s , ReplicaTasksEnd err:%s", ra.carfileHash, err.Error())
	}

	if !cachesDone {
		// caches undone
		return nil
	}

	// next step
	cr.nextStep()

	err = cr.dispatchCaches()
	if err != nil {
		cr.carfileManager.carfileCacheEnd(cr, err)
	}

	return nil
}

type findNodeResult struct {
	list                  []string
	totalCount            int
	cachedCount           int
	insufficientDiskCount int
}

// Find edges that meet the cache criteria
func (cr *CarfileRecord) findEdges(count int) *findNodeResult {
	resultInfo := &findNodeResult{}

	if count <= 0 {
		return resultInfo
	}

	nodes := make([]*node.BaseInfo, 0)
	cr.nodeManager.EdgeNodes.Range(func(key, value interface{}) bool {
		nodeID := key.(string)
		resultInfo.totalCount++

		if cI, exist := cr.Replicas.Load(nodeID); exist {
			cache := cI.(*Replica)
			if cache.status == types.CacheStatusSucceeded {
				resultInfo.cachedCount++
				return true
			}
		}

		node := value.(*node.Edge)
		if node.DiskUsage > diskUsageMax {
			resultInfo.insufficientDiskCount++
			return true
		}

		nodes = append(nodes, node.BaseInfo)
		return true
	})

	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].CurCacheCount() < nodes[j].CurCacheCount()
	})

	if count > len(nodes) {
		count = len(nodes)
	}

	for _, node := range nodes[:count] {
		resultInfo.list = append(resultInfo.list, node.NodeID)
	}
	return resultInfo
}

// Find candidates that meet the cache criteria
func (cr *CarfileRecord) findCandidates(count int) *findNodeResult {
	resultInfo := &findNodeResult{}

	if count <= 0 {
		return resultInfo
	}

	nodes := make([]*node.BaseInfo, 0)
	cr.nodeManager.CandidateNodes.Range(func(key, value interface{}) bool {
		nodeID := key.(string)
		resultInfo.totalCount++

		if cI, exist := cr.Replicas.Load(nodeID); exist {
			cache := cI.(*Replica)
			if cache.status == types.CacheStatusSucceeded {
				resultInfo.cachedCount++
				return true
			}
		}

		node := value.(*node.Candidate)
		if node.DiskUsage > diskUsageMax {
			resultInfo.insufficientDiskCount++
			return true
		}

		nodes = append(nodes, node.BaseInfo)
		return true
	})

	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].CurCacheCount() < nodes[j].CurCacheCount()
	})

	if count > len(nodes) {
		count = len(nodes)
	}

	for _, node := range nodes[:count] {
		resultInfo.list = append(resultInfo.list, node.NodeID)
	}
	return resultInfo
}