package storage

import (
	"sync"
	"time"

	"github.com/linguohua/titan/api/types"

	"github.com/linguohua/titan/node/scheduler/node"
)

// CarfileRecord CarfileRecord
type CarfileRecord struct {
	nodeManager    *node.Manager
	carfileManager *Manager

	carfileCid     string
	carfileHash    string
	replica        int64
	totalSize      int64
	totalBlocks    int64
	expirationTime time.Time

	downloadSources  []*types.DownloadSource
	candidateReploca int64
	Replicas         sync.Map

	lock sync.RWMutex

	edgeReplica      int64 // An edge node represents a reliability
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
	dInfo, err := m.nodeManager.CarfileDB.CarfileInfo(hash)
	if err != nil {
		return nil, err
	}

	cr := &CarfileRecord{}
	cr.carfileCid = dInfo.CarfileCID
	cr.nodeManager = manager.nodeManager
	cr.carfileManager = manager
	cr.totalSize = dInfo.TotalSize
	cr.replica = dInfo.NeedEdgeReplica
	cr.totalBlocks = dInfo.TotalBlocks
	cr.expirationTime = dInfo.Expiration
	cr.carfileHash = dInfo.CarfileHash
	cr.downloadSources = make([]*types.DownloadSource, 0)
	cr.nodeCacheErrs = make(map[string]string)

	raInfos, err := m.nodeManager.CarfileDB.ReplicaInfosByCarfile(hash, false)
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
