package scheduler

import (
	"sync"

	"github.com/linguohua/titan/api"
)

var (
	// node pool map 节点池
	poolMap sync.Map // {key:geo,val:*NodePool}

	// 节点所在池子记录
	poolIDMap sync.Map // {key:deviceID,val:geo}

	pendingEdgeMap      sync.Map
	pendingCandidateMap sync.Map
)

type nodeStataus int

const (
	nodeStatusNothing   nodeStataus = iota // 初始状态
	nodeStatusVerified                     // 被验证者
	nodeStatusValidator                    // 验证者
)

// NodePool Node Pool
type NodePool struct {
	geoID          string
	edgeNodes      map[string]nodeStataus
	candidateNodes map[string]nodeStataus
}

// NewNodePool new pool
func NewNodePool(geo string) *NodePool {
	pool := &NodePool{
		geoID:          geo,
		edgeNodes:      make(map[string]nodeStataus),
		candidateNodes: make(map[string]nodeStataus),
	}

	g, ok := poolMap.LoadOrStore(geo, pool)
	if ok && g != nil {
		return g.(*NodePool)
	}

	return pool
}

func (g *NodePool) resetNodeStataus() {
	for deviceID := range g.edgeNodes {
		g.edgeNodes[deviceID] = nodeStatusNothing
	}
	for deviceID := range g.candidateNodes {
		g.candidateNodes[deviceID] = nodeStatusNothing
	}
}

func (g *NodePool) addEdge(deviceID string) {
	if _, ok := g.edgeNodes[deviceID]; ok {
		return
	}

	g.edgeNodes[deviceID] = nodeStatusNothing
}

func (g *NodePool) addCandidate(deviceID string) {
	if _, ok := g.candidateNodes[deviceID]; ok {
		return
	}

	g.candidateNodes[deviceID] = nodeStatusNothing
}

func (g *NodePool) delEdge(deviceID string) {
	if _, ok := g.edgeNodes[deviceID]; ok {
		delete(g.edgeNodes, deviceID)
	}
}

func (g *NodePool) delCandidate(deviceID string) {
	if _, ok := g.candidateNodes[deviceID]; ok {
		delete(g.candidateNodes, deviceID)
	}
}

func loadNodePoolMap(geoKey string) *NodePool {
	pool, ok := poolMap.Load(geoKey)
	if ok && pool != nil {
		return pool.(*NodePool)
	}

	return nil
}

func storeNodePoolMap(geo string, val *NodePool) {
	poolMap.Store(geo, val)
}

func addEdgeToPool(deviceID, geo string) string {
	oldPoolID, ok := poolIDMap.Load(deviceID)
	if ok && oldPoolID != nil {
		oldGeo := oldPoolID.(string)
		// 得看原来的geo是否跟现在的一样
		if oldGeo != geo {
			// 从旧的组里面删除
			pool := loadNodePoolMap(oldGeo)
			if pool != nil {
				pool.delEdge(deviceID)
			}
		} else {
			return oldGeo
		}
	}

	pool := loadNodePoolMap(geo)
	if pool == nil {
		pool = NewNodePool(geo)
	}

	pool.addEdge(deviceID)

	storeNodePoolMap(geo, pool)
	poolIDMap.Store(deviceID, geo)

	return geo
}

func addCandidateToPool(deviceID, geo string) string {
	oldPoolID, ok := poolIDMap.Load(deviceID)
	if ok && oldPoolID != nil {
		oldGeo := oldPoolID.(string)
		// 得看原来的geo是否跟现在的一样
		if oldGeo != geo {
			// 从旧的组里面删除
			pool := loadNodePoolMap(oldGeo)
			if pool != nil {
				pool.delCandidate(deviceID)
			}
		} else {
			return oldGeo
		}
	}

	pool := loadNodePoolMap(geo)
	if pool == nil {
		pool = NewNodePool(geo)
	}

	pool.addCandidate(deviceID)

	storeNodePoolMap(geo, pool)
	poolIDMap.Store(deviceID, geo)

	return geo
}

func addPendingNode(deviceID, geo string, nodeType api.NodeType) {
	switch nodeType {
	case api.NodeEdge:
		pendingEdgeMap.Store(deviceID, geo)
		return
	case api.NodeCandidate:
		pendingCandidateMap.Store(deviceID, geo)
		return
	default:
		return
	}
}

func nodesToPool() {
	pendingEdgeMap.Range(func(key, value interface{}) bool {
		deviceID := key.(string)
		geo := value.(string)

		addEdgeToPool(deviceID, geo)

		pendingEdgeMap.Delete(deviceID)

		return true
	})

	pendingCandidateMap.Range(func(key, value interface{}) bool {
		deviceID := key.(string)
		geo := value.(string)

		addCandidateToPool(deviceID, geo)

		pendingCandidateMap.Delete(deviceID)

		return true
	})
}

// PrintlnMap Println
func testPrintlnPoolMap() {
	log.Info("poolMap--------------------------------")
	poolMap.Range(func(key, value interface{}) bool {
		geo := key.(string)
		pool := value.(*NodePool)

		log.Info("geo:", geo, ",edgeNodes:", pool.edgeNodes, ",candidateNodes:", pool.candidateNodes)

		return true
	})
}
