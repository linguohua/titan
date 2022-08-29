package scheduler

import "sync"

var (
	// node pool map 节点池
	poolMap sync.Map // {key:geo,val:*NodePool}

	// 节点所在池子记录
	poolIDMap sync.Map // {key:deviceID,val:geo}

)

type nodeStataus int

const (
	nodeStatusNothing   nodeStataus = iota // 初始状态
	nodeStatusVerified                     // 被验证者
	nodeStatusAssigned                     // 已分配
	nodeStatusValidator                    // 验证者
)

// NodePool Node Pool
type NodePool struct {
	geoID          string
	edgeNodes      map[string]nodeStataus
	candidateNodes map[string]nodeStataus

	surplusCandidateNodes []string
}

// NewNodePool new pool
func NewNodePool(geo string) *NodePool {
	pool := &NodePool{
		geoID:                 geo,
		edgeNodes:             make(map[string]nodeStataus),
		candidateNodes:        make(map[string]nodeStataus),
		surplusCandidateNodes: make([]string, 0),
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

	g.surplusCandidateNodes = make([]string, 0)
}

func (g *NodePool) addEdge(node *EdgeNode) {
	dID := node.deviceInfo.DeviceId

	if _, ok := g.edgeNodes[dID]; ok {
		return
	}

	g.edgeNodes[dID] = nodeStatusNothing
}

func (g *NodePool) addCandidate(node *CandidateNode) {
	dID := node.deviceInfo.DeviceId

	if _, ok := g.candidateNodes[dID]; ok {
		return
	}

	g.candidateNodes[dID] = nodeStatusNothing
}

func (g *NodePool) delEdge(dID string) {
	if _, ok := g.edgeNodes[dID]; ok {
		delete(g.edgeNodes, dID)
	}
}

func (g *NodePool) delCandidate(dID string) {
	if _, ok := g.candidateNodes[dID]; ok {
		delete(g.candidateNodes, dID)
	}
}

func loadNodePoolMap(geoKey string) *NodePool {
	pool, ok := poolMap.Load(geoKey)
	if ok && pool != nil {
		return pool.(*NodePool)
	}

	return nil
}

func storeNodePoolMap(geoKey string, val *NodePool) {
	poolMap.Store(geoKey, val)
}

func addEdgeToPool(node *EdgeNode) string {
	geo := node.geoInfo.Geo
	deviceID := node.deviceInfo.DeviceId

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

	pool.addEdge(node)

	storeNodePoolMap(geo, pool)
	poolIDMap.Store(deviceID, geo)

	return geo
}

func addCandidateToPool(node *CandidateNode) string {
	geo := node.geoInfo.Geo
	deviceID := node.deviceInfo.DeviceId

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

	pool.addCandidate(node)

	storeNodePoolMap(geo, pool)
	poolIDMap.Store(deviceID, geo)

	return geo
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
