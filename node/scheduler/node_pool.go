package scheduler

import (
	"fmt"
	"sync"
)

var (
	// node pool map 节点池
	poolMap sync.Map // {key:geo,val:*NodePool}

	// 节点所在池子记录
	poolIDMap sync.Map // {key:deviceID,val:geo}

	pendingEdgeMap      sync.Map
	pendingCandidateMap sync.Map
)

// type nodeStataus int

// const (
// 	nodeStatusNothing nodeStataus = iota // 初始状态
// 	// nodeStatusVerified                     // 被验证者
// 	nodeStatusValidator // 验证者
// )

// NodePool Node Pool
type NodePool struct {
	geoID          string
	edgeNodes      map[string]bandwidthInfo
	candidateNodes map[string]bandwidthInfo
	veriftorNodes  map[string]bandwidthInfo
}

// bandwidthInfo Info
type bandwidthInfo struct {
	BandwidthUp   int64 `json:"bandwidth_up"`   // 上行带宽B/s
	BandwidthDown int64 `json:"bandwidth_down"` // 下行带宽B/s
}

// NewNodePool new pool
func NewNodePool(geo string) *NodePool {
	pool := &NodePool{
		geoID:          geo,
		edgeNodes:      make(map[string]bandwidthInfo),
		candidateNodes: make(map[string]bandwidthInfo),
		veriftorNodes:  make(map[string]bandwidthInfo),
	}

	g, ok := poolMap.LoadOrStore(geo, pool)
	if ok && g != nil {
		return g.(*NodePool)
	}

	return pool
}

func (g *NodePool) setVeriftor(deviceID string) {
	if info, ok := g.candidateNodes[deviceID]; ok {
		g.veriftorNodes[deviceID] = info

		delete(g.candidateNodes, deviceID)

		return
	}
}

func (g *NodePool) resetVeriftors() {
	for deviceID, info := range g.veriftorNodes {
		g.candidateNodes[deviceID] = info
	}

	g.veriftorNodes = make(map[string]bandwidthInfo)
}

func (g *NodePool) addEdge(node *EdgeNode) {
	deviceID := node.deviceInfo.DeviceId
	if _, ok := g.edgeNodes[deviceID]; ok {
		return
	}

	g.edgeNodes[deviceID] = bandwidthInfo{BandwidthUp: node.deviceInfo.BandwidthUp, BandwidthDown: node.deviceInfo.BandwidthDown}
}

func (g *NodePool) addCandidate(node *CandidateNode) {
	deviceID := node.deviceInfo.DeviceId
	if _, ok := g.candidateNodes[deviceID]; ok {
		return
	}

	g.candidateNodes[deviceID] = bandwidthInfo{BandwidthUp: node.deviceInfo.BandwidthUp, BandwidthDown: node.deviceInfo.BandwidthDown}
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

func addEdgeToPool(node *EdgeNode) string {
	deviceID := node.deviceInfo.DeviceId
	geo := node.geoInfo.Geo

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
	deviceID := node.deviceInfo.DeviceId
	geo := node.geoInfo.Geo

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

func addPendingNode(edgeNode *EdgeNode, candidateNode *CandidateNode) {
	if edgeNode != nil {
		pendingEdgeMap.Store(edgeNode.deviceInfo.DeviceId, edgeNode)
	}

	if candidateNode != nil {
		pendingCandidateMap.Store(candidateNode.deviceInfo.DeviceId, candidateNode)
	}
}

func nodesToPool() {
	pendingEdgeMap.Range(func(key, value interface{}) bool {
		deviceID := key.(string)
		node := value.(*EdgeNode)

		addEdgeToPool(node)

		pendingEdgeMap.Delete(deviceID)

		return true
	})

	pendingCandidateMap.Range(func(key, value interface{}) bool {
		deviceID := key.(string)
		node := value.(*CandidateNode)

		addCandidateToPool(node)

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

		es := ""
		cs := ""
		vs := ""

		for s := range pool.edgeNodes {
			es = fmt.Sprintf("%s%s,", es, s)
		}
		for s := range pool.candidateNodes {
			cs = fmt.Sprintf("%s%s,", cs, s)
		}
		for s := range pool.veriftorNodes {
			vs = fmt.Sprintf("%s%s,", vs, s)
		}
		log.Info("geo:", geo, ",edgeNodes:", es, ",candidateNodes:", cs, ",veriftorNodes:", vs)

		return true
	})
}
