package scheduler

import (
	"fmt"
	"sync"
)

// PoolGroup Node Pool Group
type PoolGroup struct {
	// node pool map
	poolMap sync.Map // {key:geo,val:*PoolGroup}

	poolIDMap sync.Map // {key:deviceID,val:geo}

	pendingEdgeMap      sync.Map
	pendingCandidateMap sync.Map
}

type pool struct {
	geoID            string
	edgeNodeMap      map[string]*bandwidthInfo
	candidateNodeMap map[string]*bandwidthInfo
	veriftorNodeMap  map[string]*bandwidthInfo
}

// bandwidthInfo Info
type bandwidthInfo struct {
	BandwidthUp   int64 `json:"bandwidth_up"`   // B/s
	BandwidthDown int64 `json:"bandwidth_down"` // B/s
}

// NewPoolGroup new pool
func newPoolGroup() *PoolGroup {
	poolGroup := &PoolGroup{}

	return poolGroup
}

func (n *PoolGroup) loadOrNewPool(geo string) *pool {
	p := &pool{
		geoID:            geo,
		edgeNodeMap:      make(map[string]*bandwidthInfo),
		candidateNodeMap: make(map[string]*bandwidthInfo),
		veriftorNodeMap:  make(map[string]*bandwidthInfo),
	}

	g, ok := n.poolMap.LoadOrStore(geo, p)
	if ok && g != nil {
		return g.(*pool)
	}

	return p
}

func (n *PoolGroup) loadPool(geoKey string) *pool {
	p, ok := n.poolMap.Load(geoKey)
	if ok && p != nil {
		return p.(*pool)
	}

	return nil
}

func (n *PoolGroup) storePool(geo string, val *pool) {
	n.poolMap.Store(geo, val)
}

func (n *PoolGroup) addEdgeToPool(node *EdgeNode) string {
	deviceID := node.deviceInfo.DeviceId
	geo := node.geoInfo.Geo

	oldPoolID, ok := n.poolIDMap.Load(deviceID)
	if ok && oldPoolID != nil {
		oldGeo := oldPoolID.(string)
		if oldGeo != geo {
			// remove edge with old pool
			pool := n.loadPool(oldGeo)
			if pool != nil {
				pool.removeEdge(deviceID)
			}
		} else {
			return oldGeo
		}
	}

	pool := n.loadOrNewPool(geo)

	pool.addEdge(node)

	// n.storePool(geo, pool)
	n.poolIDMap.Store(deviceID, geo)

	return geo
}

func (n *PoolGroup) addCandidateToPool(node *CandidateNode) string {
	deviceID := node.deviceInfo.DeviceId
	geo := node.geoInfo.Geo

	oldPoolID, ok := n.poolIDMap.Load(deviceID)
	if ok && oldPoolID != nil {
		oldGeo := oldPoolID.(string)
		if oldGeo != geo {
			// del candidate with old pool
			pool := n.loadPool(oldGeo)
			if pool != nil {
				pool.delCandidate(deviceID)
			}
		} else {
			return oldGeo
		}
	}

	pool := n.loadOrNewPool(geo)

	pool.addCandidate(node)

	// n.storePool(geo, pool)
	n.poolIDMap.Store(deviceID, geo)

	return geo
}

func (n *PoolGroup) addPendingNode(edgeNode *EdgeNode, candidateNode *CandidateNode) {
	if edgeNode != nil {
		n.pendingEdgeMap.Store(edgeNode.deviceInfo.DeviceId, edgeNode)
	}

	if candidateNode != nil {
		n.pendingCandidateMap.Store(candidateNode.deviceInfo.DeviceId, candidateNode)
	}
}

func (n *PoolGroup) pendingNodesToPool() {
	n.pendingEdgeMap.Range(func(key, value interface{}) bool {
		deviceID := key.(string)
		node := value.(*EdgeNode)

		n.addEdgeToPool(node)

		n.pendingEdgeMap.Delete(deviceID)

		return true
	})

	n.pendingCandidateMap.Range(func(key, value interface{}) bool {
		deviceID := key.(string)
		node := value.(*CandidateNode)

		n.addCandidateToPool(node)

		n.pendingCandidateMap.Delete(deviceID)

		return true
	})

	n.printlnPoolMap()
}

// PrintlnMap Println
func (n *PoolGroup) printlnPoolMap() {
	log.Info("poolMap--------------------------------")

	n.poolMap.Range(func(key, value interface{}) bool {
		geo := key.(string)
		p := value.(*pool)

		es := ""
		cs := ""
		vs := ""

		for s := range p.edgeNodeMap {
			es = fmt.Sprintf("%s%s,", es, s)
		}
		for s := range p.candidateNodeMap {
			cs = fmt.Sprintf("%s%s,", cs, s)
		}
		for s := range p.veriftorNodeMap {
			vs = fmt.Sprintf("%s%s,", vs, s)
		}
		log.Info("geo:", geo, ",edgeNodes:", es, ",candidateNodes:", cs, ",veriftorNodes:", vs)

		return true
	})
}

func (g *pool) setVeriftor(deviceID string) {
	if info, ok := g.candidateNodeMap[deviceID]; ok {
		g.veriftorNodeMap[deviceID] = info

		delete(g.candidateNodeMap, deviceID)

		return
	}
}

func (g *pool) resetRoles() {
	for deviceID, info := range g.veriftorNodeMap {
		g.candidateNodeMap[deviceID] = info
	}

	g.veriftorNodeMap = make(map[string]*bandwidthInfo)
}

func (g *pool) addEdge(node *EdgeNode) {
	deviceID := node.deviceInfo.DeviceId
	if _, ok := g.edgeNodeMap[deviceID]; ok {
		return
	}

	g.edgeNodeMap[deviceID] = &bandwidthInfo{BandwidthUp: node.deviceInfo.BandwidthUp, BandwidthDown: node.deviceInfo.BandwidthDown}
}

func (g *pool) addCandidate(node *CandidateNode) {
	deviceID := node.deviceInfo.DeviceId
	if _, ok := g.candidateNodeMap[deviceID]; ok {
		return
	}

	g.candidateNodeMap[deviceID] = &bandwidthInfo{BandwidthUp: node.deviceInfo.BandwidthUp, BandwidthDown: node.deviceInfo.BandwidthDown}
}

func (g *pool) removeEdge(deviceID string) {
	if _, ok := g.edgeNodeMap[deviceID]; ok {
		delete(g.edgeNodeMap, deviceID)
	}
}

func (g *pool) delCandidate(deviceID string) {
	if _, ok := g.candidateNodeMap[deviceID]; ok {
		delete(g.candidateNodeMap, deviceID)
	}
}
