package scheduler

import (
	"math/rand"
	"sync"
	"time"
)

// ValidatePool validate pool
type ValidatePool struct {
	pendingEdgeMap      sync.Map
	pendingCandidateMap sync.Map

	edgeNodeMap      map[string]*EdgeNode
	candidateNodeMap map[string]*CandidateNode
	veriftorNodeMap  map[string]*CandidateNode

	// veriftorMap  map[string][]string
	veriftorList []string
}

// // bandwidthInfo Info
// type bandwidthInfo struct {
// 	BandwidthUp   int64 `json:"bandwidth_up"`   // B/s
// 	BandwidthDown int64 `json:"bandwidth_down"` // B/s
// }

func newValidatePool() *ValidatePool {
	pool := &ValidatePool{
		edgeNodeMap:      make(map[string]*EdgeNode),
		candidateNodeMap: make(map[string]*CandidateNode),
		veriftorNodeMap:  make(map[string]*CandidateNode),
	}

	return pool
}

func (g *ValidatePool) addPendingNode(edgeNode *EdgeNode, candidateNode *CandidateNode) {
	if edgeNode != nil {
		g.pendingEdgeMap.Store(edgeNode.deviceInfo.DeviceId, edgeNode)
	}

	if candidateNode != nil {
		g.pendingCandidateMap.Store(candidateNode.deviceInfo.DeviceId, candidateNode)
	}
}

func (g *ValidatePool) pendingNodesToPool() {
	g.pendingEdgeMap.Range(func(key, value interface{}) bool {
		deviceID := key.(string)
		node := value.(*EdgeNode)

		g.addEdge(node)

		g.pendingEdgeMap.Delete(deviceID)

		return true
	})

	g.pendingCandidateMap.Range(func(key, value interface{}) bool {
		deviceID := key.(string)
		node := value.(*CandidateNode)

		g.addCandidate(node)

		g.pendingCandidateMap.Delete(deviceID)

		return true
	})
}

func (g *ValidatePool) election(verifiedNodeMax int) ([]string, int) {
	g.resetNodePool()

	edgeNum := len(g.edgeNodeMap)
	candidateNum := len(g.candidateNodeMap)

	if edgeNum <= 0 && candidateNum <= 0 {
		return nil, 0
	}

	nodeTotalNum := edgeNum + candidateNum
	addNum := 0
	if nodeTotalNum%(verifiedNodeMax+1) > 0 {
		addNum = 1
	}
	needVeriftorNum := nodeTotalNum/(verifiedNodeMax+1) + addNum

	// rand election
	lackNum := g.generateRandomValidator(g.candidateNodeMap, needVeriftorNum)

	// reset count
	// scheduler.nodeManager.resetCandidateAndValidatorCount()
	return g.veriftorList, lackNum
}

func (g *ValidatePool) setVeriftor(deviceID string) {
	if info, ok := g.candidateNodeMap[deviceID]; ok {
		g.veriftorNodeMap[deviceID] = info

		delete(g.candidateNodeMap, deviceID)

		return
	}
}

func (g *ValidatePool) resetNodePool() {
	// g.veriftorMap = make(map[string][]string)
	g.veriftorList = make([]string, 0)

	g.resetRoles()
	g.pendingNodesToPool()
}

// func (g *ValidatePool) getNodeLists() (edgeList, candidateList []string) {
// 	edgeList = make([]string, 0)
// 	for deviceID := range g.edgeNodeMap {
// 		edgeList = append(edgeList, deviceID)
// 	}

// 	candidateList = make([]string, 0)
// 	for deviceID := range g.candidateNodeMap {
// 		candidateList = append(candidateList, deviceID)
// 	}

// 	return
// }

func (g *ValidatePool) resetRoles() {
	for deviceID, info := range g.veriftorNodeMap {
		g.candidateNodeMap[deviceID] = info
	}

	g.veriftorNodeMap = make(map[string]*CandidateNode)
}

func (g *ValidatePool) addEdge(node *EdgeNode) {
	deviceID := node.deviceInfo.DeviceId
	if _, ok := g.edgeNodeMap[deviceID]; ok {
		return
	}

	g.edgeNodeMap[deviceID] = node
}

func (g *ValidatePool) addCandidate(node *CandidateNode) {
	deviceID := node.deviceInfo.DeviceId
	if _, ok := g.candidateNodeMap[deviceID]; ok {
		return
	}

	g.candidateNodeMap[deviceID] = node
}

func (g *ValidatePool) removeEdge(deviceID string) {
	if _, ok := g.edgeNodeMap[deviceID]; ok {
		delete(g.edgeNodeMap, deviceID)
	}
}

func (g *ValidatePool) removeCandidate(deviceID string) {
	if _, ok := g.candidateNodeMap[deviceID]; ok {
		delete(g.candidateNodeMap, deviceID)
	}
}

func (g *ValidatePool) generateRandomValidator(candidateNodeMap map[string]*CandidateNode, count int) (lackNum int) {
	mLen := len(candidateNodeMap)
	lackNum = count - mLen
	if mLen <= 0 {
		return
	}

	if mLen <= count {
		for deviceID := range candidateNodeMap {
			g.addVeriftor(deviceID)
		}
		return
	}

	cList := make([]string, 0)
	for deviceID := range candidateNodeMap {
		cList = append(cList, deviceID)
	}

	veriftorMap := make(map[string]int)

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for len(veriftorMap) < count {
		num := r.Intn(mLen)
		exist := false

		vID := cList[num]
		if _, ok := veriftorMap[vID]; ok {
			exist = true
			continue
		}

		if !exist {
			veriftorMap[vID] = num
			g.addVeriftor(vID)
		}
	}

	return 0
}

func (g *ValidatePool) addVeriftor(deviceID string) {
	// g.veriftorMap[deviceID] = make([]string, 0)
	g.veriftorList = append(g.veriftorList, deviceID)
	g.setVeriftor(deviceID)
}
