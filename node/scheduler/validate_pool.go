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

func (p *ValidatePool) addPendingNode(edgeNode *EdgeNode, candidateNode *CandidateNode) {
	if edgeNode != nil {
		p.pendingEdgeMap.Store(edgeNode.deviceInfo.DeviceId, edgeNode)
	}

	if candidateNode != nil {
		p.pendingCandidateMap.Store(candidateNode.deviceInfo.DeviceId, candidateNode)
	}
}

func (p *ValidatePool) pendingNodesToPool() {
	p.pendingEdgeMap.Range(func(key, value interface{}) bool {
		deviceID := key.(string)
		node := value.(*EdgeNode)

		p.addEdge(node)

		p.pendingEdgeMap.Delete(deviceID)

		return true
	})

	p.pendingCandidateMap.Range(func(key, value interface{}) bool {
		deviceID := key.(string)
		node := value.(*CandidateNode)

		p.addCandidate(node)

		p.pendingCandidateMap.Delete(deviceID)

		return true
	})
}

func (p *ValidatePool) election(verifiedNodeMax int) ([]string, int) {
	p.resetNodePool()

	edgeNum := len(p.edgeNodeMap)
	candidateNum := len(p.candidateNodeMap)

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
	lackNum := p.generateRandomValidator(p.candidateNodeMap, needVeriftorNum)

	// reset count
	// scheduler.nodeManager.resetCandidateAndValidatorCount()
	return p.veriftorList, lackNum
}

func (p *ValidatePool) setVeriftor(deviceID string) {
	if info, ok := p.candidateNodeMap[deviceID]; ok {
		p.veriftorNodeMap[deviceID] = info

		delete(p.candidateNodeMap, deviceID)

		return
	}
}

func (p *ValidatePool) resetNodePool() {
	// p.veriftorMap = make(map[string][]string)
	p.veriftorList = make([]string, 0)

	p.resetRoles()
	p.pendingNodesToPool()
}

// func (p *ValidatePool) getNodeLists() (edgeList, candidateList []string) {
// 	edgeList = make([]string, 0)
// 	for deviceID := range p.edgeNodeMap {
// 		edgeList = append(edgeList, deviceID)
// 	}

// 	candidateList = make([]string, 0)
// 	for deviceID := range p.candidateNodeMap {
// 		candidateList = append(candidateList, deviceID)
// 	}

// 	return
// }

func (p *ValidatePool) resetRoles() {
	for deviceID, info := range p.veriftorNodeMap {
		p.candidateNodeMap[deviceID] = info
	}

	p.veriftorNodeMap = make(map[string]*CandidateNode)
}

func (p *ValidatePool) addEdge(node *EdgeNode) {
	deviceID := node.deviceInfo.DeviceId
	if _, ok := p.edgeNodeMap[deviceID]; ok {
		return
	}

	p.edgeNodeMap[deviceID] = node
}

func (p *ValidatePool) addCandidate(node *CandidateNode) {
	deviceID := node.deviceInfo.DeviceId
	if _, ok := p.candidateNodeMap[deviceID]; ok {
		return
	}

	p.candidateNodeMap[deviceID] = node
}

func (p *ValidatePool) removeEdge(deviceID string) {
	if _, ok := p.edgeNodeMap[deviceID]; ok {
		delete(p.edgeNodeMap, deviceID)
	}
}

func (p *ValidatePool) removeCandidate(deviceID string) {
	if _, ok := p.candidateNodeMap[deviceID]; ok {
		delete(p.candidateNodeMap, deviceID)
	}
}

func (p *ValidatePool) generateRandomValidator(candidateNodeMap map[string]*CandidateNode, count int) (lackNum int) {
	mLen := len(candidateNodeMap)
	lackNum = count - mLen
	if mLen <= 0 {
		return
	}

	if mLen <= count {
		for deviceID := range candidateNodeMap {
			p.addVeriftor(deviceID)
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
			p.addVeriftor(vID)
		}
	}

	return 0
}

func (p *ValidatePool) addVeriftor(deviceID string) {
	// p.veriftorMap[deviceID] = make([]string, 0)
	p.veriftorList = append(p.veriftorList, deviceID)
	p.setVeriftor(deviceID)
}
