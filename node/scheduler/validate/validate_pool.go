package validate

import (
	"github.com/linguohua/titan/node/scheduler/node"
	"math/rand"
	"sync"
	"time"
)

type ValidatePool struct {
	pendingEdgeMap      sync.Map
	pendingCandidateMap sync.Map

	edgeNodeMap      map[string]*node.EdgeNode
	candidateNodeMap map[string]*node.CandidateNode
	veriftorNodeMap  map[string]*node.CandidateNode

	// veriftorMap  map[string][]string
	veriftorList []string

	verifiedNodeMax int // verified node num limit
}

func (p *ValidatePool) addPendingNode(edgeNode *node.EdgeNode, candidateNode *node.CandidateNode) {
	if edgeNode != nil {
		p.pendingEdgeMap.Store(edgeNode.DeviceInfo.DeviceId, edgeNode)
	}

	if candidateNode != nil {
		p.pendingCandidateMap.Store(candidateNode.DeviceInfo.DeviceId, candidateNode)
	}
}

func (p *ValidatePool) pendingNodesToPool() {
	p.pendingEdgeMap.Range(func(key, value interface{}) bool {
		deviceID := key.(string)
		node := value.(*node.EdgeNode)

		p.addEdge(node)

		p.pendingEdgeMap.Delete(deviceID)

		return true
	})

	p.pendingCandidateMap.Range(func(key, value interface{}) bool {
		deviceID := key.(string)
		node := value.(*node.CandidateNode)

		p.addCandidate(node)

		p.pendingCandidateMap.Delete(deviceID)

		return true
	})
}

func (p *ValidatePool) election() ([]string, int) {
	p.resetNodePool()

	edgeNum := len(p.edgeNodeMap)
	candidateNum := len(p.candidateNodeMap)

	if edgeNum <= 0 && candidateNum <= 0 {
		return nil, 0
	}

	nodeTotalNum := edgeNum + candidateNum
	addNum := 0
	if nodeTotalNum%(p.verifiedNodeMax+1) > 0 {
		addNum = 1
	}
	needVeriftorNum := nodeTotalNum/(p.verifiedNodeMax+1) + addNum

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

func (p *ValidatePool) resetRoles() {
	for deviceID, info := range p.veriftorNodeMap {
		p.candidateNodeMap[deviceID] = info
	}

	p.veriftorNodeMap = make(map[string]*node.CandidateNode)
}

func (p *ValidatePool) addEdge(node *node.EdgeNode) {
	deviceID := node.DeviceInfo.DeviceId
	if _, ok := p.edgeNodeMap[deviceID]; ok {
		return
	}

	p.edgeNodeMap[deviceID] = node
}

func (p *ValidatePool) addCandidate(node *node.CandidateNode) {
	deviceID := node.DeviceInfo.DeviceId
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

	// if _, ok := p.veriftorNodeMap[deviceID]; ok {
	// 	delete(p.veriftorNodeMap, deviceID)
	// }
}

func (p *ValidatePool) generateRandomValidator(candidateNodeMap map[string]*node.CandidateNode, count int) (lackNum int) {
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
