package node

import "math/rand"

// assigns an undistributed candidate select code to a node ID and returns the assigned code
func (m *Manager) distributeCandidateSelectCode(nodeID string) int {
	m.selectCodeLock.Lock()
	defer m.selectCodeLock.Unlock()

	var code int
	if len(m.cUndistributedSelectCode) > 0 {
		for c := range m.cUndistributedSelectCode {
			code = c
			break
		}

		if _, exist := m.cDistributedSelectCode[code]; !exist {
			m.cDistributedSelectCode[code] = nodeID
			delete(m.cUndistributedSelectCode, code)
			return code
		}
	}

	m.cPullSelectCode++

	code = m.cPullSelectCode
	m.cDistributedSelectCode[code] = nodeID
	return code
}

// assigns an undistributed edge select code to a node ID and returns the assigned code
func (m *Manager) distributeEdgeSelectCode(nodeID string) int {
	m.selectCodeLock.Lock()
	defer m.selectCodeLock.Unlock()

	var code int
	if len(m.eUndistributedSelectCode) > 0 {
		for c := range m.eUndistributedSelectCode {
			code = c
			break
		}

		if _, exist := m.eDistributedSelectCode[code]; !exist {
			m.eDistributedSelectCode[code] = nodeID
			delete(m.eUndistributedSelectCode, code)
			return code
		}
	}

	m.ePullSelectCode++

	code = m.ePullSelectCode
	m.eDistributedSelectCode[code] = nodeID
	return code
}

// returns an undistributed candidate select code to the pool
func (m *Manager) repayCandidateSelectCode(code int) {
	m.selectCodeLock.Lock()
	defer m.selectCodeLock.Unlock()

	delete(m.cDistributedSelectCode, code)
	m.cUndistributedSelectCode[code] = ""
}

// returns an undistributed edge select code to the pool
func (m *Manager) repayEdgeSelectCode(code int) {
	m.selectCodeLock.Lock()
	defer m.selectCodeLock.Unlock()

	delete(m.eDistributedSelectCode, code)
	m.eUndistributedSelectCode[code] = ""
}

// returns a random integer up to max (inclusive) using the provided Rand generator
func (m *Manager) getSelectCodeRandom(max int, r *rand.Rand) int {
	max = max + 1
	return r.Intn(max)
}

// GetRandomCandidate returns a random candidate node
func (m *Manager) GetRandomCandidate() *Node {
	selectCode := m.getSelectCodeRandom(m.cPullSelectCode, m.cPullSelectRand)
	nodeID, exist := m.cDistributedSelectCode[selectCode]
	if !exist {
		return nil
	}

	return m.GetCandidateNode(nodeID)
}

// GetRandomEdge returns a random edge node
func (m *Manager) GetRandomEdge() *Node {
	selectCode := m.getSelectCodeRandom(m.ePullSelectCode, m.ePullSelectRand)
	nodeID, exist := m.eDistributedSelectCode[selectCode]
	if !exist {
		return nil
	}

	return m.GetEdgeNode(nodeID)
}
