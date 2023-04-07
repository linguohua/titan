package validate

import (
	"math"
	"math/rand"
	"time"

	"github.com/docker/go-units"
)

// reduces the beValidateGroup's bandwidth to an value between the min and max averages
func (b *BeValidateGroup) divideBeValidateToAverage(maxAverage, minAverage float64) (out map[string]float64) {
	out = make(map[string]float64)

	b.lock.Lock()
	defer func() {
		for nodeID, bUp := range out {
			delete(b.beValidates, nodeID)
			b.sumBwUp -= bUp
		}

		b.lock.Unlock()
	}()

	if b.sumBwUp < maxAverage || len(b.beValidates) <= 1 {
		return
	}

	maxReduce := b.sumBwUp - minAverage
	minReduce := b.sumBwUp - maxAverage

	tempBwUp := float64(0)
	for nodeID, bwUp := range b.beValidates {
		if bwUp > maxReduce {
			continue
		}

		if bwUp >= minReduce {
			out[nodeID] = bwUp
			return
		}

		out[nodeID] = bwUp
		tempBwUp += bwUp
		if tempBwUp >= minReduce && tempBwUp <= maxReduce {
			return
		}

		if tempBwUp > maxReduce {
			// TODO too much will be reduce here
			return
		}
	}

	out = make(map[string]float64)
	return
}

// adds a beValidate node to the beValidateGroup
func (b *BeValidateGroup) addBeValidate(nodeID string, bwUp float64) {
	b.lock.Lock()
	defer b.lock.Unlock()

	_, exist := b.beValidates[nodeID]
	if exist {
		return
	}

	b.beValidates[nodeID] = bwUp
	b.sumBwUp += bwUp
}

// adds multiple beValidate nodes to the beValidateGroup
func (b *BeValidateGroup) addBeValidates(groups map[string]float64) {
	b.lock.Lock()
	defer b.lock.Unlock()

	for nodeID, bwUp := range groups {
		_, exist := b.beValidates[nodeID]
		if exist {
			continue
		}

		b.beValidates[nodeID] = bwUp
		b.sumBwUp += bwUp
	}
}

// removes a beValidate node from the beValidateGroup
func (b *BeValidateGroup) removeBeValidate(nodeID string) {
	b.lock.Lock()
	defer b.lock.Unlock()

	bwUp, exist := b.beValidates[nodeID]
	if !exist {
		return
	}

	delete(b.beValidates, nodeID)
	b.sumBwUp -= bwUp
}

// ResetValidatorGroup clears and initializes the validator and BeValidate groups
func (m *Manager) ResetValidatorGroup(nodeIDs []string) {
	m.validatePairLock.Lock()
	defer m.validatePairLock.Unlock()

	// remove old
	for _, group := range m.beValidateGroups {
		m.unpairedGroup.addBeValidates(group.beValidates)
	}

	// init
	m.beValidateGroups = make([]*BeValidateGroup, 0)
	m.vWindows = make([]*VWindow, 0)

	for _, nodeID := range nodeIDs {
		node := m.nodeMgr.GetCandidateNode(nodeID)
		bwDn := node.BandwidthDown
		count := int(math.Floor((bwDn * bandwidthRatio) / m.getValidatorBaseBwDn()))
		log.Debugf("addValidator %s ,bandwidthDown:%.2f, count:%d", nodeID, bwDn, count)
		if count < 1 {
			continue
		}

		for i := 0; i < count; i++ {
			vr := newVWindow(nodeID)
			m.vWindows = append(m.vWindows, vr)

			bg := newBeValidateGroup()
			m.beValidateGroups = append(m.beValidateGroups, bg)
		}
	}
}

// adds a validator window to the manager with the specified node ID and bandwidth down
func (m *Manager) addValidator(nodeID string, bwDn float64) {
	m.validatePairLock.Lock()
	defer m.validatePairLock.Unlock()

	count := int(math.Floor((bwDn * bandwidthRatio) / m.getValidatorBaseBwDn()))
	log.Debugf("addValidator %s ,bandwidthDown:%.2f, count:%d", nodeID, bwDn, count)
	if count < 1 {
		return
	}

	// Do not process if node present
	for _, v := range m.vWindows {
		if v.NodeID == nodeID {
			return
		}
	}

	for i := 0; i < count; i++ {
		vr := newVWindow(nodeID)
		m.vWindows = append(m.vWindows, vr)

		bg := newBeValidateGroup()
		m.beValidateGroups = append(m.beValidateGroups, bg)
	}
}

// removes the validator window with the specified node ID from the manager
func (m *Manager) removeValidator(nodeID string) {
	m.validatePairLock.Lock()
	defer m.validatePairLock.Unlock()

	var indexes []int
	for i, v := range m.vWindows {
		if v.NodeID == nodeID {
			indexes = append(indexes, i)
		}
	}

	if len(indexes) == 0 {
		return
	}

	// update validator windows
	start := indexes[0]
	end := indexes[len(indexes)-1] + 1 // does not contain end , need to ++

	s1 := m.vWindows[:start]
	s2 := m.vWindows[end:]

	m.vWindows = append(s1, s2...)

	// update beValidateGroups
	rIndex := len(m.beValidateGroups) - len(indexes)
	removeGroups := m.beValidateGroups[rIndex:]

	m.beValidateGroups = m.beValidateGroups[:rIndex]

	// add be validate node to unpairedGroup
	for _, group := range removeGroups {
		m.unpairedGroup.addBeValidates(group.beValidates)
	}
}

// adds a be-validate node to unpairedGroup with the specified node ID and bandwidth up
func (m *Manager) addBeValidate(nodeID string, bandwidthUp float64) {
	m.unpairedGroup.addBeValidate(nodeID, bandwidthUp)
}

// removes the be-validate node with the specified node ID from the manager
func (m *Manager) removeBeValidate(nodeID string) {
	m.validatePairLock.Lock()
	defer m.validatePairLock.Unlock()

	if _, exist := m.unpairedGroup.beValidates[nodeID]; exist {
		m.unpairedGroup.removeBeValidate(nodeID)
	}

	for _, bg := range m.beValidateGroups {
		bwUp, exist := bg.beValidates[nodeID]
		if exist {
			bg.sumBwUp -= bwUp
			delete(bg.beValidates, nodeID)
			return
		}
	}
}

// divides the be-validate nodes in the manager into groups with similar average bandwidth
func (m *Manager) divideIntoGroups() {
	m.validatePairLock.Lock()
	defer m.validatePairLock.Unlock()

	sumBwUp := m.unpairedGroup.sumBwUp
	groupCount := len(m.beValidateGroups)
	for _, group := range m.beValidateGroups {
		sumBwUp += group.sumBwUp
	}

	averageUp := sumBwUp / float64(groupCount)
	maxAverage := averageUp + toleranceBwUp
	minAverage := averageUp - toleranceBwUp

	log.Debugf("sumUp:%.2f groupCount:%d averageUp:%.2f  %.2f ~ %.2f \n", sumBwUp, groupCount, averageUp, minAverage, maxAverage)
	for _, group := range m.beValidateGroups {
		rm := group.divideBeValidateToAverage(maxAverage, minAverage)
		if len(rm) > 0 {
			m.unpairedGroup.addBeValidates(rm)
		}
	}

	log.Debugf("divideIntoGroups size:%d , start %s \n", len(m.unpairedGroup.beValidates), time.Now().String())
	// O n+m (n is the group count, m is the beValidate node count)
	for _, groups := range m.beValidateGroups {
		if groups.sumBwUp >= maxAverage {
			continue
		}

		for nodeID, bwUp := range m.unpairedGroup.beValidates {
			if bwUp > maxAverage || groups.sumBwUp+bwUp <= maxAverage {
				groups.addBeValidate(nodeID, bwUp)
				m.unpairedGroup.removeBeValidate(nodeID)
			}

			if groups.sumBwUp >= maxAverage {
				break
			}
		}

		if len(m.unpairedGroup.beValidates) == 0 {
			break
		}

	}
	log.Debugf("divideIntoGroups size:%d , end %s \n", len(m.unpairedGroup.beValidates), time.Now().String())
}

// PairValidatorsAndBeValidates randomly pair validators and beValidates based on their bandwidth capabilities.
func (m *Manager) PairValidatorsAndBeValidates() []*VWindow {
	log.Debugf("PairValidatorsAndBeValidates start %s \n", time.Now().String())
	m.divideIntoGroups()
	log.Debugf("PairValidatorsAndBeValidates end %s \n", time.Now().String())

	vs := len(m.vWindows)
	bs := len(m.beValidateGroups)
	if vs != bs {
		log.Errorf("group len are not the same vs:%d,bs:%d", vs, bs)
		return nil
	}

	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(m.beValidateGroups), func(i, j int) {
		m.beValidateGroups[i], m.beValidateGroups[j] = m.beValidateGroups[j], m.beValidateGroups[i]
	})

	for i, v := range m.vWindows {
		groups := m.beValidateGroups[i]

		v.BeValidates = groups.beValidates
	}

	return m.vWindows
}

func (m *Manager) getValidatorBaseBwDn() float64 {
	cfg, err := m.config()
	if err != nil {
		log.Errorf("schedulerConfig err:%s", err.Error())
		return 0
	}

	return float64(cfg.ValidatorBaseBwDn * units.MiB)
}
