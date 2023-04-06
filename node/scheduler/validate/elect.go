package validate

import (
	"math"
	"math/rand"
	"time"
)

var (
	firstElectInterval = 5 * time.Minute    // Time of the first election
	electionCycle      = 5 * 24 * time.Hour // Length of the election cycle
)

// triggers the election process at a regular interval.
func (m *Manager) electTicker() {
	validators, err := m.nodeMgr.FetchValidators(m.nodeMgr.ServerID)
	if err != nil {
		log.Errorf("fetch current validators: %v", err)
		return
	}

	expiration := electionCycle
	if len(validators) <= 0 {
		expiration = firstElectInterval
	}

	electTicker := time.NewTicker(expiration)
	defer electTicker.Stop()

	doElect := func() {
		electTicker.Reset(electionCycle)
		err := m.elect()
		if err != nil {
			log.Errorf("elect err:%s", err.Error())
		}
	}

	for {
		select {
		case <-electTicker.C:
			doElect()
		case <-m.updateCh:
			doElect()
		}
	}
}

// elect triggers an election and updates the list of validators.
func (m *Manager) elect() error {
	log.Debugln("start elect ")
	validators := m.electValidators()

	m.ResetValidatorGroup(validators)

	return m.nodeMgr.UpdateValidators(validators, m.nodeMgr.ServerID)
}

// StartElection triggers an election manually.
func (m *Manager) StartElection() {
	m.updateCh <- struct{}{}
}

// returns the ratio of validators that should be elected, based on the scheduler configuration.
func (m *Manager) getValidatorRatio() float64 {
	cfg, err := m.config()
	if err != nil {
		log.Errorf("schedulerConfig err:%s", err.Error())
		return 0
	}

	return cfg.ValidatorRatio
}

// performs the election process and returns the list of elected validators.
func (m *Manager) electValidators() []string {
	ratio := m.getValidatorRatio()

	list := m.nodeMgr.GetAllCandidateNodes()

	needValidatorCount := int(math.Ceil(float64(len(list)) * ratio))
	if needValidatorCount <= 0 {
		return nil
	}

	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(list), func(i, j int) {
		list[i], list[j] = list[j], list[i]
	})

	if needValidatorCount > len(list) {
		needValidatorCount = len(list)
	}

	list = list[:needValidatorCount]

	return list
}
