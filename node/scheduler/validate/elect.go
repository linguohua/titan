package validate

import (
	"math"
	"math/rand"
	"time"
)

var (
	firstElectInterval = 5 * time.Minute    // Time of the first election
	electionCycle      = 5 * 24 * time.Hour // election cycle
)

func (m *Manager) electTicker() {
	validators, err := m.nodeMgr.LoadValidators(m.nodeMgr.ServerID)
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

func (m *Manager) elect() error {
	log.Debugln("start elect ")
	validators := m.electValidators()

	m.ResetValidatorGroup(validators)

	return m.nodeMgr.UpdateValidators(validators, m.nodeMgr.ServerID)
}

// StartElect elect
func (m *Manager) StartElect() {
	m.updateCh <- struct{}{}
}

func (m *Manager) getValidatorRatio() float64 {
	cfg, err := m.config()
	if err != nil {
		log.Errorf("schedulerConfig err:%s", err.Error())
		return 0
	}

	return cfg.ValidatorRatio
}

// electValidators elect
func (m *Manager) electValidators() (out []string) {
	ratio := m.getValidatorRatio()

	out = m.nodeMgr.GetAllCandidates()

	needValidatorCount := int(math.Ceil(float64(len(out)) * ratio))
	if needValidatorCount <= 0 {
		return
	}

	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(out), func(i, j int) {
		out[i], out[j] = out[j], out[i]
	})

	if needValidatorCount > len(out) {
		needValidatorCount = len(out)
	}

	out = out[:needValidatorCount]

	return
}
