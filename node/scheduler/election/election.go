package election

import (
	"context"
	"math"
	"sort"
	"sync"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/node/scheduler/node"
)

var log = logging.Logger("election")

var (
	reelectInterval = 30 * time.Minute   // Re-elect interval
	electInterval   = 2 * 24 * time.Hour // Elect cycle
)

// Election election
type Election struct {
	opts       EleOption
	updateCh   chan struct{}
	nodeMgr    *node.Manager
	lock       sync.RWMutex
	validators map[string]struct{}

	validatorRatio float64 // Ratio of validators elected 0~1
	reelectEnable  bool
}

// EleOption election option
type EleOption struct {
	ctx context.Context
}

func newEleOption(opts ...Option) EleOption {
	opt := EleOption{
		ctx: context.Background(),
	}
	for _, o := range opts {
		o(&opt)
	}
	return opt
}

// Option election option
type Option func(opt *EleOption)

// NewElection return new election instance
func NewElection(manager *node.Manager) *Election {
	ele := &Election{
		nodeMgr:        manager,
		opts:           newEleOption(),
		validators:     make(map[string]struct{}),
		updateCh:       make(chan struct{}, 1),
		validatorRatio: 1,
	}

	go ele.electTicker()
	go ele.reelectTicker()

	return ele
}

func (v *Election) electTicker() {
	err := v.fetchCurrentValidators()
	if err != nil {
		log.Errorf("fetch current validators: %v", err)
		return
	}

	expiration := electInterval
	if len(v.validators) <= 0 {
		expiration = 10 * time.Minute
	}

	electTicker := time.NewTicker(expiration)
	defer electTicker.Stop()

	for {
		select {
		case <-electTicker.C:
			electTicker.Reset(electInterval)
			err := v.elect()
			if err != nil {
				log.Errorf("elect err:%s", err.Error())
			}
		case <-v.updateCh:
			electTicker.Reset(electInterval)
			err := v.replenishElect()
			if err != nil {
				log.Errorf("replenishValidators err:%s", err.Error())
			}
		case <-v.opts.ctx.Done():
			return
		}
	}
}

func (v *Election) elect() error {
	log.Debugln("start elect ")
	validators := v.electValidators(false)
	return v.saveValidators(validators)
}

// StartElect elect
func (v *Election) StartElect() {
	v.updateCh <- struct{}{}
}

// UpdateValidatorRatio update validator ratio
func (v *Election) UpdateValidatorRatio(f float64) {
	if f < 0 {
		return
	}

	if f != v.validatorRatio {
		v.validatorRatio = f
		v.reelectEnable = true
	}
}

func (v *Election) electValidators(isAppend bool) (out []string) {
	out = make([]string, 0)

	defer func() {
		// TODO save NextElectionTime
		log.Infof("elect validators count: %d", len(out))

		v.reelectEnable = false
	}()

	candidates := v.getAllCandidates()
	candidateCount := len(candidates)

	needValidatorCount := int(math.Ceil(float64(candidateCount) * v.validatorRatio))
	if needValidatorCount <= 0 {
		return
	}

	if needValidatorCount >= candidateCount {
		for _, candidate := range candidates {
			out = append(out, candidate.NodeID)
		}
		return
	}

	sort.Slice(candidates, func(i, j int) bool {
		// TODO Consider node reliability
		return candidates[i].BandwidthDown > candidates[j].BandwidthDown
	})

	if !isAppend {
		for i := 0; i < needValidatorCount; i++ {
			candidate := candidates[i]
			out = append(out, candidate.NodeID)
		}

		return out
	}

	for k := range v.validators {
		out = append(out, k)
	}

	difference := needValidatorCount - len(v.validators)
	if difference > 0 {
		// need to add
		for i := 0; i < needValidatorCount; i++ {
			candidate := candidates[i]
			nodeID := candidate.NodeID
			if _, exist := v.validators[nodeID]; exist {
				continue
			}

			out = append(out, candidate.NodeID)
		}
	} else if difference < 0 {
		// need to reduce
		out = out[:needValidatorCount]
	}

	return out
}

func (v *Election) saveValidators(validators []string) error {
	err := v.nodeMgr.UpdateValidators(validators, v.nodeMgr.ServerID)
	if err != nil {
		return err
	}

	v.lock.Lock()
	v.validators = make(map[string]struct{})
	for _, validator := range validators {
		v.validators[validator] = time.Now()
	}
	v.lock.Unlock()

	return nil
}

func (v *Election) fetchCurrentValidators() error {
	list, err := v.nodeMgr.LoadValidators(v.nodeMgr.ServerID)
	if err != nil {
		return err
	}

	v.lock.Lock()
	defer v.lock.Unlock()

	for _, item := range list {
		v.validators[item] = time.Now()
	}

	return nil
}

func (v *Election) replenishElect() error {
	log.Infof("replenish-validators start")

	validators := v.electValidators(true)
	return v.saveValidators(validators)
}

func (v *Election) getAllCandidates() []*node.Candidate {
	candidates := make([]*node.Candidate, 0)

	v.nodeMgr.CandidateNodes.Range(func(key, value interface{}) bool {
		node := value.(*node.Candidate)
		candidates = append(candidates, node)

		return true
	})

	return candidates
}

// if the validator node goes offline (for a long time), it is necessary to re-elect a new validator.
// if the number of validators required changes,it is necessary to re-elect a new validator.
func (v *Election) reelectTicker() {
	ticker := time.NewTicker(reelectInterval)
	defer ticker.Stop()

	for {
		<-ticker.C
		v.lock.Lock()
		var validatorOffline bool
		for nodeID := range v.validators {
			node := v.nodeMgr.GetCandidateNode(nodeID)
			if node != nil {
				v.validators[nodeID] = time.Now()
				continue
			}
			validatorOffline = true
		}
		v.lock.Unlock()
		if !validatorOffline && !v.reelectEnable {
			continue
		}
		v.updateCh <- struct{}{}
	}
}
