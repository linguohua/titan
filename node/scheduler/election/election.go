package election

import (
	"context"
	"math"
	"sort"
	"sync"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/node/scheduler/db/cache"
	"github.com/linguohua/titan/node/scheduler/node"
)

var log = logging.Logger("election")

var reelectInterval = 30 * time.Minute // Check if the validator needs to be supplemented

// Election election
type Election struct {
	opts       EleOption
	update     chan struct{}
	manager    *node.Manager
	vlk        sync.RWMutex
	validators map[string]time.Time

	validatorRatio float64 // Ratio of validators elected 0~1
	reelectEnable  bool
}

//EleOption election option
type EleOption struct {
	ctx context.Context
	// electInterval is the time electInterval for re-election
	electInterval time.Duration
	// vExpiration is the maximum interval when the validator is offline
	vExpiration time.Duration
}

func newEleOption(opts ...Option) EleOption {
	opt := EleOption{
		ctx:           context.Background(),
		electInterval: 2 * 24 * time.Hour,
		vExpiration:   30 * time.Minute,
	}
	for _, o := range opts {
		o(&opt)
	}
	return opt
}

// Option election option
type Option func(opt *EleOption)

// func ElectIntervalOption(interval time.Duration) Option {
// 	return func(opt *EleOption) {
// 		opt.interval = interval
// 	}
// }

// func ExpirationOption(expiration time.Duration) Option {
// 	return func(opt *EleOption) {
// 		opt.expiration = expiration
// 	}
// }

// NewElection new election
func NewElection(manager *node.Manager, opts ...Option) *Election {
	ele := &Election{
		manager:        manager,
		opts:           newEleOption(opts...),
		validators:     make(map[string]time.Time),
		update:         make(chan struct{}, 1),
		validatorRatio: 1,
	}

	// open election
	go ele.electTicker()
	go ele.reelectTicker()

	return ele
}

func (v *Election) electTicker() {
	expiration, err := v.fetchCurrentValidators()
	if err != nil {
		log.Errorf("fetch current validators: %v", err)
		return
	}

	if len(v.validators) <= 0 {
		expiration = 10 * time.Minute
	}

	electTicker := time.NewTicker(expiration)
	defer electTicker.Stop()

	for {
		select {
		case <-electTicker.C:
			electTicker.Reset(v.opts.electInterval)
			err := v.elect()
			if err != nil {
				log.Errorf("elect err:%s", err.Error())
			}
		case <-v.update:
			electTicker.Reset(v.opts.electInterval)
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
	log.Info("election starting")

	validators := v.electValidators(false)
	return v.saveValidators(validators)
}

// StartElect elect
func (v *Election) StartElect() {
	v.update <- struct{}{}
}

// ChangeValidatorRatio change validator ratio
func (v *Election) ChangeValidatorRatio(f float64) {
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
		now := time.Now()
		err := cache.GetDB().UpdateBaseInfo(cache.NextElectionTimeField, now.Add(v.opts.electInterval).Unix())
		if err != nil {
			log.Error(err.Error())
		}

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
			out = append(out, candidate.DeviceId)
		}
		return
	}

	sort.Slice(candidates, func(i, j int) bool {
		//TODO Consider node reliability
		return candidates[i].BandwidthDown > candidates[j].BandwidthDown
	})

	if !isAppend {
		for i := 0; i < needValidatorCount; i++ {
			candidate := candidates[i]
			out = append(out, candidate.DeviceId)
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
			deviceID := candidate.DeviceId
			if _, exist := v.validators[deviceID]; exist {
				continue
			}

			out = append(out, candidate.DeviceId)
		}
	} else if difference < 0 {
		// need to reduce
		// TODO random
		out = out[:needValidatorCount]
	}

	return out
}

func (v *Election) saveValidators(validators []string) error {
	err := cache.GetDB().SetValidatorsToList(validators, v.opts.electInterval)
	if err != nil {
		return err
	}

	v.vlk.Lock()
	v.validators = make(map[string]time.Time)
	for _, validator := range validators {
		v.validators[validator] = time.Now()
	}
	v.vlk.Unlock()

	return nil
}

func (v *Election) fetchCurrentValidators() (time.Duration, error) {
	list, expiration, err := cache.GetDB().GetValidatorsAndExpirationTime()
	if err != nil {
		return expiration, err
	}
	v.vlk.Lock()
	for _, item := range list {
		v.validators[item] = time.Now()
	}
	v.vlk.Unlock()

	if expiration < 0 {
		expiration = v.opts.electInterval
	}

	return expiration, nil
}

func (v *Election) replenishElect() error {
	log.Infof("replenish-validators start")

	validators := v.electValidators(true)
	return v.saveValidators(validators)
}

func (v *Election) getAllCandidates() []*node.CandidateNode {
	candidates := make([]*node.CandidateNode, 0)

	v.manager.CandidateNodeMap.Range(func(key, value interface{}) bool {
		node := value.(*node.CandidateNode)
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
		select {
		case <-ticker.C:
			v.vlk.Lock()
			var expire bool
			for deviceID, _ := range v.validators {
				node := v.manager.GetCandidateNode(deviceID)
				if node != nil {
					v.validators[deviceID] = time.Now()
					continue
				}

				// if lastActive.Before(time.Now().Add(-v.opts.vExpiration)) {
				// 	expire = true
				// 	log.Infof("validator %s offline for more than %v", deviceID, v.opts.vExpiration)
				// 	delete(v.validators, deviceID)
				// }
			}
			v.vlk.Unlock()
			if !expire && !v.reelectEnable {
				continue
			}
			v.update <- struct{}{}
		}
	}
}
