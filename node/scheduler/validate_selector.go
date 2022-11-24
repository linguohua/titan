package scheduler

import (
	"context"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/linguohua/titan/node/scheduler/db/cache"
)

var HeartbeatInterval = 1 * time.Minute

type ValidateSelector struct {
	opts       ValidateOptions
	connect    chan string
	update     chan struct{}
	manage     *NodeManager
	vlk        sync.RWMutex
	validators map[string]time.Time

	startTime time.Time
}

type ValidateOptions struct {
	ctx context.Context
	// interval is the time interval for re-election
	interval time.Duration
	// expiration is the maximum interval when the validator is offline
	expiration time.Duration
}

func newValidateOption(opts ...Option) ValidateOptions {
	opt := ValidateOptions{
		ctx:        context.Background(),
		interval:   2 * 24 * time.Hour,
		expiration: 30 * time.Minute,
	}
	for _, o := range opts {
		o(&opt)
	}
	return opt
}

type Option func(opt *ValidateOptions)

func ElectIntervalOption(interval time.Duration) Option {
	return func(opt *ValidateOptions) {
		opt.interval = interval
	}
}

func ExpirationOption(expiration time.Duration) Option {
	return func(opt *ValidateOptions) {
		opt.expiration = expiration
	}
}

func newValidateSelector(manage *NodeManager, opts ...Option) *ValidateSelector {
	return &ValidateSelector{
		manage:     manage,
		opts:       newValidateOption(opts...),
		validators: make(map[string]time.Time),
		connect:    make(chan string, 1),
		update:     make(chan struct{}, 1),
	}
}

func (v *ValidateSelector) Run() {
	go v.checkValidatorExpiration()
	if err := v.fetchCurrentValidators(); err != nil {
		log.Errorf("fetch current validators: %v", err)
	}

	ticker := time.NewTicker(v.opts.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			v.elect()
			ticker.Reset(v.opts.interval)
		case id := <-v.connect:
			v.deviceConnect(id)
		case <-v.update:
			v.updateValidators()
		case <-v.opts.ctx.Done():
			return
		}
	}
}

func (v *ValidateSelector) elect() error {
	v.startTime = time.Now()

	log.Info("election starting")
	defer func() {
		log.Infof("election completed, cost: %v", time.Since(v.startTime))
	}()

	winners, err := v.winner(false)
	if err != nil {
		log.Errorf("winner: %v", err)
		return err
	}

	return v.saveWinners(winners)
}

func (v *ValidateSelector) winner(isAppend bool) ([]*CandidateNode, error) {
	var out []*CandidateNode
	defer func() {
		log.Infof("election winners count: %d", len(out))
	}()

	if !v.sufficientCandidates() {
		out = append(out, v.getAllCandidates()...)
		return out, nil
	}

	candidates := v.getAllCandidates()
	allNodes := v.getAllNode()
	sort.Slice(allNodes, func(i, j int) bool {
		return allNodes[i].deviceInfo.BandwidthUp > allNodes[j].deviceInfo.BandwidthUp
	})

	if isAppend && len(v.validators) > 0 {
		var currentValidators []*CandidateNode
		var excludeValidators []*CandidateNode
		for _, candidate := range candidates {
			v.vlk.RLock()
			_, ok := v.validators[candidate.deviceInfo.DeviceId]
			v.vlk.RUnlock()
			if !ok {
				excludeValidators = append(excludeValidators, candidate)
				continue
			}
			currentValidators = append(currentValidators, candidate)
		}

		chosen, remains := chooseCandidates(currentValidators, allNodes)
		out = append(out, chosen...)
		allNodes = remains
		candidates = excludeValidators
	}

	if len(allNodes) == 0 {
		return out, nil
	}

	rand.Shuffle(len(candidates), func(i, j int) {
		candidates[i], candidates[j] = candidates[j], candidates[i]
	})
	chosen, _ := chooseCandidates(candidates, allNodes)
	out = append(out, chosen...)

	return out, nil
}

func chooseCandidates(candidates []*CandidateNode, allNodes []*Node) (chosen []*CandidateNode, remains []*Node) {
	if len(allNodes) == 0 {
		return
	}
	minBandwidthUp := allNodes[len(allNodes)-1].deviceInfo.BandwidthUp
	for _, candidate := range candidates {
		bandwidth := candidate.deviceInfo.BandwidthDown
		var skips []*Node
		for i := 0; i < len(allNodes); i++ {
			if candidate.deviceInfo.BandwidthDown < allNodes[i].deviceInfo.BandwidthUp {
				skips = append(skips, allNodes[i])
				continue
			}
			if bandwidth < allNodes[i].deviceInfo.BandwidthUp {
				skips = append(skips, allNodes[i])
				continue
			}
			if bandwidth < minBandwidthUp {
				skips = append(skips, allNodes[i:]...)
				break
			}
			bandwidth = bandwidth - allNodes[i].deviceInfo.BandwidthUp
		}
		if bandwidth < candidate.deviceInfo.BandwidthDown {
			chosen = append(chosen, candidate)
		}
		if len(skips) == 0 {
			break
		}
		allNodes = skips
		remains = allNodes
	}
	return
}

func (v *ValidateSelector) saveWinners(winners []*CandidateNode) error {
	err := cache.GetDB().RemoveValidatorList()
	if err != nil {
		return err
	}

	v.validators = make(map[string]time.Time)
	for _, winner := range winners {
		v.vlk.Lock()
		v.validators[winner.deviceInfo.DeviceId] = time.Now()
		v.vlk.Unlock()

		err := cache.GetDB().SetValidatorToList(winner.deviceInfo.DeviceId)
		if err != nil {
			log.Errorf("SetValidatorToList err : %s", err.Error())
		}
	}

	return nil
}

func (v *ValidateSelector) fetchCurrentValidators() error {
	validators, err := cache.GetDB().GetValidatorsWithList()
	if err != nil {
		return err
	}
	v.vlk.Lock()
	for _, item := range validators {
		v.validators[item] = time.Now()
	}
	v.vlk.Unlock()
	return nil
}

// sufficientCandidates the total download bandwidth of all candidate nodes must be greater than the total upload bandwidth of all nodes
func (v *ValidateSelector) sufficientCandidates() bool {
	var candidateDwnBdw, needUpBdw float64

	for _, candidate := range v.getAllCandidates() {
		candidateDwnBdw += candidate.deviceInfo.BandwidthDown
	}

	for _, node := range v.getAllNode() {
		needUpBdw += node.deviceInfo.BandwidthUp
	}

	if candidateDwnBdw < needUpBdw {
		log.Error("candidates insufficient upload bandwidth")
		return false
	}
	return true
}

// deviceConnect when a batch of edge nodes goes online, the number of validators may need to be recalculated.
func (v *ValidateSelector) deviceConnect(deviceID string) error {
	log.Infof("device connect: %s", deviceID)
	v.vlk.Lock()
	_, ok := v.validators[deviceID]
	if ok {
		v.validators[deviceID] = time.Now()
	}
	v.vlk.Unlock()

	if !ok {
		return v.maybeReElect()
	}
	return nil
}

func (v *ValidateSelector) maybeReElect() error {
	var validators []*CandidateNode
	candidates := v.getAllCandidates()
	allNodes := v.getAllNode()

	v.vlk.RLock()
	for _, candidate := range candidates {
		if _, ok := v.validators[candidate.deviceInfo.DeviceId]; ok {
			validators = append(validators, candidate)
		}
	}
	v.vlk.RUnlock()

	_, remains := chooseCandidates(validators, allNodes)
	if len(remains) == 0 && len(validators) > 0 {
		return nil
	}

	v.update <- struct{}{}
	return nil
}

func (v *ValidateSelector) updateValidators() error {
	start := time.Now()

	log.Infof("re-election start")
	defer func() {
		log.Infof("re-election cost: %v", time.Since(start))
	}()

	winners, err := v.winner(true)
	if err != nil {
		return err
	}

	return v.saveWinners(winners)
}

func (v *ValidateSelector) NodeConnect(deviceID string) {
	v.connect <- deviceID
}

func (v *ValidateSelector) getAllCandidates() []*CandidateNode {
	var candidates []*CandidateNode
	v.manage.candidateNodeMap.Range(func(key, value interface{}) bool {
		candidates = append(candidates, value.(*CandidateNode))
		return true
	})
	return candidates
}

func (v *ValidateSelector) getAllNode() []*Node {
	var nodes []*Node
	v.manage.edgeNodeMap.Range(func(key, value interface{}) bool {
		nodes = append(nodes, &value.(*EdgeNode).Node)
		return true
	})
	v.manage.candidateNodeMap.Range(func(key, value interface{}) bool {
		nodes = append(nodes, &value.(*CandidateNode).Node)
		return true
	})

	return nodes
}

// checkValidatorExpiration if the validator node goes offline (for a long time), it is necessary to re-elect a new validator.
// But the edge node doesn't need to do anything.
func (v *ValidateSelector) checkValidatorExpiration() {
	ticker := time.NewTicker(HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			v.vlk.Lock()
			var expire bool
			for deviceID, lastActive := range v.validators {
				_, ok := v.manage.candidateNodeMap.Load(deviceID)
				if ok {
					v.validators[deviceID] = time.Now()
					continue
				}
				if lastActive.Before(time.Now().Add(-v.opts.expiration)) {
					expire = true
					log.Infof("validator %s offline for more than %v", deviceID, v.opts.expiration)
					delete(v.validators, deviceID)
				}
			}
			v.vlk.Unlock()
			if !expire {
				continue
			}
			v.update <- struct{}{}
		}
	}
}

func (v *ValidateSelector) GenerateValidation() map[string][]string {
	candidates := v.getAllCandidates()
	allNodes := v.getAllNode()

	var validations []*Node
	for _, node := range allNodes {
		v.vlk.RLock()
		_, ok := v.validators[node.deviceInfo.DeviceId]
		v.vlk.RUnlock()
		if ok {
			continue
		}
		validations = append(validations, node)
	}

	sort.Slice(validations, func(i, j int) bool {
		return validations[i].deviceInfo.BandwidthUp > validations[j].deviceInfo.BandwidthUp
	})

	var currentValidators []*CandidateNode
	for _, candidate := range candidates {
		v.vlk.RLock()
		_, ok := v.validators[candidate.deviceInfo.DeviceId]
		v.vlk.RUnlock()
		if !ok {
			continue
		}
		currentValidators = append(currentValidators, candidate)
	}

	rand.Shuffle(len(currentValidators), func(i, j int) {
		currentValidators[i], currentValidators[j] = currentValidators[j], currentValidators[i]
	})

	out := make(map[string][]string)

	minBandwidthUp := validations[len(validations)-1].deviceInfo.BandwidthUp
	for _, candidate := range currentValidators {
		var skips []*Node
		bandwidth := candidate.deviceInfo.BandwidthDown
		for i := 0; i < len(validations); i++ {
			if candidate.deviceInfo.BandwidthDown < validations[i].deviceInfo.BandwidthUp {
				skips = append(skips, validations[i])
				continue
			}
			if bandwidth < validations[i].deviceInfo.BandwidthUp {
				skips = append(skips, validations[i])
				continue
			}
			if bandwidth < minBandwidthUp {
				skips = append(skips, validations[i:]...)
				break
			}
			bandwidth = bandwidth - validations[i].deviceInfo.BandwidthUp
			if _, ok := out[candidate.deviceInfo.DeviceId]; !ok {
				out[candidate.deviceInfo.DeviceId] = make([]string, 0)
			}
			out[candidate.deviceInfo.DeviceId] = append(out[candidate.deviceInfo.DeviceId], validations[i].deviceInfo.DeviceId)
		}
		validations = skips
	}

	if len(validations) > 0 {
		log.Errorf("GenerateValidation remain nodes: %d", len(validations))
	}

	return out
}

func (v *ValidateSelector) getNextElectionTime() time.Time {
	diff := time.Now().Sub(v.startTime)
	left := v.opts.interval - diff
	if left <= 0 {
		return time.Now()
	}

	return time.Now().Add(left)
}
