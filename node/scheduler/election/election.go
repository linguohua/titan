package election

import (
	"context"
	"math/rand"
	"sort"
	"sync"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/node/scheduler/db/cache"
	"github.com/linguohua/titan/node/scheduler/node"
)

var log = logging.Logger("election")

var HeartbeatInterval = 1 * time.Minute

// Election election
type Election struct {
	opts       EleOption
	connect    chan string
	update     chan struct{}
	manager    *node.Manager
	vlk        sync.RWMutex
	validators map[string]time.Time

	startTime time.Time

	validatorRatio float64 // Ratio of validators elected 0~1
}

//EleOption election option
type EleOption struct {
	ctx context.Context
	// interval is the time interval for re-election
	interval time.Duration
	// expiration is the maximum interval when the validator is offline
	expiration time.Duration
}

func newEleOption(opts ...Option) EleOption {
	opt := EleOption{
		ctx:        context.Background(),
		interval:   2 * 24 * time.Hour,
		expiration: 30 * time.Minute,
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
		connect:        make(chan string, 1),
		update:         make(chan struct{}, 1),
		validatorRatio: 1,
	}

	// open election
	go ele.run()

	return ele
}

func (v *Election) run() {
	go v.checkValidatorExpiration()
	if err := v.fetchCurrentValidators(); err != nil {
		log.Errorf("fetch current validators: %v", err)
	}
	// todo: may be delete
	go func() {
		tn := time.NewTicker(10 * time.Minute)
		defer tn.Stop()
		for {
			select {
			case <-tn.C:
				err := v.elect()
				if err != nil {
					log.Error(err.Error())
				}
				return
			}
		}
	}()

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

func (v *Election) elect() error {
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

func (v *Election) Start() {
	v.update <- struct{}{}
}

func (v *Election) winner(isAppend bool) ([]*node.CandidateNode, error) {
	var out []*node.CandidateNode
	defer func() {
		log.Infof("election winners count: %d", len(out))
	}()

	defer func() {
		now := time.Now()
		v.startTime = now
		err := cache.GetDB().UpdateBaseInfo("NextElectionTime", now.Add(v.opts.interval).Unix())
		if err != nil {
			log.Error(err.Error())
		}
	}()

	if !v.sufficientCandidates() {
		out = append(out, v.getAllCandidates()...)
		return out, nil
	}

	candidates := v.getAllCandidates()
	allNodes := v.getAllNode()
	sort.Slice(allNodes, func(i, j int) bool {
		return allNodes[i].BandwidthUp > allNodes[j].BandwidthUp
	})

	if isAppend && len(v.validators) > 0 {
		var currentValidators []*node.CandidateNode
		var excludeValidators []*node.CandidateNode
		for _, candidate := range candidates {
			v.vlk.RLock()
			_, exist := v.validators[candidate.DeviceId]
			v.vlk.RUnlock()
			if !exist {
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

func chooseCandidates(candidates []*node.CandidateNode, allNodes []*node.Node) (chosen []*node.CandidateNode, remains []*node.Node) {
	if len(allNodes) == 0 {
		return
	}
	minBandwidthUp := allNodes[len(allNodes)-1].BandwidthUp
	for _, candidate := range candidates {
		bandwidth := candidate.BandwidthDown
		var skips []*node.Node
		for i := 0; i < len(allNodes); i++ {
			if candidate.BandwidthDown < allNodes[i].BandwidthUp {
				skips = append(skips, allNodes[i])
				continue
			}
			if bandwidth < allNodes[i].BandwidthUp {
				skips = append(skips, allNodes[i])
				continue
			}
			if bandwidth < minBandwidthUp {
				skips = append(skips, allNodes[i:]...)
				break
			}
			bandwidth = bandwidth - allNodes[i].BandwidthUp
		}
		if bandwidth < candidate.BandwidthDown {
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

func (v *Election) saveWinners(winners []*node.CandidateNode) error {
	err := cache.GetDB().RemoveValidatorList()
	if err != nil {
		return err
	}

	v.validators = make(map[string]time.Time)
	for _, winner := range winners {
		v.vlk.Lock()
		v.validators[winner.DeviceId] = time.Now()
		v.vlk.Unlock()

		err := cache.GetDB().SetValidatorToList(winner.DeviceId)
		if err != nil {
			log.Errorf("SetValidatorToList err : %s", err.Error())
		}
	}

	return nil
}

func (v *Election) fetchCurrentValidators() error {
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
func (v *Election) sufficientCandidates() bool {
	var candidateDwnBdw, needUpBdw float64

	for _, candidate := range v.getAllCandidates() {
		candidateDwnBdw += candidate.BandwidthDown
	}

	for _, node := range v.getAllNode() {
		needUpBdw += node.BandwidthUp
	}

	if candidateDwnBdw < needUpBdw {
		log.Error("candidates insufficient upload bandwidth")
		return false
	}
	return true
}

// deviceConnect when a batch of edge nodes goes online, the number of validators may need to be recalculated.
func (v *Election) deviceConnect(deviceID string) error {
	log.Infof("device connect: %s", deviceID)
	v.vlk.Lock()
	_, exist := v.validators[deviceID]
	if exist {
		v.validators[deviceID] = time.Now()
	}
	v.vlk.Unlock()

	if !exist {
		return v.maybeReElect()
	}
	return nil
}

func (v *Election) maybeReElect() error {
	var validators []*node.CandidateNode
	candidates := v.getAllCandidates()
	allNodes := v.getAllNode()

	v.vlk.RLock()
	for _, candidate := range candidates {
		if _, exist := v.validators[candidate.DeviceId]; exist {
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

func (v *Election) updateValidators() error {
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

func (v *Election) NodeConnect(deviceID string) {
	v.connect <- deviceID
}

func (v *Election) getAllCandidates() []*node.CandidateNode {
	var candidates []*node.CandidateNode

	// cs := v.manage.GetAllCandidate()
	v.manager.CandidateNodeMap.Range(func(key, value interface{}) bool {
		node := value.(*node.CandidateNode)
		candidates = append(candidates, node)

		return true
	})

	return candidates
}

func (v *Election) getAllNode() []*node.Node {
	var nodes []*node.Node

	// es := v.manage.GetAllEdge()
	v.manager.EdgeNodeMap.Range(func(key, value interface{}) bool {
		node := value.(*node.EdgeNode)
		nodes = append(nodes, &node.Node)
		return true
	})

	// cs := v.manage.GetAllCandidate()
	v.manager.CandidateNodeMap.Range(func(key, value interface{}) bool {
		node := value.(*node.CandidateNode)
		nodes = append(nodes, &node.Node)
		return true
	})

	return nodes
}

// checkValidatorExpiration if the validator node goes offline (for a long time), it is necessary to re-elect a new validator.
// But the edge node doesn't need to do anything.
func (v *Election) checkValidatorExpiration() {
	ticker := time.NewTicker(HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			v.vlk.Lock()
			var expire bool
			for deviceID, lastActive := range v.validators {
				node := v.manager.GetCandidateNode(deviceID)
				if node != nil {
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

func (v *Election) GenerateValidation() map[string][]string {
	candidates := v.getAllCandidates()
	allNodes := v.getAllNode()

	var validations []*node.Node
	for _, node := range allNodes {
		v.vlk.RLock()
		_, exist := v.validators[node.DeviceId]
		v.vlk.RUnlock()
		if exist {
			continue
		}
		validations = append(validations, node)
	}

	sort.Slice(validations, func(i, j int) bool {
		return validations[i].BandwidthUp > validations[j].BandwidthUp
	})

	var currentValidators []*node.CandidateNode
	for _, candidate := range candidates {
		v.vlk.RLock()
		_, exist := v.validators[candidate.DeviceId]
		v.vlk.RUnlock()
		if !exist {
			continue
		}
		currentValidators = append(currentValidators, candidate)
	}

	rand.Shuffle(len(currentValidators), func(i, j int) {
		currentValidators[i], currentValidators[j] = currentValidators[j], currentValidators[i]
	})

	out := make(map[string][]string)

	minBandwidthUp := validations[len(validations)-1].BandwidthUp
	for _, candidate := range currentValidators {
		var skips []*node.Node
		bandwidth := candidate.BandwidthDown
		for i := 0; i < len(validations); i++ {
			if candidate.BandwidthDown < validations[i].BandwidthUp {
				skips = append(skips, validations[i])
				continue
			}
			if bandwidth < validations[i].BandwidthUp {
				skips = append(skips, validations[i])
				continue
			}
			if bandwidth < minBandwidthUp {
				skips = append(skips, validations[i:]...)
				break
			}
			bandwidth = bandwidth - validations[i].BandwidthUp
			if _, exist := out[candidate.DeviceId]; !exist {
				out[candidate.DeviceId] = make([]string, 0)
			}
			out[candidate.DeviceId] = append(out[candidate.DeviceId], validations[i].DeviceId)
		}
		validations = skips
	}

	if len(validations) > 0 {
		log.Errorf("GenerateValidation remain nodes: %d", len(validations))
	}

	return out
}

func (v *Election) getNextElectionTime() time.Time {
	diff := time.Now().Sub(v.startTime)
	left := v.opts.interval - diff
	if left <= 0 {
		return time.Now()
	}

	return time.Now().Add(left)
}
