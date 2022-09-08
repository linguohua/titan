package scheduler

import (
	"time"

	"github.com/linguohua/titan/node/scheduler/db"
	"github.com/ouqiang/timewheel"
)

// Election Election
type Election struct {
	timewheelElection *timewheel.TimeWheel
	electionTime      int // election time interval (minute)

	verifiedNodeMax int // verified node num limit
}

// init timers
func (e *Election) initElectionTimewheels(scheduler *Scheduler) {
	// election timewheel
	e.timewheelElection = timewheel.New(1*time.Second, 3600, func(_ interface{}) {
		err := e.electionValidators(scheduler)
		if err != nil {
			log.Panicf("electionValidators err:%v", err.Error())
		}
		e.timewheelElection.AddTimer(time.Duration(e.electionTime)*60*time.Second, "election", nil)
	})
	e.timewheelElection.Start()
	e.timewheelElection.AddTimer(time.Duration(1)*60*time.Second, "election", nil)
}

func newElection(verifiedNodeMax int) *Election {
	e := &Election{
		electionTime:    60,
		verifiedNodeMax: verifiedNodeMax,
	}

	return e
}

func (e *Election) cleanValidators(scheduler *Scheduler) error {
	validators, err := db.GetCacheDB().GetValidatorsWithList()
	if err != nil {
		return err
	}

	for _, validator := range validators {
		err = db.GetCacheDB().RemoveValidatorGeoList(validator)
		if err != nil {
			log.Warnf("RemoveValidatorGeoList err:%v, validator:%v", err.Error(), validator)
		}

		node := scheduler.nodeManager.getCandidateNode(validator)
		if node != nil {
			node.isValidator = false
		}
	}

	err = db.GetCacheDB().RemoveValidatorList()
	if err != nil {
		return err
	}

	return nil
}

// election
func (e *Election) electionValidators(scheduler *Scheduler) error {
	err := e.cleanValidators(scheduler)
	if err != nil {
		return err
	}

	scheduler.poolGroup.pendingNodesToPool()

	alreadyAssignValidatorMap := make(map[string]string) // key:deviceID val:geo
	unAssignCandidates := make([]string, 0)              // deviceIDs
	lackValidatorMap := make(map[string]int)             //key:geo val:lack-validator-num

	scheduler.poolGroup.poolMap.Range(func(key, value interface{}) bool {
		geo := key.(string)
		pool := value.(*pool)
		if pool == nil {
			return true
		}

		pool.resetRoles()

		edgeNum := len(pool.edgeNodes)
		candidateNum := len(pool.candidateNodes)
		if edgeNum <= 0 && candidateNum <= 0 {
			return true
		}

		nodeTotalNum := edgeNum + candidateNum
		addNum := 0
		if nodeTotalNum%(e.verifiedNodeMax+1) > 0 {
			addNum = 1
		}
		needVeriftorNum := nodeTotalNum/(e.verifiedNodeMax+1) + addNum

		if candidateNum >= needVeriftorNum {
			// election validators and put to alreadyAssignValidatorMap
			// put other candidates to unAssignCandidates
			vn := 0
			for deviceID := range pool.candidateNodes {
				vn++
				if vn > needVeriftorNum {
					unAssignCandidates = append(unAssignCandidates, deviceID)
				} else {
					alreadyAssignValidatorMap[deviceID] = geo
				}
			}
		} else {
			for deviceID := range pool.candidateNodes {
				alreadyAssignValidatorMap[deviceID] = geo
			}

			lackValidatorMap[geo] = needVeriftorNum - candidateNum
		}

		return true
	})

	candidateNotEnough := false
	// again election
	if len(lackValidatorMap) > 0 {
		for geo, num := range lackValidatorMap {
			if len(unAssignCandidates) == 0 {
				candidateNotEnough = true
				break
			}

			n := num
			if len(unAssignCandidates) < num {
				n = len(unAssignCandidates)
			}

			validatorIDs := unAssignCandidates[0:n]
			unAssignCandidates = unAssignCandidates[n:]

			for _, validatorID := range validatorIDs {
				alreadyAssignValidatorMap[validatorID] = geo
			}
		}

		if candidateNotEnough {
			log.Warnf("Candidate Not Enough  assignEdge: %v", lackValidatorMap)
		}
	}

	// save election result
	for validatorID, geo := range alreadyAssignValidatorMap {
		validator := scheduler.nodeManager.getCandidateNode(validatorID)
		if validator != nil {
			validator.isValidator = true
		}

		err = db.GetCacheDB().SetValidatorToList(validatorID)
		if err != nil {
			return err
		}

		err = db.GetCacheDB().SetGeoToValidatorList(validatorID, geo)
		if err != nil {
			return err
		}

		// move the validator to validatorMap
		validatorGeo, ok := scheduler.poolGroup.poolIDMap.Load(validatorID)
		if ok && validatorGeo != nil {
			vGeo := validatorGeo.(string)
			poolGroup := scheduler.poolGroup.loadPool(vGeo)
			if poolGroup != nil {
				poolGroup.setVeriftor(validatorID)
			}
		}
	}

	// reset count
	scheduler.nodeManager.resetCandidateAndValidatorCount()

	return nil
}
