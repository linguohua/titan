package scheduler

import (
	"time"

	"github.com/linguohua/titan/node/scheduler/db/cache"
	"github.com/ouqiang/timewheel"
)

// Election Election
type Election struct {
	timewheelElection *timewheel.TimeWheel
	electionTime      int // election time interval (minute)

	verifiedNodeMax int // verified node num limit
}

// init timers
func (e *Election) initElectionTimewheel(scheduler *Scheduler) {
	// election timewheel
	e.timewheelElection = timewheel.New(1*time.Second, 3600, func(_ interface{}) {
		err := e.startElection(scheduler)
		if err != nil {
			log.Panicf("startElection err:%v", err.Error())
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
	validatorList, err := cache.GetDB().GetValidatorsWithList()
	if err != nil {
		return err
	}

	for _, validator := range validatorList {
		// err = cache.GetDB().RemoveValidatorGeoList(validator)
		// if err != nil {
		// 	log.Warnf("RemoveValidatorGeoList err:%v, validator:%v", err.Error(), validator)
		// }

		node := scheduler.nodeManager.getCandidateNode(validator)
		if node != nil {
			node.isValidator = false
		}
	}

	err = cache.GetDB().RemoveValidatorList()
	if err != nil {
		return err
	}

	return nil
}

// election
func (e *Election) startElection(scheduler *Scheduler) error {
	err := e.cleanValidators(scheduler)
	if err != nil {
		return err
	}

	vList, lackNum := scheduler.validatePool.election(e.verifiedNodeMax)

	// save election result
	for _, validatorID := range vList {
		err := cache.GetDB().SetValidatorToList(validatorID)
		if err != nil {
			log.Errorf("SetValidatorToList err : %v", err.Error())
		}

		// validator := scheduler.nodeManager.getCandidateNode(validatorID)
		// if validator != nil {
		// 	validator.isValidator = true
		// }
	}
	log.Infof("election validators:%v,lackNum:%v", vList, lackNum)

	return nil
}
