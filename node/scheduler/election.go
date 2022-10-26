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

	validatePool *ValidatePool
}

// init timers
func (e *Election) initElectionTask() {
	// election timewheel
	e.timewheelElection = timewheel.New(time.Second, 3600, func(_ interface{}) {
		e.timewheelElection.AddTimer((time.Duration(e.electionTime)*60-1)*time.Second, "election", nil)
		err := e.startElection()
		if err != nil {
			log.Panicf("startElection err:%v", err.Error())
		}
	})
	e.timewheelElection.Start()
	e.timewheelElection.AddTimer(time.Duration(1)*60*time.Second, "election", nil)
}

func newElection(pool *ValidatePool) *Election {
	e := &Election{
		electionTime: 60 * 24 * 2,
		validatePool: pool,
	}

	e.initElectionTask()

	return e
}

func (e *Election) cleanValidators() error {
	// validatorList, err := cache.GetDB().GetValidatorsWithList()
	// if err != nil {
	// 	return err
	// }

	// for _, validator := range validatorList {
	// err = cache.GetDB().RemoveValidatorGeoList(validator)
	// if err != nil {
	// 	log.Warnf("RemoveValidatorGeoList err:%v, validator:%v", err.Error(), validator)
	// }

	// node := scheduler.nodeManager.getCandidateNode(validator)
	// if node != nil {
	// 	node.isValidator = false
	// }
	// }

	err := cache.GetDB().RemoveValidatorList()
	if err != nil {
		return err
	}

	return nil
}

// election
func (e *Election) startElection() error {
	err := e.cleanValidators()
	if err != nil {
		return err
	}

	vList, lackNum := e.validatePool.election()

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
