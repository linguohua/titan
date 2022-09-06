package scheduler

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/scheduler/db"
	"github.com/ouqiang/timewheel"
	"golang.org/x/xerrors"
)

// ElectionValidate ElectionValidate
type ElectionValidate struct {
	seed int64
	// fidsMap map[string][]string
	roundID string

	duration          int
	validateBlockMax  int // validate block num limit
	verifiedNodeMax   int // verified node num limit
	timewheelElection *timewheel.TimeWheel
	electionTime      int // election time interval (minute)
	timewheelValidate *timewheel.TimeWheel
	validateTime      int // validate time interval (minute)
}

// init timers
func (e *ElectionValidate) initValidateTimewheels(scheduler *Scheduler) {
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

	// validate timewheel
	e.timewheelValidate = timewheel.New(1*time.Second, 3600, func(_ interface{}) {
		err := e.startValidates(scheduler)
		if err != nil {
			log.Panicf("startValidates err:%v", err.Error())
		}
		e.timewheelValidate.AddTimer(time.Duration(e.validateTime)*60*time.Second, "validate", nil)
	})
	e.timewheelValidate.Start()
	e.timewheelValidate.AddTimer(time.Duration(2)*60*time.Second, "validate", nil)
}

func newElectionValidate() *ElectionValidate {
	e := &ElectionValidate{
		seed:             int64(1),
		duration:         10,
		validateBlockMax: 100,
		verifiedNodeMax:  10,
		electionTime:     60,
		validateTime:     10,
	}

	return e
}

func (e *ElectionValidate) getReqValidates(scheduler *Scheduler, validatorID string, list []string) ([]api.ReqValidate, []string) {
	req := make([]api.ReqValidate, 0)
	errList := make([]string, 0)

	for _, deviceID := range list {
		addr := ""
		edgeNode := scheduler.nodeManager.getEdgeNode(deviceID)
		if edgeNode == nil {
			candidateNode := scheduler.nodeManager.getCandidateNode(deviceID)
			if candidateNode == nil {
				errList = append(errList, deviceID)
				continue
			}
			addr = candidateNode.Node.addr
		} else {
			addr = edgeNode.Node.addr
		}

		// cache datas
		num, err := db.GetCacheDB().GetCacheBlockNum(deviceID)
		if err != nil {
			log.Warnf("validate GetCacheBlockInfos err:%v,DeviceId:%v", err.Error(), deviceID)
			continue
		}

		if num <= 0 {
			continue
		}

		// fids := make([]string, 0)
		// for _, tag := range datas {
		// 	if tag == dataDefaultTag {
		// 		continue
		// 	}

		// 	fids = append(fids, tag)

		// 	if len(fids) >= e.validateBlockMax {
		// 		break
		// 	}
		// }

		req = append(req, api.ReqValidate{Seed: e.seed, NodeURL: addr, Duration: e.duration, RoundID: e.roundID, Type: string(api.TypeNameEdge)})

		// e.fidsMap[deviceID] = fids
		//
		err = db.GetCacheDB().SetValidateResultInfo(e.roundID, deviceID, validatorID, "", db.ValidateStatusCreate)
		if err != nil {
			log.Warnf("validate SetValidateResultInfo err:%v,DeviceId:%v", err.Error(), deviceID)
			continue
		}

		err = db.GetCacheDB().SetNodeToValidateingList(deviceID)
		if err != nil {
			log.Warnf("validate SetNodeToValidateingList err:%v,DeviceId:%v", err.Error(), deviceID)
			continue
		}
	}

	return req, errList
}

func (e *ElectionValidate) getRandFid(max int, r *rand.Rand) int {
	if max > 0 {
		return r.Intn(max)
	}

	return max
}

// func toCidV0(c cid.Cid) (cid.Cid, error) {
// 	if c.Type() != cid.DagProtobuf {
// 		return cid.Cid{}, fmt.Errorf("can't convert non-dag-pb nodes to cidv0")
// 	}
// 	return cid.NewCidV0(c.Hash()), nil
// }

// func toCidV1(c cid.Cid) (cid.Cid, error) {
// 	return cid.NewCidV1(c.Type(), c.Hash()), nil
// }

// TODO save to sql
func (e *ElectionValidate) saveValidateResult(sID string, deviceID string, validatorID string, msg string, status db.ValidateStatus) error {
	err := db.GetCacheDB().SetValidateResultInfo(sID, deviceID, "", msg, status)
	if err != nil {
		return err
	}

	err = db.GetCacheDB().RemoveNodeWithValidateingList(deviceID)
	if err != nil {
		return err
	}

	if msg != "" {
		err = db.GetCacheDB().SetNodeToValidateErrorList(sID, deviceID)
		if err != nil {
			return err
		}
	}

	return nil
}

func (e *ElectionValidate) validateResult(validateResults *api.ValidateResults) error {
	if validateResults.RoundID != e.roundID {
		return xerrors.Errorf("roundID err")
	}

	deviceID := validateResults.DeviceID
	log.Infof("validateResult:%v", deviceID)

	status := db.ValidateStatusSuccess
	msg := ""

	if validateResults.IsTimeout {
		status = db.ValidateStatusTimeOut
		msg = fmt.Sprint("Time out")
		return e.saveValidateResult(e.roundID, deviceID, "", msg, status)
	}

	r := rand.New(rand.NewSource(e.seed))
	rlen := len(validateResults.Cids)

	if rlen <= 0 {
		status = db.ValidateStatusFail
		msg = fmt.Sprint("Results is nil")
		return e.saveValidateResult(e.roundID, deviceID, "", msg, status)
	}

	// list := e.fidsMap[deviceID]
	// max := len(list)
	max, err := db.GetCacheDB().GetCacheBlockNum(deviceID)
	if err != nil {
		log.Warnf("validateResult GetCacheBlockNum err:%v,DeviceId:%v", err.Error(), deviceID)
		return err
	}

	for i := 0; i < rlen; i++ {
		index := e.getRandFid(int(max), r)
		resultCid := validateResults.Cids[i]

		cids, err := db.GetCacheDB().GetCacheBlockInfos(deviceID, int64(index), int64(index))
		if err != nil {
			status = db.ValidateStatusFail
			msg = fmt.Sprintf("GetCacheBlockInfos err:%v,resultCid:%v,index:%v", err.Error(), resultCid, i)
			break
		}

		cid := cids[0]
		// fidStr := list[index]
		// // log.Infof("fidStr:%v,resultFid:%v,index:%v", fidStr, result.Fid, i)
		// if fidStr != result.Fid {
		// 	status = db.ValidateStatusFail
		// 	msg = fmt.Sprintf("fidStr:%v,resultFid:%v,index:%v", fidStr, result.Fid, i)
		// 	break
		// }

		if resultCid == "" {
			status = db.ValidateStatusFail
			msg = fmt.Sprintf("resultCid:%v,cid:%v", resultCid, cid)
			break
		}

		// tag, err := db.GetCacheDB().GetCacheBlockInfo(deviceID, result.Cid)
		// if err != nil {
		// 	status = db.ValidateStatusFail
		// 	msg = fmt.Sprintf("GetCacheBlockInfo err:%v,deviceID:%v,resultCid:%v,resultFid:%v", err.Error(), deviceID, result.Cid, result.Fid)
		// 	break
		// }

		if resultCid != cid {
			status = db.ValidateStatusFail
			msg = fmt.Sprintf("result.Cid:%v,Cid:%v", resultCid, cid)
			break
		}
	}

	return e.saveValidateResult(e.roundID, deviceID, "", msg, status)
}

func (e *ElectionValidate) checkValidateTimeOut() error {
	edgeIDs, err := db.GetCacheDB().GetNodesWithValidateingList()
	if err != nil {
		return err
	}

	if len(edgeIDs) > 0 {
		log.Infof("checkValidateTimeOut list:%v", edgeIDs)

		for _, edgeID := range edgeIDs {
			err = db.GetCacheDB().SetValidateResultInfo(e.roundID, edgeID, "", "", db.ValidateStatusTimeOut)
			if err != nil {
				log.Warnf("checkValidateTimeOut SetValidateResultInfo err:%v,DeviceId:%v", err.Error(), edgeID)
				continue
			}

			err = db.GetCacheDB().RemoveNodeWithValidateingList(edgeID)
			if err != nil {
				log.Warnf("checkValidateTimeOut RemoveNodeWithValidateList err:%v,DeviceId:%v", err.Error(), edgeID)
				continue
			}
		}
	}

	return nil
}

func (e *ElectionValidate) cleanValidators(scheduler *Scheduler) error {
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
func (e *ElectionValidate) electionValidators(scheduler *Scheduler) error {
	err := e.cleanValidators(scheduler)
	if err != nil {
		return err
	}

	scheduler.poolGroup.pendingNodesToPool()
	scheduler.poolGroup.printlnPoolMap()

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
		n := 0
		if nodeTotalNum%(e.verifiedNodeMax+1) > 0 {
			n = 1
		}
		needVeriftorNum := nodeTotalNum/(e.verifiedNodeMax+1) + n

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

// Validate
func (e *ElectionValidate) startValidates(scheduler *Scheduler) error {
	err := db.GetCacheDB().RemoveValidateingList()
	if err != nil {
		return err
	}

	sID, err := db.GetCacheDB().IncrValidateRoundID()
	if err != nil {
		return err
	}

	e.roundID = fmt.Sprintf("%d", sID)
	e.seed = time.Now().UnixNano()
	// e.fidsMap = make(map[string][]string)

	// find validators
	validators, err := db.GetCacheDB().GetValidatorsWithList()
	if err != nil {
		return err
	}

	geoMap := make(map[string][]string)

	for _, validatorID := range validators {
		geos, err := db.GetCacheDB().GetGeoWithValidatorList(validatorID)
		if err != nil {
			log.Warnf("GetGeoWithValidatorList err:%v,validatorID:%v", err.Error(), validatorID)
			continue
		}

		for _, geo := range geos {
			if list, ok := geoMap[geo]; ok {
				geoMap[geo] = append(list, validatorID)
			} else {
				geoMap[geo] = []string{validatorID}
			}
		}
	}

	validatorMap := make(map[string][]string)

	for geo, validatorList := range geoMap {
		poolGroup := scheduler.poolGroup.loadPool(geo)
		if poolGroup == nil {
			log.Warnf("validates loadGroupMap is nil ,geo:%v", geo)
			continue
		}

		verifiedList := make([]string, 0)
		// rand group
		for deviceID := range poolGroup.edgeNodes {
			verifiedList = append(verifiedList, deviceID)
		}

		for deviceID := range poolGroup.candidateNodes {
			verifiedList = append(verifiedList, deviceID)
		}

		// assign verified (edge and candidate)
		verifiedLen := len(verifiedList)
		validatorLen := len(validatorList)

		num := verifiedLen / validatorLen
		if verifiedLen%validatorLen > 0 {
			num++
		}

		for i := 0; i < validatorLen; i++ {
			validatorID := validatorList[i]

			end := (i * num) + num
			if end > verifiedLen {
				end = verifiedLen
			}
			newList := verifiedList[i*num : end]

			if list, ok := validatorMap[validatorID]; ok {
				validatorMap[validatorID] = append(list, newList...)
			} else {
				validatorMap[validatorID] = newList
			}
		}
	}

	for validatorID, list := range validatorMap {
		req, errList := e.getReqValidates(scheduler, validatorID, list)
		offline := false
		validator := scheduler.nodeManager.getCandidateNode(validatorID)
		if validator != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()

			err := validator.nodeAPI.ValidateBlocks(ctx, req)
			if err != nil {
				log.Warnf("ValidateData err:%v, DeviceId:%v", err.Error(), validatorID)
				offline = true
			}

		} else {
			offline = true
		}

		if offline {
			err = db.GetCacheDB().SetNodeToValidateErrorList(e.roundID, validatorID)
			if err != nil {
				log.Errorf("SetNodeToValidateErrorList ,err:%v,deviceID:%v", err.Error(), validatorID)
			}
		}

		for _, deviceID := range errList {
			err = db.GetCacheDB().SetNodeToValidateErrorList(e.roundID, deviceID)
			if err != nil {
				log.Errorf("SetNodeToValidateErrorList ,err:%v,deviceID:%v", err.Error(), deviceID)
			}
		}

		log.Infof("validatorID :%v, List:%v", validatorID, list)
	}

	t := time.NewTimer(time.Duration(e.duration*2) * time.Second)

	for {
		select {
		case <-t.C:
			return e.checkValidateTimeOut()
		}
	}
}
