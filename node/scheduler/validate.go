package scheduler

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/scheduler/db/cache"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"github.com/ouqiang/timewheel"
	"golang.org/x/xerrors"
)

const (
	errMsgTimeOut  = "TimeOut"
	errMsgBlockNil = "Block Nil;map len:%v,count:%v"
	errMsgCidFail  = "Cid Fail;resultCid:%v,cid:%v,fid:%v,index:%v"
)

// Validate Validate
type Validate struct {
	seed int64

	roundID string

	duration         int
	validateBlockMax int // validate block num limit
	verifiedNodeMax  int // verified node num limit

	timewheelValidate *timewheel.TimeWheel
	validateTime      int // validate time interval (minute)

	maxFidMap map[string]int64
}

// init timers
func (e *Validate) initValidateTimewheel(scheduler *Scheduler) {
	// validate timewheel
	e.timewheelValidate = timewheel.New(1*time.Second, 3600, func(_ interface{}) {
		err := e.startValidate(scheduler)
		if err != nil {
			log.Panicf("startValidate err:%v", err.Error())
		}
		e.timewheelValidate.AddTimer(time.Duration(e.validateTime)*60*time.Second, "validate", nil)
	})
	e.timewheelValidate.Start()
	e.timewheelValidate.AddTimer(time.Duration(2)*60*time.Second, "validate", nil)
}

func newValidate(verifiedNodeMax int) *Validate {
	e := &Validate{
		seed:             int64(1),
		duration:         10,
		validateBlockMax: 100,
		verifiedNodeMax:  verifiedNodeMax,
		validateTime:     10,
	}

	return e
}

func (e *Validate) getReqValidates(scheduler *Scheduler, validatorID string, list []string) ([]api.ReqValidate, []string) {
	req := make([]api.ReqValidate, 0)
	errList := make([]string, 0)

	var nodeType api.NodeType

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
			nodeType = api.NodeCandidate
		} else {
			addr = edgeNode.Node.addr
			nodeType = api.NodeEdge
		}

		// cache datas
		num, err := cache.GetDB().GetBlockFidNum(deviceID)
		if err != nil {
			log.Warnf("validate GetBlockFidNum err:%v,DeviceId:%v", err.Error(), deviceID)
			continue
		}

		if num <= 0 {
			continue
		}

		maxFid, err := cache.GetDB().GetNodeCacheFid(deviceID)
		if err != nil {
			log.Warnf("validate GetNodeCacheTag err:%v,DeviceId:%v", err.Error(), deviceID)
			continue
		}
		e.maxFidMap[deviceID] = maxFid

		req = append(req, api.ReqValidate{Seed: e.seed, NodeURL: addr, Duration: e.duration, RoundID: e.roundID, NodeType: int(nodeType), MaxFid: int(maxFid)})

		resultInfo := &persistent.ValidateResult{RoundID: e.roundID, DeviceID: deviceID, ValidatorID: validatorID, Status: int(persistent.ValidateStatusCreate)}
		err = persistent.GetDB().SetValidateResultInfo(resultInfo)
		if err != nil {
			log.Warnf("validate SetValidateResultInfo err:%v,DeviceId:%v", err.Error(), deviceID)
			continue
		}

		err = cache.GetDB().SetNodeToValidateingList(deviceID)
		if err != nil {
			log.Warnf("validate SetNodeToValidateingList err:%v,DeviceId:%v", err.Error(), deviceID)
			continue
		}
	}

	return req, errList
}

func (e *Validate) getRandNum(max int, r *rand.Rand) int {
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
func (e *Validate) saveValidateResult(rID string, deviceID string, validatorID string, msg string, status cache.ValidateStatus) error {
	resultInfo := &persistent.ValidateResult{RoundID: rID, DeviceID: deviceID, Status: int(status), Msg: msg}
	err := persistent.GetDB().SetValidateResultInfo(resultInfo)
	if err != nil {
		return err
	}

	err = cache.GetDB().RemoveNodeWithValidateingList(deviceID)
	if err != nil {
		return err
	}

	if msg != "" {
		err = persistent.GetDB().SetNodeToValidateErrorList(rID, deviceID)
		if err != nil {
			return err
		}
	}

	return nil
}

func (e *Validate) validateResult(validateResults *api.ValidateResults) error {
	if validateResults.RoundID != e.roundID {
		return xerrors.Errorf("roundID err")
	}

	deviceID := validateResults.DeviceID
	log.Infof("validateResult:%v", deviceID)

	status := cache.ValidateStatusSuccess
	msg := ""

	if validateResults.IsTimeout {
		status = cache.ValidateStatusTimeOut
		msg = errMsgTimeOut
		return e.saveValidateResult(e.roundID, deviceID, "", msg, status)
	}

	r := rand.New(rand.NewSource(e.seed))
	rlen := len(validateResults.Cids)

	if rlen <= 0 || validateResults.RandomCount <= 0 {
		status = cache.ValidateStatusFail
		msg = fmt.Sprintf(errMsgBlockNil, rlen, validateResults.RandomCount)
		return e.saveValidateResult(e.roundID, deviceID, "", msg, status)
	}

	maxFid := e.maxFidMap[deviceID]

	for index := 0; index < validateResults.RandomCount; index++ {
		fid := e.getRandNum(int(maxFid), r) + 1
		resultCid := validateResults.Cids[index]

		fidStr := fmt.Sprintf("%d", fid)

		cid, err := cache.GetDB().GetBlockCidWithFid(deviceID, fidStr)
		if err != nil || cid == "" {
			// status = cache.ValidateStatusFail
			// msg = fmt.Sprintf("GetCacheBlockInfos err:%v,resultCid:%v,cid:%v,index:%v", err.Error(), resultCid, cid, index)
			continue
		}

		if resultCid != cid {
			status = cache.ValidateStatusFail
			msg = fmt.Sprintf(errMsgCidFail, resultCid, cid, fid, index)
			break
		}
	}

	return e.saveValidateResult(e.roundID, deviceID, "", msg, status)
}

func (e *Validate) checkValidateTimeOut() error {
	deviceIDs, err := cache.GetDB().GetNodesWithValidateingList()
	if err != nil {
		return err
	}

	if len(deviceIDs) > 0 {
		log.Infof("checkValidateTimeOut list:%v", deviceIDs)

		for _, deviceID := range deviceIDs {
			resultInfo := &persistent.ValidateResult{RoundID: e.roundID, DeviceID: deviceID, Status: int(cache.ValidateStatusTimeOut)}
			err = persistent.GetDB().SetValidateResultInfo(resultInfo)
			if err != nil {
				log.Warnf("checkValidateTimeOut SetValidateResultInfo err:%v,DeviceId:%v", err.Error(), deviceID)
				continue
			}

			err = cache.GetDB().RemoveNodeWithValidateingList(deviceID)
			if err != nil {
				log.Warnf("checkValidateTimeOut RemoveNodeWithValidateList err:%v,DeviceId:%v", err.Error(), deviceID)
				continue
			}
		}
	}

	return nil
}

// Validate
func (e *Validate) startValidate(scheduler *Scheduler) error {
	err := cache.GetDB().RemoveValidateingList()
	if err != nil {
		return err
	}

	sID, err := cache.GetDB().IncrValidateRoundID()
	if err != nil {
		return err
	}

	e.roundID = fmt.Sprintf("%d", sID)
	e.seed = time.Now().UnixNano()
	e.maxFidMap = make(map[string]int64)

	// find validators
	validators, err := cache.GetDB().GetValidatorsWithList()
	if err != nil {
		return err
	}

	geoMap := make(map[string][]string)

	for _, validatorID := range validators {
		geos, err := cache.GetDB().GetGeoWithValidatorList(validatorID)
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
		for deviceID := range poolGroup.edgeNodeMap {
			verifiedList = append(verifiedList, deviceID)
		}

		for deviceID := range poolGroup.candidateNodeMap {
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
			err = persistent.GetDB().SetNodeToValidateErrorList(e.roundID, validatorID)
			if err != nil {
				log.Errorf("SetNodeToValidateErrorList ,err:%v,deviceID:%v", err.Error(), validatorID)
			}
		}

		for _, deviceID := range errList {
			err = persistent.GetDB().SetNodeToValidateErrorList(e.roundID, deviceID)
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
