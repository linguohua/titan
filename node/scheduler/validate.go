package scheduler

import (
	"container/list"
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/scheduler/db/cache"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"github.com/ouqiang/timewheel"
	"golang.org/x/xerrors"
)

const (
	errMsgTimeOut  = "TimeOut"
	missBlock      = "MissBlock"
	errMsgBlockNil = "Block Nil;map len:%d,count:%d"
	errMsgCidFail  = "Cid Fail;resultCid:%s,cid_db:%s,fid:%s,index:%d"
)

// Validate Validate
type Validate struct {
	seed int64

	roundID string

	duration         int
	validateBlockMax int // validate block num limit

	timewheelValidate *timewheel.TimeWheel
	validateTime      int // validate time interval (minute)

	maxFidMap map[string]int64

	resultQueue   *list.List
	resultChannel chan bool

	validatePool *ValidatePool
	nodeManager  *NodeManager

	open bool
}

// init timers
func (v *Validate) initValidateTask() {
	// validate timewheel
	v.timewheelValidate = timewheel.New(time.Second, 3600, func(_ interface{}) {
		v.timewheelValidate.AddTimer((time.Duration(v.validateTime)*60-1)*time.Second, "validate", nil)
		err := v.startValidate()
		if err != nil {
			log.Panicf("startValidate err:%s", err.Error())
		}
	})
	v.timewheelValidate.Start()
	v.timewheelValidate.AddTimer(time.Duration(2)*60*time.Second, "validate", nil)

	go v.initChannelTask()
}

func newValidate(pool *ValidatePool, manager *NodeManager) *Validate {
	e := &Validate{
		seed:             int64(1),
		duration:         10,
		validateBlockMax: 100,
		validateTime:     5,
		resultQueue:      list.New(),
		resultChannel:    make(chan bool, 1),
		validatePool:     pool,
		nodeManager:      manager,
		open:             true,
	}

	e.initValidateTask()

	return e
}

func (v *Validate) getReqValidates(validatorID string, list []string) ([]api.ReqValidate, []string) {
	req := make([]api.ReqValidate, 0)
	errList := make([]string, 0)

	var nodeType api.NodeType

	for _, deviceID := range list {
		addr := ""
		edgeNode := v.nodeManager.getEdgeNode(deviceID)
		if edgeNode == nil {
			candidateNode := v.nodeManager.getCandidateNode(deviceID)
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
		num, err := persistent.GetDB().GetDeviceBlockNum(deviceID)
		if err != nil {
			// log.Warnf("validate GetBlockNum err:%v,DeviceId:%v", err.Error(), deviceID)
			err = v.saveValidateResult(v.roundID, deviceID, validatorID, err.Error(), persistent.ValidateStatusOther)
			if err != nil {
				log.Warnf("validate SetValidateResultInfo err:%s,DeviceId:%s", err.Error(), deviceID)
			}

			continue
		}

		if num <= 0 {
			err = v.saveValidateResult(v.roundID, deviceID, validatorID, missBlock, persistent.ValidateStatusOther)
			if err != nil {
				log.Warnf("validate SetValidateResultInfo err:%s,DeviceId:%s", err.Error(), deviceID)
			}

			continue
		}

		maxFid, err := cache.GetDB().GetNodeCacheFid(deviceID)
		if err != nil {
			log.Warnf("validate GetNodeCacheTag err:%s,DeviceId:%s", err.Error(), deviceID)
			continue
		}
		v.maxFidMap[deviceID] = maxFid

		req = append(req, api.ReqValidate{Seed: v.seed, NodeURL: addr, Duration: v.duration, RoundID: v.roundID, NodeType: int(nodeType), MaxFid: int(maxFid)})

		resultInfo := &persistent.ValidateResult{RoundID: v.roundID, DeviceID: deviceID, ValidatorID: validatorID, Status: int(persistent.ValidateStatusCreate)}
		err = persistent.GetDB().SetValidateResultInfo(resultInfo)
		if err != nil {
			log.Warnf("validate SetValidateResultInfo err:%s,DeviceId:%s", err.Error(), deviceID)
			continue
		}

		err = cache.GetDB().SetNodeToValidateingList(deviceID)
		if err != nil {
			log.Warnf("validate SetNodeToValidateingList err:%s,DeviceId:%s", err.Error(), deviceID)
			continue
		}
	}

	return req, errList
}

func (v *Validate) getRandNum(max int, r *rand.Rand) int {
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
func (v *Validate) saveValidateResult(rID string, deviceID string, validatorID string, msg string, status persistent.ValidateStatus) error {
	resultInfo := &persistent.ValidateResult{RoundID: rID, DeviceID: deviceID, Status: int(status), Msg: msg}
	err := persistent.GetDB().SetValidateResultInfo(resultInfo)
	if err != nil {
		return err
	}

	err = cache.GetDB().RemoveNodeWithValidateingList(deviceID)
	if err != nil {
		return err
	}

	if status == persistent.ValidateStatusSuccess || status == persistent.ValidateStatusOther {
		_, err = cache.GetDB().IncrNodeValidateTime(deviceID, 1)
		if err != nil {
			return err
		}
		return nil
	}

	if status == persistent.ValidateStatusFail || status == persistent.ValidateStatusTimeOut {
		err = persistent.GetDB().SetNodeToValidateErrorList(rID, deviceID)
		if err != nil {
			return err
		}
	}

	return nil
}

func (v *Validate) initChannelTask() {
	for {
		<-v.resultChannel

		v.doValidate()
	}
}

func (v *Validate) writeChanWithSelect(b bool) {
	select {
	case v.resultChannel <- b:
		return
	default:
		// log.Warnf("channel blocked, can not write")
	}
}

func (v *Validate) doValidate() {
	for v.resultQueue.Len() > 0 {
		element := v.resultQueue.Front() // First element
		validateResults := element.Value.(*api.ValidateResults)
		v.validate(validateResults)

		v.resultQueue.Remove(element) // Dequeue

		// v.writeChanWithSelect(true)
	}
}

func (v *Validate) pushResultToQueue(validateResults *api.ValidateResults) error {
	log.Infof("validateResult:%s,round:%s", validateResults.DeviceID, validateResults.RoundID)
	if v.resultQueue == nil {
		return xerrors.New("resultQueue is nil")
	}
	v.resultQueue.PushBack(validateResults)

	v.writeChanWithSelect(true)

	return nil
}

func (v *Validate) validate(validateResults *api.ValidateResults) error {
	if validateResults.RoundID != v.roundID {
		return xerrors.Errorf("roundID err")
	}
	log.Infof("do validate:%s,round:%s", validateResults.DeviceID, validateResults.RoundID)

	defer func() {
		v.updateLatency(validateResults.DeviceID, validateResults.Latency)
	}()

	deviceID := validateResults.DeviceID

	status := persistent.ValidateStatusSuccess
	msg := ""

	if validateResults.IsTimeout {
		status = persistent.ValidateStatusTimeOut
		msg = errMsgTimeOut
		return v.saveValidateResult(v.roundID, deviceID, "", msg, status)
	}

	r := rand.New(rand.NewSource(v.seed))
	rlen := len(validateResults.Cids)

	if rlen <= 0 || validateResults.RandomCount <= 0 {
		status = persistent.ValidateStatusFail
		msg = fmt.Sprintf(errMsgBlockNil, rlen, validateResults.RandomCount)
		return v.saveValidateResult(v.roundID, deviceID, "", msg, status)
	}

	cacheInfos, err := persistent.GetDB().GetBlocksFID(deviceID)
	if err != nil || len(cacheInfos) <= 0 {
		status = persistent.ValidateStatusOther
		msg = err.Error()
		return v.saveValidateResult(v.roundID, deviceID, "", msg, status)
	}

	maxFid := v.maxFidMap[deviceID]

	for index := 0; index < validateResults.RandomCount; index++ {
		fid := v.getRandNum(int(maxFid), r) + 1
		resultCid := validateResults.Cids[index]

		fidStr := fmt.Sprintf("%d", fid)

		cid := cacheInfos[fidStr]
		// cid, err := persistent.GetDB().GetBlockCidWithFid(deviceID, fidStr)
		if cid == "" {
			// log.Warnf("cid nil;fid:%s", fidStr)
			// status = cache.ValidateStatusFail
			// msg = fmt.Sprintf("GetCacheBlockInfos err:%v,resultCid:%v,cid:%v,index:%v", err.Error(), resultCid, cid, index)
			continue
		}

		if !v.compareCid(cid, resultCid) {
			status = persistent.ValidateStatusFail
			msg = fmt.Sprintf(errMsgCidFail, resultCid, cid, fidStr, index)
			break
		}
	}

	return v.saveValidateResult(v.roundID, deviceID, "", msg, status)
}

func (v *Validate) checkValidateTimeOut() error {
	deviceIDs, err := cache.GetDB().GetNodesWithValidateingList()
	if err != nil {
		return err
	}

	if len(deviceIDs) > 0 {
		log.Infof("checkValidateTimeOut list:%v", deviceIDs)

		for _, deviceID := range deviceIDs {
			v.saveValidateResult(v.roundID, deviceID, "", errMsgTimeOut, persistent.ValidateStatusTimeOut)
		}
	}

	return nil
}

func (v *Validate) matchValidator(validatorList []string, deviceID string, validatorMap map[string][]string) (map[string][]string, []string) {
	cs := v.nodeManager.findCandidateNodes(validatorList, nil)

	validatorID := ""
	if len(cs) > 0 {
		validatorID = cs[randomNum(0, len(cs))].deviceInfo.DeviceId
	} else {
		return validatorMap, validatorList
	}

	vList := make([]string, 0)
	if list, ok := validatorMap[validatorID]; ok {
		vList = append(list, deviceID)
	} else {
		vList = append(vList, deviceID)
	}
	validatorMap[validatorID] = vList

	if len(vList) >= v.validatePool.verifiedNodeMax {
		for i, id := range validatorList {
			if id == validatorID {
				if i >= len(validatorList)-1 {
					validatorList = validatorList[0:i]
				} else {
					validatorList = append(validatorList[0:i], validatorList[i+1:]...)
				}
				break
			}
		}
	}

	return validatorMap, validatorList
}

// Validate
func (v *Validate) startValidate() error {
	// log.Infof("------------startValidate:open,%v", v.open)
	if !v.open {
		return nil
	}

	log.Info("------------startValidate:")
	err := cache.GetDB().RemoveValidateingList()
	if err != nil {
		return err
	}

	sID, err := cache.GetDB().IncrValidateRoundID()
	if err != nil {
		return err
	}

	v.roundID = fmt.Sprintf("%d", sID)
	v.seed = time.Now().UnixNano()
	v.maxFidMap = make(map[string]int64)

	// find validators
	validatorMap := make(map[string][]string)

	validatorList, err := cache.GetDB().GetValidatorsWithList()
	if err != nil {
		return err
	}

	for deviceID := range v.validatePool.edgeNodeMap {
		validatorMap, validatorList = v.matchValidator(validatorList, deviceID, validatorMap)
	}

	for deviceID := range v.validatePool.candidateNodeMap {
		validatorMap, validatorList = v.matchValidator(validatorList, deviceID, validatorMap)
	}

	for validatorID, list := range validatorMap {
		req, errList := v.getReqValidates(validatorID, list)
		offline := false
		validator := v.nodeManager.getCandidateNode(validatorID)
		if validator != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()

			err := validator.nodeAPI.ValidateBlocks(ctx, req)
			if err != nil {
				log.Warnf("ValidateData err:%s, DeviceId:%s", err.Error(), validatorID)
				offline = true
			}

		} else {
			offline = true
		}

		if offline {
			err = persistent.GetDB().SetNodeToValidateErrorList(v.roundID, validatorID)
			if err != nil {
				log.Errorf("SetNodeToValidateErrorList ,err:%s,deviceID:%s", err.Error(), validatorID)
			}
		}

		for _, deviceID := range errList {
			err = persistent.GetDB().SetNodeToValidateErrorList(v.roundID, deviceID)
			if err != nil {
				log.Errorf("SetNodeToValidateErrorList ,err:%s,deviceID:%s", err.Error(), deviceID)
			}
		}

		log.Infof("validatorID :%s, List:%v", validatorID, list)
	}

	t := time.NewTimer(time.Duration(v.duration*2) * time.Second)

	for {
		select {
		case <-t.C:
			return v.checkValidateTimeOut()
		}
	}
}

func (v *Validate) compareCid(cidStr1, cidStr2 string) bool {
	// log.Warnf("cidStr1:%s,cidStr2:%s", cidStr1, cidStr2)

	target1, err := cid.Decode(cidStr1)
	if err != nil {
		// log.Warnf("err1:%s", err.Error())
		return false
	}

	target2, err := cid.Decode(cidStr2)
	if err != nil {
		// log.Warnf("err2:%s", err.Error())
		return false
	}
	// log.Warnf("Hash1:%s,Hash2:%s", target1.Hash().String(), target2.Hash().String())

	return target1.Hash().String() == target2.Hash().String()
}

func (v *Validate) updateLatency(deviceID string, latency float64) error {
	return cache.GetDB().UpdateNodeLatency(deviceID, latency)
}
