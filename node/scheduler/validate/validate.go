package validate

import (
	"container/list"
	"context"
	"fmt"
	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/node/scheduler/node"
	"math/rand"
	"time"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/helper"
	"github.com/linguohua/titan/node/scheduler/db/cache"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"github.com/ouqiang/timewheel"
	"golang.org/x/xerrors"
)

const (
	errMsgTimeOut  = "TimeOut"
	missBlock      = "MissBlock"
	errMsgBlockNil = "Block Nil;map len:%d,count:%d"
	errMsgCidFail  = "Cid Fail;resultCid:%s,cid_db:%s,fid:%d,index:%d"
)

var log = logging.Logger("validate")
var myRand = rand.New(rand.NewSource(time.Now().UnixNano()))

// Validate Validate
type Validate struct {
	seed int64

	roundID string

	duration         int
	validateBlockMax int // block num limit

	timewheelValidate *timewheel.TimeWheel
	validateTime      int // time interval (minute)

	maxFidMap map[string]int64

	resultQueue   *list.List
	resultChannel chan bool

	nodeManager *node.Manager

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

func NewValidate(manager *node.Manager) *Validate {
	e := &Validate{
		seed:             1,
		duration:         10,
		validateBlockMax: 100,
		validateTime:     5,
		resultQueue:      list.New(),
		resultChannel:    make(chan bool, 1),
		nodeManager:      manager,
		open:             false,
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
		edgeNode := v.nodeManager.GetEdgeNode(deviceID)
		if edgeNode == nil {
			candidateNode := v.nodeManager.GetCandidateNode(deviceID)
			if candidateNode == nil {
				errList = append(errList, deviceID)
				continue
			}
			addr = candidateNode.Node.Addr
			nodeType = api.NodeCandidate
		} else {
			addr = edgeNode.Node.Addr
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
	}
}

func (v *Validate) PushResultToQueue(validateResults *api.ValidateResults) error {
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
		err := v.updateLatency(validateResults.DeviceID, validateResults.Latency)
		if err != nil {
			log.Errorf(err.Error())
			return
		}
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

		// fidStr := fmt.Sprintf("%d", fid)

		cid := cacheInfos[fid]
		if cid == "" {
			// log.Warnf("cid nil;fid:%s", fidStr)
			// status = cache.ValidateStatusFail
			// msg = fmt.Sprintf("GetCacheBlockInfos err:%v,resultCid:%v,cid:%v,index:%v", err.Error(), resultCid, cid, index)
			continue
		}

		if !v.compareCid(cid, resultCid) {
			status = persistent.ValidateStatusFail
			msg = fmt.Sprintf(errMsgCidFail, resultCid, cid, fid, index)
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

func randomNum(start, end int) int {
	max := end - start
	if max <= 0 {
		return start
	}

	x := myRand.Intn(10000)
	y := x % end

	return y + start
}

func (v *Validate) matchValidator(validatorList []string, deviceID string, validatorMap map[string][]string) (map[string][]string, []string) {

	cs := v.nodeManager.FindCandidateNodes(validatorList, nil)

	if cs == nil || len(cs) == 0 {
		return nil, nil
	}

	validatorID := cs[randomNum(0, len(cs))].DeviceInfo.DeviceId

	vList := make([]string, 0)
	if list, ok := validatorMap[validatorID]; ok {
		vList = append(list, deviceID)
	} else {
		vList = append(vList, deviceID)
	}
	validatorMap[validatorID] = vList

	return validatorMap, validatorList
}

func (v *Validate) validateMapping(validatorList []string) map[string][]string {
	result := make(map[string][]string)
	v.nodeManager.EdgeNodeMap.Range(func(key, value any) bool {

		validatorID := validatorList[randomNum(0, len(validatorList))]

		if validated, ok := result[validatorID]; ok {
			validated = append(validated, key.(string))
			result[validatorID] = validated
		} else {
			vd := make([]string, 0)
			vd = append(vd, key.(string))
			result[validatorID] = vd
		}

		return true
	})

	v.nodeManager.CandidateNodeMap.Range(func(key, value any) bool {

		validatorID := differentValue(validatorList, key.(string))
		if validatorID == "" {
			return false
		}

		if validated, ok := result[validatorID]; ok {
			validated = append(validated, key.(string))
			result[validatorID] = validated
		} else {
			vd := make([]string, 0)
			vd = append(vd, key.(string))
			result[validatorID] = vd
		}

		return true
	})
	return result
}

func differentValue(list []string, compare string) string {
	if list == nil || len(list) == 0 {
		return ""
	}
	value := list[randomNum(0, len(list))]
	if compare == value {
		return differentValue(list, compare)
	}
	return value
}

// Validate
func (v *Validate) startValidate() error {
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

	validatorList, err := cache.GetDB().GetValidatorsWithList()
	if err != nil {
		return err
	}

	validatorMap := v.validateMapping(validatorList)

	for validatorID, validatedList := range validatorMap {
		req, errList := v.getReqValidates(validatorID, validatedList)
		offline := false
		validator := v.nodeManager.GetCandidateNode(validatorID)
		if validator != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()

			err := validator.NodeAPI.ValidateBlocks(ctx, req)
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

		log.Infof("validatorID :%s, List:%v", validatorID, validatedList)
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
	hash1, err := helper.CIDString2HashString(cidStr1)
	if err != nil {
		return false
	}

	hash2, err := helper.CIDString2HashString(cidStr2)
	if err != nil {
		return false
	}

	return hash1 == hash2
}

func (v *Validate) updateLatency(deviceID string, latency float64) error {
	return cache.GetDB().UpdateDeviceInfo(deviceID, func(deviceInfo *api.DevicesInfo) {
		deviceInfo.Latency = latency
	})
}
