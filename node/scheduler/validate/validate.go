package validate

import (
	"container/list"
	"context"
	"fmt"
	"github.com/robfig/cron"
	"math/rand"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/node/scheduler/node"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/helper"
	"github.com/linguohua/titan/node/scheduler/db/cache"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"golang.org/x/xerrors"
)

const (
	errMsgTimeOut  = "TimeOut"
	missBlock      = "MissBlock"
	errMsgBlockNil = "Block Nil;map len:%d,count:%d"
	errMsgCidFail  = "Cid Fail;resultCid:%s,cid_db:%s,fid:%d,index:%d"
)

var (
	log    = logging.Logger("validate")
	myRand = rand.New(rand.NewSource(time.Now().UnixNano()))
)

// Validate Validate
type Validate struct {
	ctx  context.Context
	seed int64

	// validate round number
	roundID int64

	duration int

	// block num limit
	validateBlockMax int

	// time interval (minute)
	validateTime int

	// fid is maximum value of each device storage record
	// key is device id
	// value is fid
	maxFidMap map[string]int64

	// temporary storage of call back message
	resultQueue *list.List
	// heartbeat of call back
	resultChannel chan bool

	nodeManager *node.Manager

	// validate switch
	open bool
}

// init timers
func (v *Validate) initValidateTask() {
	spec := fmt.Sprintf("* */%d * * * *", v.validateTime)
	crontab := cron.New()
	err := crontab.AddFunc(spec, func() {
		err := v.startValidate()
		if err != nil {
			log.Panicf("startValidate err:%s", err.Error())
		}
	})

	if err != nil {
		log.Panicf(err.Error())
	}

	crontab.Start()

	// wait call back message
	go v.initChannelTask()
}

func NewValidate(manager *node.Manager) *Validate {
	e := &Validate{
		ctx:              context.Background(),
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

func (v *Validate) assemblyReqValidates(validatorID string, list []*tmpDeviceMeta) []api.ReqValidate {
	req := make([]api.ReqValidate, 0)

	for _, device := range list {
		// count device cid number
		num, err := persistent.GetDB().CountCidOfDevice(device.deviceId)
		if err != nil {
			log.Warnf("failed to count cid from device : %s", device.deviceId)
			continue
		}

		// there is no cached cid in the device
		if num <= 0 {
			log.Warnf("no cached cid of device : %s", device.deviceId)
			continue
		}

		maxFid, err := cache.GetDB().GetNodeCacheFid(device.deviceId)
		if err != nil {
			log.Warnf("GetNodeCacheTag err:%s,DeviceId:%s", err.Error(), device.deviceId)
			continue
		}

		v.maxFidMap[device.deviceId] = maxFid

		req = append(req, api.ReqValidate{Seed: v.seed, NodeURL: device.addr, Duration: v.duration, RoundID: v.roundID, NodeType: int(device.nodeType), MaxFid: int(maxFid)})

		err = cache.GetDB().SetNodeToValidateingList(device.deviceId)
		if err != nil {
			log.Warnf("SetNodeToValidateingList err:%s, DeviceId:%s", err.Error(), device.deviceId)
			continue
		}

		resultInfo := &persistent.ValidateResult{
			RoundID:     v.roundID,
			DeviceID:    device.deviceId,
			ValidatorID: validatorID,
			Status:      persistent.ValidateStatusCreate.Int(),
			StartTime:   time.Now(),
		}

		err = persistent.GetDB().InsertValidateResultInfo(resultInfo)
		if err != nil {
			log.Errorf("InsertValidateResultInfo err:%s, DeviceId:%s", err.Error(), device.deviceId)
			continue
		}
	}

	return req
}

func (v *Validate) getRandNum(max int, r *rand.Rand) int {
	if max > 0 {
		return r.Intn(max)
	}

	return max
}

func (v *Validate) UpdateValidateResult(roundId int64, deviceID, msg string, status persistent.ValidateStatus) error {
	resultInfo := &persistent.ValidateResult{RoundID: roundId, DeviceID: deviceID, Msg: msg, Status: status.Int(), EndTime: time.Now()}
	return persistent.GetDB().UpdateValidateResultInfo(resultInfo)
}

func (v *Validate) initChannelTask() {
	for {
		select {
		case <-v.resultChannel:
			v.doCallback()
		}
	}
}

func (v *Validate) doCallback() {
	for v.resultQueue.Len() > 0 {
		element := v.resultQueue.Front() // First element
		if validateResults, ok := element.Value.(*api.ValidateResults); ok {
			err := v.handleValidateResult(validateResults)
			if err != nil {
				log.Errorf("deviceId[%s] handle validate result fail : %s", validateResults.DeviceID, err.Error())
			}
			v.resultQueue.Remove(element) // Dequeue
		}
	}
}

func (v *Validate) PushResultToQueue(validateResults *api.ValidateResults) {
	log.Infof("validateResult:%s,round:%d", validateResults.DeviceID, validateResults.RoundID)
	v.resultQueue.PushBack(validateResults)
	v.resultChannel <- true
}

func (v *Validate) handleValidateResult(validateResults *api.ValidateResults) error {
	log.Debug("validate result : %v", *validateResults)
	if validateResults.RoundID != v.roundID {
		return xerrors.Errorf("roundID err")
	}
	log.Infof("do validate:%s,round:%s", validateResults.DeviceID, validateResults.RoundID)

	deviceID := validateResults.DeviceID

	if validateResults.IsTimeout {
		return v.UpdateValidateResult(v.roundID, deviceID, errMsgTimeOut, persistent.ValidateStatusTimeOut)
	}

	r := rand.New(rand.NewSource(v.seed))
	cidLength := len(validateResults.Cids)

	if cidLength <= 0 || validateResults.RandomCount <= 0 {
		msg := fmt.Sprintf(errMsgBlockNil, cidLength, validateResults.RandomCount)
		log.Debug("validate fail :", msg)
		return v.UpdateValidateResult(v.roundID, deviceID, msg, persistent.ValidateStatusFail)
	}

	cacheInfos, err := persistent.GetDB().GetBlocksFID(deviceID)
	if err != nil || len(cacheInfos) <= 0 {
		msg := fmt.Sprintf("failed to query by device [%s] : %s", deviceID, err.Error())
		return v.UpdateValidateResult(v.roundID, deviceID, msg, persistent.ValidateStatusOther)
	}

	maxFid := v.maxFidMap[deviceID]

	for index := 0; index < validateResults.RandomCount; index++ {
		fid := v.getRandNum(int(maxFid), r) + 1
		resultCid := validateResults.Cids[index]

		cid := cacheInfos[fid]
		if cid == "" {
			continue
		}

		if !v.compareCid(cid, resultCid) {
			msg := fmt.Sprintf(errMsgCidFail, resultCid, cid, fid, index)
			log.Debug("validate fail :", msg)
			return v.UpdateValidateResult(v.roundID, deviceID, msg, persistent.ValidateStatusFail)
		}
	}

	return v.UpdateValidateResult(v.roundID, deviceID, "ok", persistent.ValidateStatusSuccess)
}

func (v *Validate) checkValidateTimeOut() error {
	deviceIDs, err := cache.GetDB().GetNodesWithValidateingList()
	if err != nil {
		return err
	}

	if len(deviceIDs) > 0 {
		log.Infof("checkValidateTimeOut list:%v", deviceIDs)

		for _, deviceID := range deviceIDs {
			err := v.UpdateValidateResult(v.roundID, deviceID, errMsgTimeOut, persistent.ValidateStatusTimeOut)
			if err != nil {
				log.Errorf(err.Error())
			}
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

type tmpDeviceMeta struct {
	deviceId string
	nodeType api.NodeType
	addr     string
}

func (v *Validate) validateMapping(validatorList []string) (map[string][]*tmpDeviceMeta, error) {
	result := make(map[string][]*tmpDeviceMeta)
	edges := v.nodeManager.GetAllEdge()
	for _, edgeNode := range edges {
		tn := new(tmpDeviceMeta)
		tn.nodeType = api.NodeEdge
		tn.deviceId = edgeNode.DeviceInfo.DeviceId
		tn.addr = edgeNode.Node.Addr

		validatorID := validatorList[randomNum(0, len(validatorList))]

		if validated, ok := result[validatorID]; ok {
			validated = append(validated, tn)
			result[validatorID] = validated
		} else {
			vd := make([]*tmpDeviceMeta, 0)
			vd = append(vd, tn)
			result[validatorID] = vd
		}
	}

	candidates := v.nodeManager.GetAllCandidate()
	for _, candidateNode := range candidates {
		tn := new(tmpDeviceMeta)
		tn.deviceId = candidateNode.DeviceInfo.DeviceId
		tn.nodeType = api.NodeCandidate
		tn.addr = candidateNode.Node.Addr

		validatorID := differentValue(validatorList, candidateNode.DeviceInfo.DeviceId)

		if validated, ok := result[validatorID]; ok {
			validated = append(validated, tn)
			result[validatorID] = validated
		} else {
			vd := make([]*tmpDeviceMeta, 0)
			vd = append(vd, tn)
			result[validatorID] = vd
		}
	}

	if len(result) == 0 {
		return nil, fmt.Errorf("%s", "edge node and candidate node are empty")
	}

	return result, nil
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

	v.roundID = sID
	v.seed = time.Now().UnixNano()
	v.maxFidMap = make(map[string]int64)

	validatorList, err := cache.GetDB().GetValidatorsWithList()
	if err != nil {
		return err
	}
	log.Debug("validator is ", validatorList)

	// no successful election
	if validatorList == nil || len(validatorList) == 0 {
		return nil
	}

	validatorMap, err := v.validateMapping(validatorList)
	if err != nil {
		return err
	}

	for validatorID, validatedList := range validatorMap {

		req := v.assemblyReqValidates(validatorID, validatedList)

		validator := v.nodeManager.GetCandidateNode(validatorID)
		if validator == nil {
			log.Warnf("validator [%s] is null", validatorID)
			continue
		}

		err = validator.NodeAPI.ValidateBlocks(v.ctx, req)
		if err != nil {
			log.Errorf(err.Error())
			continue
		}

		log.Infof("validatorID :%s, List:%v", validatorID, validatedList)
	}

	go func() {
		time.Sleep(time.Duration(v.duration*2) * time.Second)
		err := v.checkValidateTimeOut()
		if err != nil {
			log.Errorf(err.Error())
			return
		}
	}()

	return nil
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

func (v *Validate) SetValidateSwitch(open bool) {
	v.open = open
}

func (v *Validate) GetValidateRunningState() bool {
	return v.open
}
