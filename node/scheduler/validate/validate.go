package validate

import (
	"container/list"
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/robfig/cron"
	"golang.org/x/xerrors"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/node/scheduler/node"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/helper"
	"github.com/linguohua/titan/node/scheduler/db/cache"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
)

var log = logging.Logger("scheduler/validate")

// Validate Validate
type Validate struct {
	ctx        context.Context
	mu         sync.Mutex
	seed       int64
	curRoundID int64
	duration   int

	// validate start-up time interval (minute)
	interval int

	// fid is maximum value of each device storage record
	// key is device id
	// value is fid
	maxFidMap sync.Map

	// temporary storage of call back message
	resultQueue *list.List
	// heartbeat of call back
	resultChannel chan bool

	nodeManager *node.Manager

	// timer
	crontab *cron.Cron
	// validate is not running
	running bool
	// validate switch
	enable bool
}

// NewValidate new validate
func NewValidate(manager *node.Manager, enable bool) *Validate {
	e := &Validate{
		ctx:           context.Background(),
		duration:      10,
		interval:      5,
		resultQueue:   list.New(),
		resultChannel: make(chan bool, 1),
		nodeManager:   manager,
		crontab:       cron.New(),
		enable:        enable,
	}

	e.initValidateTask()
	// handle validate result data
	go e.initValidateResultTask()

	return e
}

// validation task scheduled initialization
func (v *Validate) initValidateTask() {
	spec := fmt.Sprintf("0 */%d * * * *", v.interval)
	err := v.crontab.AddFunc(spec, func() {
		err := v.startValidate()
		if err != nil {
			log.Errorf("verification failed to open")
		}
	})

	if err != nil {
		log.Panicf(err.Error())
	}

	v.crontab.Start()
}

func (v *Validate) startValidate() error {
	// log.Info("=======>> start validate <<=======")

	cur, err := cache.GetDB().IncrValidateRoundID()
	if err != nil {
		return err
	}
	v.curRoundID = cur
	v.seed = time.Now().UnixNano()

	// before opening validation
	// check the last round of verification
	err = v.checkValidateTimeOut()
	if err != nil {
		log.Errorf(err.Error())
		return err
	}

	if !v.enable {
		v.running = false
		return nil
	}

	err = v.execute()
	if err != nil {
		v.running = false
		log.Errorf(err.Error())
		return err
	}
	return nil
}

func (v *Validate) checkValidateTimeOut() error {
	deviceIDs, err := cache.GetDB().GetNodesWithVerifyingList()
	if err != nil {
		return err
	}

	if deviceIDs != nil && len(deviceIDs) > 0 {
		result := &persistent.ValidateResult{
			RoundID: v.curRoundID - 1,
			Status:  persistent.ValidateStatusTimeOut.Int(),
			EndTime: time.Now(),
		}
		err = persistent.GetDB().SetTimeoutToValidateInfos(result, deviceIDs)
		if err != nil {
			log.Errorf(err.Error())
		}
	}

	return err
}

func (v *Validate) execute() error {
	v.running = true

	err := cache.GetDB().RemoveVerifyingList()
	if err != nil {
		return err
	}

	validatorList, err := cache.GetDB().GetValidatorsWithList()
	if err != nil {
		return err
	}

	// no successful election
	if validatorList == nil || len(validatorList) == 0 {
		log.Warn("validator list is null")
		return nil
	}

	log.Debug("validator is", validatorList)

	validatorMap := v.assignValidator(validatorList)

	for validatorID, reqList := range validatorMap {
		v.sendValidateInfoToValidator(validatorID, reqList)
	}

	// validatorMap, err := v.validateMappingByUniformAlgorithm(validatorList)
	// if err != nil {
	// 	return err
	// }

	// for validatorID, validatedList := range validatorMap {
	// 	go func(vId string, list []*validatedDeviceInfo) {
	// 		log.Debugf("validator id : %s, validated list : %v", vId, list)
	// 		req := v.assemblyValidateReqAndStore(vId, list)

	// 		validator := v.nodeManager.GetCandidateNode(vId)
	// 		if validator == nil {
	// 			log.Errorf("validator [%s] is null", vId)
	// 			return
	// 		}

	// 		err = validator.GetAPI().ValidateNodes(v.ctx, req)
	// 		if err != nil {
	// 			log.Errorf(err.Error())
	// 			return
	// 		}
	// 		log.Debugf("validator id : %s, send success", vId)

	// 	}(validatorID, validatedList)
	// }
	return nil
}

func (v *Validate) sendValidateInfoToValidator(validatorID string, reqList []api.ReqValidate) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	validator := v.nodeManager.GetCandidateNode(validatorID)
	if validator == nil {
		log.Errorf("validator [%s] is null", validatorID)
		return
	}

	err := validator.GetAPI().ValidateNodes(ctx, reqList)
	if err != nil {
		log.Errorf("ValidateNodes [%s] err:%s", validatorID, err.Error())
		return
	}

}

func (v *Validate) assignValidator(validatorList []string) map[string][]api.ReqValidate {
	result := make(map[string][]api.ReqValidate)

	validatorMp := make(map[string]float64)
	for _, id := range validatorList {
		value, ok := v.nodeManager.CandidateNodeMap.Load(id)
		if ok {
			candidateNode := value.(*node.CandidateNode)
			validatorMp[id] = candidateNode.BandwidthDown
		}
	}

	validatedList := make([]*validatedDeviceInfo, 0)
	v.nodeManager.EdgeNodeMap.Range(func(key, value interface{}) bool {
		edgeNode := value.(*node.EdgeNode)
		info := &validatedDeviceInfo{}
		info.nodeType = api.NodeEdge
		info.deviceID = edgeNode.DeviceId
		info.addr = edgeNode.Node.GetAddress()
		info.bandwidth = edgeNode.BandwidthUp
		validatedList = append(validatedList, info)
		return true
	})
	v.nodeManager.CandidateNodeMap.Range(func(key, value interface{}) bool {
		candidateNode := value.(*node.CandidateNode)
		if _, ok := validatorMp[candidateNode.DeviceId]; ok {
			return true
		}
		info := &validatedDeviceInfo{}
		info.deviceID = candidateNode.DeviceId
		info.nodeType = api.NodeCandidate
		info.addr = candidateNode.Node.GetAddress()
		info.bandwidth = candidateNode.BandwidthUp
		validatedList = append(validatedList, info)
		return true
	})

	vLen := len(validatorList)
	getValidator := func(index int, bandwidthUp float64) string {
		i := index % vLen
		for j := i; j < vLen; j++ {
			deviceID := validatorList[j]
			bandwidthDown, ok := validatorMp[deviceID]
			if !ok {
				continue
			}

			if bandwidthDown >= bandwidthUp {
				validatorMp[deviceID] -= bandwidthUp
				return deviceID
			}
		}

		return ""
	}

	for i, validated := range validatedList {
		reqValidate, err := v.getNodeReqValidate(validated)
		if err != nil {
			log.Errorf("node:%s , getNodeReqValidate err:%s", validated.deviceID, err.Error())
			continue
		}

		validatorID := getValidator(i, validated.bandwidth)
		list, exit := result[validatorID]
		if !exit {
			list = make([]api.ReqValidate, 0)
		}
		list = append(list, reqValidate)

		result[validatorID] = list
	}

	return result
}

func (v *Validate) getNodeReqValidate(validated *validatedDeviceInfo) (api.ReqValidate, error) {
	req := api.ReqValidate{
		RandomSeed: v.seed,
		NodeURL:    validated.addr,
		Duration:   v.duration,
		RoundID:    v.curRoundID,
		NodeType:   int(validated.nodeType)}

	hash, err := persistent.GetDB().GetRandCarfileWithNode(validated.deviceID)
	if err != nil {
		return req, err
	}

	cid, err := helper.HashString2CidString(hash)
	if err != nil {
		return req, err
	}
	req.CarfileCID = cid

	return req, nil
}

type validatedDeviceInfo struct {
	deviceID  string
	nodeType  api.NodeType
	addr      string
	bandwidth float64
}

// // verified edge node are allocated to verifiers one by one
// func (v *Validate) validateMapping(validatorList []string) (map[string][]validatedDeviceInfo, error) {
// 	result := make(map[string][]validatedDeviceInfo)
// 	vm := make(map[string]struct{})
// 	for _, v := range validatorList {
// 		vm[v] = struct{}{}
// 	}

// 	v.nodeManager.EdgeNodeMap.Range(func(key, value interface{}) bool {
// 		edgeNode := value.(*node.EdgeNode)
// 		var tn validatedDeviceInfo
// 		tn.nodeType = api.NodeEdge
// 		tn.deviceID = edgeNode.DeviceId
// 		tn.addr = edgeNode.Node.GetAddress()

// 		validatorID := validatorList[v.generatorForRandomNumber(0, len(validatorList))]

// 		if validated, exist := result[validatorID]; exist {
// 			validated = append(validated, tn)
// 			result[validatorID] = validated
// 		} else {
// 			vd := make([]validatedDeviceInfo, 0)
// 			vd = append(vd, tn)
// 			result[validatorID] = vd
// 		}

// 		return true
// 	})

// 	// candidates := v.nodeManager.GetAllCandidate()
// 	v.nodeManager.CandidateNodeMap.Range(func(key, value interface{}) bool {
// 		candidateNode := value.(*node.CandidateNode)
// 		// the verifier is not verified
// 		if _, ok := vm[candidateNode.DeviceId]; ok {
// 			return true
// 		}
// 		var tn validatedDeviceInfo
// 		tn.deviceID = candidateNode.DeviceId
// 		tn.nodeType = api.NodeCandidate
// 		tn.addr = candidateNode.Node.GetAddress()

// 		validatorID := validatorList[v.generatorForRandomNumber(0, len(validatorList))]
// 		if validated, exist := result[validatorID]; exist {
// 			validated = append(validated, tn)
// 			result[validatorID] = validated
// 		} else {
// 			vd := make([]validatedDeviceInfo, 0)
// 			vd = append(vd, tn)
// 			result[validatorID] = vd
// 		}

// 		return true
// 	})

// 	if len(result) == 0 {
// 		return nil, fmt.Errorf("%s", "edge node and candidate node are empty")
// 	}

// 	return result, nil
// }

// func (v *Validate) validateMappingByUniformAlgorithm(validatorList []string) (map[string][]*validatedDeviceInfo, error) {
// 	result := make(map[string][]*validatedDeviceInfo)

// 	validatorMp := make(map[string]float64)
// 	for _, id := range validatorList {
// 		value, ok := v.nodeManager.CandidateNodeMap.Load(id)
// 		if ok {
// 			candidateNode := value.(*node.CandidateNode)
// 			validatorMp[id] = candidateNode.BandwidthDown
// 		}
// 	}

// 	validatedList := make([]*validatedDeviceInfo, 0)
// 	v.nodeManager.EdgeNodeMap.Range(func(key, value interface{}) bool {
// 		edgeNode := value.(*node.EdgeNode)
// 		info := &validatedDeviceInfo{}
// 		info.nodeType = api.NodeEdge
// 		info.deviceID = edgeNode.DeviceId
// 		info.addr = edgeNode.Node.GetAddress()
// 		info.bandwidth = edgeNode.BandwidthUp
// 		validatedList = append(validatedList, info)
// 		return true
// 	})
// 	v.nodeManager.CandidateNodeMap.Range(func(key, value interface{}) bool {
// 		candidateNode := value.(*node.CandidateNode)
// 		if _, ok := validatorMp[candidateNode.DeviceId]; ok {
// 			return true
// 		}
// 		info := &validatedDeviceInfo{}
// 		info.deviceID = candidateNode.DeviceId
// 		info.nodeType = api.NodeCandidate
// 		info.addr = candidateNode.Node.GetAddress()
// 		info.bandwidth = candidateNode.BandwidthUp
// 		validatedList = append(validatedList, info)
// 		return true
// 	})

// 	if len(validatedList) == 0 {
// 		return nil, fmt.Errorf("%s", "no verified person")
// 	}

// 	// based on facts BandwidthDown >> BandwidthUp, Don't judge BandwidthUp >= BandwidthDown
// 	switch {
// 	case len(validatorList) >= len(validatedList):
// 		for k, vd := range validatedList {
// 			result[validatorList[k]] = []*validatedDeviceInfo{vd}
// 		}
// 	default:
// 		avgList := GetAverageArray(validatedList, len(validatorList))
// 		for k, vl := range avgList {
// 			curList := vl
// 			var sum float64
// 			for _, validate := range curList {
// 				sum += validate.bandwidth
// 			}

// 			realList := curList
// 			if validatorMp[validatorList[k]] < sum {
// 				delta := sum - validatorMp[validatorList[k]]
// 				// index := 0
// 				// todo: add store not verified
// 				for ck, value := range curList {
// 					delta = delta - value.bandwidth
// 					if delta <= 0 {
// 						// index = k+1
// 						realList = curList[ck+1:]
// 						break
// 					}
// 				}
// 			}

// 			result[validatorList[k]] = realList
// 		}
// 	}

// 	return result, nil
// }

func (v *Validate) generatorForRandomNumber(start, end int) int {
	max := end - start
	if max <= 0 {
		return start
	}
	rand.Seed(time.Now().UnixNano())
	y := rand.Intn(max)
	return start + y
}

func (v *Validate) getRandNum(max int, r *rand.Rand) int {
	if max > 0 {
		return r.Intn(max)
	}

	return max
}

// UpdateFailValidateResult update validate result info
func (v *Validate) UpdateFailValidateResult(roundID int64, deviceID string, status persistent.ValidateStatus) error {
	resultInfo := &persistent.ValidateResult{RoundID: roundID, DeviceID: deviceID, Status: status.Int(), EndTime: time.Now()}
	return persistent.GetDB().UpdateFailValidateResultInfo(resultInfo)
}

// UpdateSuccessValidateResult update validate result info
func (v *Validate) UpdateSuccessValidateResult(validateResults *api.ValidateResults) error {
	resultInfo := &persistent.ValidateResult{
		RoundID:     validateResults.RoundID,
		DeviceID:    validateResults.DeviceID,
		BlockNumber: int64(len(validateResults.Cids)),
		Status:      persistent.ValidateStatusSuccess.Int(),
		Bandwidth:   validateResults.Bandwidth,
		Duration:    validateResults.CostTime,
		EndTime:     time.Now(),
	}
	return persistent.GetDB().UpdateSuccessValidateResultInfo(resultInfo)
}

func (v *Validate) initValidateResultTask() {
	for {
		<-v.resultChannel
		v.iterationValidateResult()
	}
}

func (v *Validate) iterationValidateResult() {
	for v.resultQueue.Len() > 0 {
		// take out first element
		v.mu.Lock()
		element := v.resultQueue.Front()
		v.mu.Unlock()
		if element == nil {
			return
		}
		if validateResults, ok := element.Value.(*api.ValidateResults); ok {
			err := v.handleValidateResult(validateResults)
			if err != nil {
				log.Errorf("deviceId[%s] handle validate result fail : %s", validateResults.DeviceID, err.Error())
			}
		}
		// dequeue
		v.mu.Lock()
		v.resultQueue.Remove(element)
		v.mu.Unlock()
	}
}

// PushResultToQueue add result info to queue
func (v *Validate) PushResultToQueue(validateResults *api.ValidateResults) {
	v.mu.Lock()
	v.resultQueue.PushBack(validateResults)
	v.mu.Unlock()
	v.resultChannel <- true
}

func (v *Validate) handleValidateResult(validateResults *api.ValidateResults) error {
	if validateResults.RoundID != v.curRoundID {
		return xerrors.Errorf("round id mismatch")
	}

	log.Debugf("validate result : %+v", *validateResults)

	defer func() {
		err := cache.GetDB().RemoveNodeWithVerifyingList(validateResults.DeviceID)
		if err != nil {
			log.Errorf("remove edge node [%s] fail : %s", validateResults.DeviceID, err.Error())
			return
		}
		count, err := cache.GetDB().CountVerifyingNode(v.ctx)
		if err != nil {
			log.Error("CountVerifyingNode fail :", err.Error())
			return
		}
		if count == 0 {
			v.running = false
		}
	}()

	if validateResults.IsCancel {
		return v.UpdateFailValidateResult(validateResults.RoundID, validateResults.DeviceID, persistent.ValidateStatusCancel)
	}

	if validateResults.IsTimeout {
		return v.UpdateFailValidateResult(validateResults.RoundID, validateResults.DeviceID, persistent.ValidateStatusTimeOut)
	}

	// r := rand.New(rand.NewSource(v.seed))
	// cidLength := len(validateResults.Cids)

	// if cidLength <= 0 || validateResults.RandomCount <= 0 {
	// 	log.Errorf("round [%d] and deviceID [%s], %s", validateResults.RoundID, validateResults.DeviceID, "validate result is null or random count is 0")
	// 	return v.UpdateFailValidateResult(validateResults.RoundID, validateResults.DeviceID, persistent.ValidateStatusFail)
	// }

	// cacheInfos, err := persistent.GetDB().GetBlocksFID(validateResults.DeviceID)
	// if err != nil || len(cacheInfos) <= 0 {
	// 	log.Errorf("round [%d] and deviceID [%s], failed to query : %s", validateResults.RoundID, validateResults.DeviceID, err.Error())
	// 	return v.UpdateFailValidateResult(validateResults.RoundID, validateResults.DeviceID, persistent.ValidateStatusOther)
	// }

	// mFValue, _ := v.maxFidMap.Load(validateResults.DeviceID)
	// maxFid := mFValue.(int64)

	// for index := 0; index < validateResults.RandomCount; index++ {
	// 	fid := v.getRandNum(int(maxFid), r) + 1
	// 	resultCid := validateResults.Cids[index]

	// 	cid := cacheInfos[fid]
	// 	if cid == "" {
	// 		continue
	// 	}

	// 	if !v.compareCid(cid, resultCid) {
	// 		log.Errorf("round [%d] and deviceID [%s], validate fail resultCid:%s, cid_db:%s,fid:%d,index:%d", validateResults.RoundID, validateResults.DeviceID, resultCid, cid, fid, index)
	// 		return v.UpdateFailValidateResult(validateResults.RoundID, validateResults.DeviceID, persistent.ValidateStatusFail)
	// 	}
	// }

	return v.UpdateSuccessValidateResult(validateResults)
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

// EnableValidate enable validate task
func (v *Validate) EnableValidate(enable bool) {
	v.enable = enable
}

// IsEnable is validate task enable
func (v *Validate) IsEnable() bool {
	return v.enable
}

// StartValidateOnceTask start validate task
func (v *Validate) StartValidateOnceTask() error {
	if v.running {
		return fmt.Errorf("validation in progress, cannot start again")
	}

	go func() {
		time.Sleep(time.Duration(v.interval) * time.Minute)
		v.crontab.Start()
	}()

	v.enable = true
	v.crontab.Stop()

	return v.startValidate()
}
