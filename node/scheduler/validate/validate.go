package validate

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/robfig/cron"
	"golang.org/x/xerrors"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/node/cidutil"
	"github.com/linguohua/titan/node/scheduler/node"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/scheduler/db/cache"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
)

var log = logging.Logger("scheduler/validate")

const (
	duration = 10 // Verification time (Unit:Second)
	interval = 5  // validate start-up time interval (Unit:minute)
)

// Validate Validate
type Validate struct {
	nodeManager *node.Manager
	ctx         context.Context
	lock        sync.Mutex
	seed        int64
	curRoundID  int64
	crontab     *cron.Cron // timer
	enable      bool       // validate switch
}

// NewValidate new validate
func NewValidate(manager *node.Manager, enable bool) *Validate {
	e := &Validate{
		ctx:         context.Background(),
		nodeManager: manager,
		crontab:     cron.New(),
		enable:      enable,
	}

	e.initValidateTask()

	return e
}

// validation task scheduled initialization
func (v *Validate) initValidateTask() {
	spec := fmt.Sprintf("0 */%d * * * *", interval)
	err := v.crontab.AddFunc(spec, func() {
		if !v.enable {
			return
		}

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

	roundID, err := cache.GetDB().IncrValidateRoundID()
	if err != nil {
		return err
	}
	v.curRoundID = roundID
	v.seed = time.Now().UnixNano()

	// before opening validation
	// check the last round of verification
	err = v.checkValidateTimeOut()
	if err != nil {
		log.Errorf(err.Error())
		return err
	}

	err = v.execute()
	if err != nil {
		log.Errorf(err.Error())
	}
	return err
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
		return xerrors.New("validator list is null")
	}

	log.Debug("validator is", validatorList)

	validatorMap := v.assignValidator(validatorList)
	if validatorMap == nil {
		return xerrors.New("assignValidator map is null")
	}

	for validatorID, reqList := range validatorMap {
		v.sendValidateInfoToValidator(validatorID, reqList)
	}

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

func (v *Validate) getValidatedList(validatorMap map[string]float64) []*validatedDeviceInfo {
	validatedList := make([]*validatedDeviceInfo, 0)
	v.nodeManager.EdgeNodeMap.Range(func(key, value interface{}) bool {
		edgeNode := value.(*node.EdgeNode)
		info := &validatedDeviceInfo{}
		info.nodeType = api.NodeEdge
		info.deviceID = edgeNode.DeviceID
		info.addr = edgeNode.Node.GetRPCURL()
		info.bandwidth = edgeNode.BandwidthUp
		validatedList = append(validatedList, info)
		return true
	})
	v.nodeManager.CandidateNodeMap.Range(func(key, value interface{}) bool {
		candidateNode := value.(*node.CandidateNode)
		if _, exist := validatorMap[candidateNode.DeviceID]; exist {
			return true
		}
		info := &validatedDeviceInfo{}
		info.deviceID = candidateNode.DeviceID
		info.nodeType = api.NodeCandidate
		info.addr = candidateNode.Node.GetRPCURL()
		info.bandwidth = candidateNode.BandwidthUp
		validatedList = append(validatedList, info)
		return true
	})

	return validatedList
}

func (v *Validate) assignValidator(validatorList []string) map[string][]api.ReqValidate {
	validateReqs := make(map[string][]api.ReqValidate)

	validatorMap := make(map[string]float64)
	for _, id := range validatorList {
		value, exist := v.nodeManager.CandidateNodeMap.Load(id)
		if exist {
			candidateNode := value.(*node.CandidateNode)
			validatorMap[id] = candidateNode.BandwidthDown
		}
	}

	validatedList := v.getValidatedList(validatorMap)
	if len(validatedList) <= 0 {
		return nil
	}

	findValidator := func(offset int, bandwidthUp float64) string {
		vLen := len(validatorList)
		for i := offset; i < offset+vLen; i++ {
			index := i % vLen
			deviceID := validatorList[index]
			bandwidthDown, exist := validatorMap[deviceID]
			if !exist {
				continue
			}

			if bandwidthDown >= bandwidthUp {
				validatorMap[deviceID] -= bandwidthUp
				return deviceID
			}
		}

		return ""
	}

	infos := make([]*persistent.ValidateResult, 0)
	validateds := make([]string, 0)

	for i, validated := range validatedList {
		reqValidate, err := v.getNodeReqValidate(validated)
		if err != nil {
			log.Errorf("node:%s , getNodeReqValidate err:%s", validated.deviceID, err.Error())
			continue
		}

		validateds = append(validateds, validated.deviceID)

		validatorID := findValidator(i, validated.bandwidth)
		list, exist := validateReqs[validatorID]
		if !exist {
			list = make([]api.ReqValidate, 0)
		}
		list = append(list, reqValidate)

		validateReqs[validatorID] = list

		info := &persistent.ValidateResult{
			RoundID:     v.curRoundID,
			DeviceID:    validated.deviceID,
			ValidatorID: validatorID,
			StartTime:   time.Now(),
			Status:      int(persistent.ValidateStatusCreate),
		}
		infos = append(infos, info)
	}

	err := persistent.GetDB().AddValidateResultInfos(infos)
	if err != nil {
		log.Errorf("AddValidateResultInfos err:%s", err.Error())
		return nil
	}

	if len(validateds) > 0 {
		err = cache.GetDB().SetNodesToVerifyingList(validateds)
		if err != nil {
			log.Errorf("SetNodesToVerifyingList err:%s", err.Error())
		}
	}

	return validateReqs
}

func (v *Validate) getNodeReqValidate(validated *validatedDeviceInfo) (api.ReqValidate, error) {
	req := api.ReqValidate{
		RandomSeed: v.seed,
		NodeURL:    validated.addr,
		Duration:   duration,
		RoundID:    v.curRoundID,
		NodeType:   int(validated.nodeType),
	}

	hash, err := persistent.GetDB().GetRandCarfileWithNode(validated.deviceID)
	if err != nil {
		// log.Warnf("GetRandCarfileWithNode err: %s", err.Error())
		return req, err
	}

	// log.Warnf("hash: %s", hash)
	cid, err := cidutil.HashString2CIDString(hash)
	if err != nil {
		log.Warnf("HashString2CidString err: %s", err.Error())
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

// updateFailValidateResult update validate result info
func (v *Validate) updateFailValidateResult(roundID int64, deviceID string, status persistent.ValidateStatus) error {
	resultInfo := &persistent.ValidateResult{RoundID: roundID, DeviceID: deviceID, Status: status.Int(), EndTime: time.Now()}
	return persistent.GetDB().UpdateFailValidateResultInfo(resultInfo)
}

// updateSuccessValidateResult update validate result info
func (v *Validate) updateSuccessValidateResult(validateResults *api.ValidateResults) error {
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

// ValidateResult node validate result
func (v *Validate) ValidateResult(validateResult *api.ValidateResults) error {
	if validateResult.RoundID != v.curRoundID {
		return xerrors.Errorf("round id does not match")
	}

	// log.Debugf("validate result : %+v", *validateResult)

	var status persistent.ValidateStatus

	defer func() {
		var err error
		if status == persistent.ValidateStatusSuccess {
			err = v.updateSuccessValidateResult(validateResult)
		} else {
			err = v.updateFailValidateResult(validateResult.RoundID, validateResult.DeviceID, status)
		}
		if err != nil {
			log.Errorf("UpdateValidateResult [%s] fail : %s", validateResult.DeviceID, err.Error())
		}

		err = cache.GetDB().RemoveValidatedWithList(validateResult.DeviceID)
		if err != nil {
			log.Errorf("RemoveValidatedWithList [%s] fail : %s", validateResult.DeviceID, err.Error())
			return
		}
	}()

	if validateResult.IsCancel {
		status = persistent.ValidateStatusCancel
		return nil
	}

	if validateResult.IsTimeout {
		status = persistent.ValidateStatusTimeOut
		return nil
	}

	hash, err := cidutil.CIDString2HashString(validateResult.CarfileCID)
	if err != nil {
		status = persistent.ValidateStatusOther
		log.Errorf("handleValidateResult CIDString2HashString %s, err:%s", validateResult.CarfileCID, err.Error())
		return nil
	}

	candidates, err := persistent.GetDB().GetCachesWithCandidate(hash)
	if err != nil {
		status = persistent.ValidateStatusOther
		log.Errorf("handleValidateResult GetCaches %s , err:%s", validateResult.CarfileCID, err.Error())
		return nil
	}

	max := len(validateResult.Cids)
	var cCidMap map[int]string
	for _, deviceID := range candidates {
		node := v.nodeManager.GetCandidateNode(deviceID)
		if node == nil {
			continue
		}

		// log.Infof("candidate : %s , seed: %d , max:%d", node.DeviceId, v.seed, max)

		cCidMap, err = node.GetAPI().GetBlocksOfCarfile(context.Background(), validateResult.CarfileCID, v.seed, max)
		if err != nil {
			log.Errorf("candidate %s GetBlocksOfCarfile err:%s", deviceID, err.Error())
			continue
		}

		break
	}

	if len(cCidMap) <= 0 {
		status = persistent.ValidateStatusOther
		log.Errorf("handleValidateResult candidate map is nil , %s", validateResult.CarfileCID)
		return nil
	}

	carfileRecord, err := persistent.GetDB().GetCarfileInfo(hash)
	if err != nil {
		status = persistent.ValidateStatusOther
		log.Errorf("handleValidateResult GetCarfileInfo %s , err:%s", validateResult.CarfileCID, err.Error())
		return nil
	}

	r := rand.New(rand.NewSource(v.seed))
	// do validate
	for i := 0; i < max; i++ {
		resultCid := validateResult.Cids[i]
		randNum := v.getRandNum(carfileRecord.TotalBlocks, r)
		vCid := cCidMap[randNum]

		// TODO Penalize the candidate if vCid error

		if !v.compareCid(resultCid, vCid) {
			status = persistent.ValidateStatusFail
			log.Errorf("round [%d] and deviceID [%s], validate fail resultCid:%s, vCid:%s,randNum:%d,index:%d", validateResult.RoundID, validateResult.DeviceID, resultCid, vCid, randNum, i)
			return nil
		}
	}

	status = persistent.ValidateStatusSuccess
	return nil
}

func (v *Validate) compareCid(cidStr1, cidStr2 string) bool {
	hash1, err := cidutil.CIDString2HashString(cidStr1)
	if err != nil {
		return false
	}

	hash2, err := cidutil.CIDString2HashString(cidStr2)
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
	count, err := cache.GetDB().CountVerifyingNode()
	if err != nil {
		return err
	}

	if count > 0 {
		return fmt.Errorf("validation in progress, cannot start again")
	}

	go func() {
		time.Sleep(time.Duration(interval) * time.Minute)
		v.crontab.Start()
	}()

	// v.enable = true
	v.crontab.Stop()

	return v.startValidate()
}
