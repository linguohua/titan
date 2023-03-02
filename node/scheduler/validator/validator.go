package validator

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
)

var log = logging.Logger("scheduler/validator")

const (
	duration = 10 // Verification time (Unit:Second)
	interval = 5  // validator start-up time interval (Unit:minute)
)

// Validator Validator
type Validator struct {
	nodeManager *node.Manager
	ctx         context.Context
	lock        sync.Mutex
	seed        int64
	curRoundID  int64
	crontab     *cron.Cron // timer
	cache       *cache.NodeMgrCache
}

// NewValidator return new validator instance
func NewValidator(manager *node.Manager) *Validator {
	e := &Validator{
		ctx:         context.Background(),
		nodeManager: manager,
		crontab:     cron.New(),
	}

	e.initValidateTask()

	return e
}

// validation task scheduled initialization
func (v *Validator) initValidateTask() {
	spec := fmt.Sprintf("0 */%d * * * *", interval)
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

func (v *Validator) startValidate() error {
	roundID, err := v.cache.IncrValidateRoundID()
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

func (v *Validator) checkValidateTimeOut() error {
	deviceIDs, err := v.cache.GetNodesWithVerifyingList()
	if err != nil {
		return err
	}

	if deviceIDs != nil && len(deviceIDs) > 0 {
		err = v.nodeManager.NodeMgrDB.SetValidateTimeoutOfNodes(v.curRoundID-1, deviceIDs)
		if err != nil {
			log.Errorf(err.Error())
		}
	}

	return err
}

func (v *Validator) execute() error {
	err := v.cache.RemoveVerifyingList()
	if err != nil {
		return err
	}

	validatorList, err := v.cache.GetValidatorsWithList()
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

func (v *Validator) sendValidateInfoToValidator(validatorID string, reqList []api.ReqValidate) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	validator := v.nodeManager.GetCandidateNode(validatorID)
	if validator == nil {
		log.Errorf("validator [%s] is null", validatorID)
		return
	}

	err := validator.API().ValidateNodes(ctx, reqList)
	if err != nil {
		log.Errorf("ValidateNodes [%s] err:%s", validatorID, err.Error())
		return
	}
}

func (v *Validator) getValidatedList(validatorMap map[string]float64) []*validatedDeviceInfo {
	validatedList := make([]*validatedDeviceInfo, 0)
	v.nodeManager.EdgeNodes.Range(func(key, value interface{}) bool {
		edgeNode := value.(*node.Edge)
		info := &validatedDeviceInfo{}
		info.nodeType = api.NodeEdge
		info.deviceID = edgeNode.DeviceID
		info.addr = edgeNode.BaseInfo.RPCURL()
		info.bandwidth = edgeNode.BandwidthUp
		validatedList = append(validatedList, info)
		return true
	})
	v.nodeManager.CandidateNodes.Range(func(key, value interface{}) bool {
		candidateNode := value.(*node.Candidate)
		if _, exist := validatorMap[candidateNode.DeviceID]; exist {
			return true
		}
		info := &validatedDeviceInfo{}
		info.deviceID = candidateNode.DeviceID
		info.nodeType = api.NodeCandidate
		info.addr = candidateNode.BaseInfo.RPCURL()
		info.bandwidth = candidateNode.BandwidthUp
		validatedList = append(validatedList, info)
		return true
	})

	return validatedList
}

func (v *Validator) assignValidator(validatorList []string) map[string][]api.ReqValidate {
	validateReqs := make(map[string][]api.ReqValidate)

	validatorMap := make(map[string]float64)
	for _, id := range validatorList {
		value, exist := v.nodeManager.CandidateNodes.Load(id)
		if exist {
			candidateNode := value.(*node.Candidate)
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

	infos := make([]*api.ValidateResult, 0)
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

		info := &api.ValidateResult{
			RoundID:     v.curRoundID,
			DeviceID:    validated.deviceID,
			ValidatorID: validatorID,
			StartTime:   time.Now(),
			Status:      api.ValidateStatusCreate,
		}
		infos = append(infos, info)
	}

	err := v.nodeManager.NodeMgrDB.InitValidateResultInfos(infos)
	if err != nil {
		log.Errorf("AddValidateResultInfos err:%s", err.Error())
		return nil
	}

	if len(validateds) > 0 {
		err = v.cache.SetNodesToVerifyingList(validateds)
		if err != nil {
			log.Errorf("SetNodesToVerifyingList err:%s", err.Error())
		}
	}

	return validateReqs
}

func (v *Validator) getNodeReqValidate(validated *validatedDeviceInfo) (api.ReqValidate, error) {
	req := api.ReqValidate{
		RandomSeed: v.seed,
		NodeURL:    validated.addr,
		Duration:   duration,
		RoundID:    v.curRoundID,
		NodeType:   int(validated.nodeType),
	}

	hash, err := v.nodeManager.CarfileDB.RandomCarfileFromNode(validated.deviceID)
	if err != nil {
		return req, err
	}

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

func (v *Validator) generatorForRandomNumber(start, end int) int {
	max := end - start
	if max <= 0 {
		return start
	}
	rand.Seed(time.Now().UnixNano())
	y := rand.Intn(max)
	return start + y
}

func (v *Validator) getRandNum(max int, r *rand.Rand) int {
	if max > 0 {
		return r.Intn(max)
	}

	return max
}

// updateFailValidateResult update validator result info
func (v *Validator) updateFailValidateResult(deviceID string, status api.ValidateStatus) error {
	resultInfo := &api.ValidateResult{RoundID: v.curRoundID, DeviceID: deviceID, Status: status}
	return v.nodeManager.NodeMgrDB.UpdateValidateResultInfo(resultInfo)
}

// updateSuccessValidateResult update validator result info
func (v *Validator) updateSuccessValidateResult(validateResults *api.ValidateResults) error {
	resultInfo := &api.ValidateResult{
		RoundID:     validateResults.RoundID,
		DeviceID:    validateResults.DeviceID,
		BlockNumber: int64(len(validateResults.Cids)),
		Status:      api.ValidateStatusSuccess,
		Bandwidth:   validateResults.Bandwidth,
		Duration:    validateResults.CostTime,
	}
	return v.nodeManager.NodeMgrDB.UpdateValidateResultInfo(resultInfo)
}

// ValidateResult node validator result
func (v *Validator) ValidateResult(validateResult *api.ValidateResults) error {
	if validateResult.RoundID != v.curRoundID {
		return xerrors.Errorf("round id does not match")
	}

	// log.Debugf("validator result : %+v", *validateResult)

	var status api.ValidateStatus

	defer func() {
		var err error
		if status == api.ValidateStatusSuccess {
			err = v.updateSuccessValidateResult(validateResult)
		} else {
			err = v.updateFailValidateResult(validateResult.DeviceID, status)
		}
		if err != nil {
			log.Errorf("UpdateValidateResult [%s] fail : %s", validateResult.DeviceID, err.Error())
		}

		err = v.cache.RemoveValidatedWithList(validateResult.DeviceID)
		if err != nil {
			log.Errorf("RemoveValidatedWithList [%s] fail : %s", validateResult.DeviceID, err.Error())
			return
		}
	}()

	if validateResult.IsCancel {
		status = api.ValidateStatusCancel
		return nil
	}

	if validateResult.IsTimeout {
		status = api.ValidateStatusTimeOut
		return nil
	}

	hash, err := cidutil.CIDString2HashString(validateResult.CarfileCID)
	if err != nil {
		status = api.ValidateStatusOther
		log.Errorf("handleValidateResult CIDString2HashString %s, err:%s", validateResult.CarfileCID, err.Error())
		return nil
	}

	candidates, err := v.nodeManager.CarfileDB.CandidatesWithHash(hash)
	if err != nil {
		status = api.ValidateStatusOther
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

		cCidMap, err = node.API().GetBlocksOfCarfile(context.Background(), validateResult.CarfileCID, v.seed, max)
		if err != nil {
			log.Errorf("candidate %s GetBlocksOfCarfile err:%s", deviceID, err.Error())
			continue
		}

		break
	}

	if len(cCidMap) <= 0 {
		status = api.ValidateStatusOther
		log.Errorf("handleValidateResult candidate map is nil , %s", validateResult.CarfileCID)
		return nil
	}

	carfileRecord, err := v.nodeManager.CarfileDB.LoadCarfileInfo(hash)
	if err != nil {
		status = api.ValidateStatusOther
		log.Errorf("handleValidateResult GetCarfileInfo %s , err:%s", validateResult.CarfileCID, err.Error())
		return nil
	}

	r := rand.New(rand.NewSource(v.seed))
	// do validator
	for i := 0; i < max; i++ {
		resultCid := validateResult.Cids[i]
		randNum := v.getRandNum(carfileRecord.TotalBlocks, r)
		vCid := cCidMap[randNum]

		// TODO Penalize the candidate if vCid error

		if !v.compareCid(resultCid, vCid) {
			status = api.ValidateStatusFail
			log.Errorf("round [%d] and deviceID [%s], validator fail resultCid:%s, vCid:%s,randNum:%d,index:%d", validateResult.RoundID, validateResult.DeviceID, resultCid, vCid, randNum, i)
			return nil
		}
	}

	status = api.ValidateStatusSuccess
	return nil
}

func (v *Validator) compareCid(cidStr1, cidStr2 string) bool {
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

// StartValidateOnceTask start validator task
func (v *Validator) StartValidateOnceTask() error {
	count, err := v.cache.CountVerifyingNode()
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
