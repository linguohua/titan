package validator

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/robfig/cron"
	"golang.org/x/xerrors"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/node/cidutil"
	"github.com/linguohua/titan/node/scheduler/node"

	"github.com/linguohua/titan/api"
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
	curRoundID  string
	crontab     *cron.Cron // timer
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
		e := v.startValidate()
		if e != nil {
			log.Errorf("verification failed to open %s", e.Error())
		}
	})
	if err != nil {
		log.Panicf(err.Error())
	}

	v.crontab.Start()
}

func (v *Validator) startValidate() error {
	roundID := uuid.NewString()
	v.curRoundID = roundID
	v.seed = time.Now().UnixNano()

	// before opening validation
	// check the last round of verification
	// err = v.checkValidateTimeOut()
	// if err != nil {
	// 	log.Errorf(err.Error())
	// 	return err
	// }

	return v.execute()
}

// func (v *Validator) checkValidateTimeOut() error {
// 	nodeIDs, err := v.nodeManager.NodeMgrDB.GetNodesWithVerifyingList("Server_ID")
// 	if err != nil {
// 		return err
// 	}

// 	if nodeIDs != nil && len(nodeIDs) > 0 {
// 		err = v.nodeManager.NodeMgrDB.SetValidateTimeoutOfNodes(v.curRoundID-1, nodeIDs)
// 		if err != nil {
// 			log.Errorf(err.Error())
// 		}
// 	}

// 	return err
// }

func (v *Validator) execute() error {
	err := v.nodeManager.NodeMgrDB.RemoveVerifyingList(v.nodeManager.ServerID)
	if err != nil {
		return err
	}

	validatorList, err := v.nodeManager.NodeMgrDB.GetValidatorsWithList(v.nodeManager.ServerID)
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

func (v *Validator) getValidatedList(validatorMap map[string]float64) []*validatedNodeInfo {
	validatedList := make([]*validatedNodeInfo, 0)
	v.nodeManager.EdgeNodes.Range(func(key, value interface{}) bool {
		edgeNode := value.(*node.Edge)
		info := &validatedNodeInfo{}
		info.nodeType = api.NodeEdge
		info.nodeID = edgeNode.NodeID
		info.addr = edgeNode.BaseInfo.RPCURL()
		info.bandwidth = edgeNode.BandwidthUp
		validatedList = append(validatedList, info)
		return true
	})
	v.nodeManager.CandidateNodes.Range(func(key, value interface{}) bool {
		candidateNode := value.(*node.Candidate)
		if _, exist := validatorMap[candidateNode.NodeID]; exist {
			return true
		}
		info := &validatedNodeInfo{}
		info.nodeID = candidateNode.NodeID
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
			nodeID := validatorList[index]
			bandwidthDown, exist := validatorMap[nodeID]
			if !exist {
				continue
			}

			if bandwidthDown >= bandwidthUp {
				validatorMap[nodeID] -= bandwidthUp
				return nodeID
			}
		}

		return ""
	}

	infos := make([]*api.ValidateResultInfo, 0)
	validateds := make([]string, 0)

	for i, validated := range validatedList {
		reqValidate, err := v.getNodeReqValidate(validated)
		if err != nil {
			// log.Errorf("node:%s , getNodeReqValidate err:%s", validated.nodeID, err.Error())
			continue
		}

		validateds = append(validateds, validated.nodeID)

		validatorID := findValidator(i, validated.bandwidth)
		list, exist := validateReqs[validatorID]
		if !exist {
			list = make([]api.ReqValidate, 0)
		}
		list = append(list, reqValidate)

		validateReqs[validatorID] = list

		info := &api.ValidateResultInfo{
			RoundID:     v.curRoundID,
			NodeID:      validated.nodeID,
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
		err = v.nodeManager.NodeMgrDB.SetNodesToVerifyingList(validateds, v.nodeManager.ServerID)
		if err != nil {
			log.Errorf("SetNodesToVerifyingList err:%s", err.Error())
		}
	}

	return validateReqs
}

func (v *Validator) getNodeReqValidate(validated *validatedNodeInfo) (api.ReqValidate, error) {
	req := api.ReqValidate{
		RandomSeed: v.seed,
		NodeURL:    validated.addr,
		Duration:   duration,
		RoundID:    v.curRoundID,
		NodeType:   int(validated.nodeType),
	}

	hash, err := v.nodeManager.CarfileDB.RandomCarfileFromNode(validated.nodeID)
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

type validatedNodeInfo struct {
	nodeID    string
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
func (v *Validator) updateFailValidateResult(nodeID string, status api.ValidateStatus) error {
	resultInfo := &api.ValidateResultInfo{RoundID: v.curRoundID, NodeID: nodeID, Status: status}
	return v.nodeManager.NodeMgrDB.UpdateValidateResultInfo(resultInfo)
}

// updateSuccessValidateResult update validator result info
func (v *Validator) updateSuccessValidateResult(validateResult *api.ValidatedResult) error {
	resultInfo := &api.ValidateResultInfo{
		RoundID:     validateResult.RoundID,
		NodeID:      validateResult.NodeID,
		BlockNumber: int64(len(validateResult.Cids)),
		Status:      api.ValidateStatusSuccess,
		Bandwidth:   validateResult.Bandwidth,
		Duration:    validateResult.CostTime,
	}
	return v.nodeManager.NodeMgrDB.UpdateValidateResultInfo(resultInfo)
}

// Result node validator result
func (v *Validator) Result(validateResult *api.ValidatedResult) error {
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
			err = v.updateFailValidateResult(validateResult.NodeID, status)
		}
		if err != nil {
			log.Errorf("UpdateValidateResult [%s] fail : %s", validateResult.NodeID, err.Error())
		}

		err = v.nodeManager.NodeMgrDB.RemoveValidatedWithList(validateResult.NodeID, v.nodeManager.ServerID)
		if err != nil {
			log.Errorf("RemoveValidatedWithList [%s] fail : %s", validateResult.NodeID, err.Error())
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
	for _, nodeID := range candidates {
		node := v.nodeManager.GetCandidateNode(nodeID)
		if node == nil {
			continue
		}

		cCidMap, err = node.API().GetBlocksOfCarfile(context.Background(), validateResult.CarfileCID, v.seed, max)
		if err != nil {
			log.Errorf("candidate %s GetBlocksOfCarfile err:%s", nodeID, err.Error())
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
			log.Errorf("round [%d] and nodeID [%s], validator fail resultCid:%s, vCid:%s,randNum:%d,index:%d", validateResult.RoundID, validateResult.NodeID, resultCid, vCid, randNum, i)
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
	count, err := v.nodeManager.NodeMgrDB.CountVerifyingNode(v.nodeManager.ServerID)
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
