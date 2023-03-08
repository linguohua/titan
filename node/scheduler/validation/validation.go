package validation

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
	"github.com/linguohua/titan/api/types"
	"github.com/linguohua/titan/node/cidutil"
	"github.com/linguohua/titan/node/scheduler/node"

	"github.com/linguohua/titan/api"
)

var log = logging.Logger("scheduler/validation")

const (
	duration = 10 // Verification time (Unit:Second)
	interval = 5  // validator start-up time interval (Unit:minute)
)

// Validation Validation
type Validation struct {
	nodeManager *node.Manager
	ctx         context.Context
	lock        sync.Mutex
	seed        int64
	curRoundID  string
	crontab     *cron.Cron // timer
}

// New return new validator instance
func New(manager *node.Manager) *Validation {
	e := &Validation{
		ctx:         context.Background(),
		nodeManager: manager,
		crontab:     cron.New(),
	}

	e.initValidateTask()

	return e
}

// validation task scheduled initialization
func (v *Validation) initValidateTask() {
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

func (v *Validation) startValidate() error {
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

func (v *Validation) execute() error {
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

func (v *Validation) sendValidateInfoToValidator(validatorID string, reqList []api.ReqValidate) {
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

func (v *Validation) getValidateList(validatorMap map[string]float64) []*validateNodeInfo {
	validateList := make([]*validateNodeInfo, 0)
	v.nodeManager.EdgeNodes.Range(func(key, value interface{}) bool {
		edgeNode := value.(*node.Edge)
		info := &validateNodeInfo{}
		info.nodeType = types.NodeEdge
		info.nodeID = edgeNode.NodeID
		info.addr = edgeNode.BaseInfo.RPCURL()
		info.bandwidth = edgeNode.BandwidthUp
		validateList = append(validateList, info)
		return true
	})
	v.nodeManager.CandidateNodes.Range(func(key, value interface{}) bool {
		candidateNode := value.(*node.Candidate)
		if _, exist := validatorMap[candidateNode.NodeID]; exist {
			return true
		}
		info := &validateNodeInfo{}
		info.nodeID = candidateNode.NodeID
		info.nodeType = types.NodeCandidate
		info.addr = candidateNode.BaseInfo.RPCURL()
		info.bandwidth = candidateNode.BandwidthUp
		validateList = append(validateList, info)
		return true
	})

	return validateList
}

func (v *Validation) assignValidator(validatorList []string) map[string][]api.ReqValidate {
	validateReqs := make(map[string][]api.ReqValidate)

	validatorMap := make(map[string]float64)
	for _, id := range validatorList {
		value, exist := v.nodeManager.CandidateNodes.Load(id)
		if exist {
			candidateNode := value.(*node.Candidate)
			validatorMap[id] = candidateNode.BandwidthDown
		}
	}

	validateList := v.getValidateList(validatorMap)
	if len(validateList) <= 0 {
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

	infos := make([]*types.ValidatedResultInfo, 0)
	vs := make([]string, 0)

	for i, vInfo := range validateList {
		reqValidate, err := v.getNodeReqValidate(vInfo)
		if err != nil {
			// log.Errorf("node:%s , getNodeReqValidate err:%s", validated.nodeID, err.Error())
			continue
		}

		vs = append(vs, vInfo.nodeID)

		validatorID := findValidator(i, vInfo.bandwidth)
		list, exist := validateReqs[validatorID]
		if !exist {
			list = make([]api.ReqValidate, 0)
		}
		list = append(list, reqValidate)

		validateReqs[validatorID] = list

		info := &types.ValidatedResultInfo{
			RoundID:     v.curRoundID,
			NodeID:      vInfo.nodeID,
			ValidatorID: validatorID,
			StartTime:   time.Now(),
			Status:      types.ValidateStatusCreate,
		}
		infos = append(infos, info)
	}

	err := v.nodeManager.NodeMgrDB.InitValidatedResultInfos(infos)
	if err != nil {
		log.Errorf("AddValidateResultInfos err:%s", err.Error())
		return nil
	}

	if len(vs) > 0 {
		err = v.nodeManager.NodeMgrDB.SetNodesToVerifyingList(vs, v.nodeManager.ServerID)
		if err != nil {
			log.Errorf("SetNodesToVerifyingList err:%s", err.Error())
		}
	}

	return validateReqs
}

func (v *Validation) getNodeReqValidate(validated *validateNodeInfo) (api.ReqValidate, error) {
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
		log.Warnf("HashString2CidString %s err: %s ", hash, err.Error())
		return req, err
	}
	req.CarfileCID = cid

	return req, nil
}

type validateNodeInfo struct {
	nodeID    string
	nodeType  types.NodeType
	addr      string
	bandwidth float64
}

func (v *Validation) generatorForRandomNumber(start, end int) int {
	max := end - start
	if max <= 0 {
		return start
	}
	rand.Seed(time.Now().UnixNano())
	y := rand.Intn(max)
	return start + y
}

func (v *Validation) getRandNum(max int, r *rand.Rand) int {
	if max > 0 {
		return r.Intn(max)
	}

	return max
}

// updateFailValidatedResult update validator result info
func (v *Validation) updateFailValidatedResult(nodeID string, status types.ValidateStatus) error {
	resultInfo := &types.ValidatedResultInfo{RoundID: v.curRoundID, NodeID: nodeID, Status: status}
	return v.nodeManager.NodeMgrDB.UpdateValidatedResultInfo(resultInfo)
}

// updateSuccessValidatedResult update validator result info
func (v *Validation) updateSuccessValidatedResult(validateResult *api.ValidatedResult) error {
	resultInfo := &types.ValidatedResultInfo{
		RoundID:     validateResult.RoundID,
		NodeID:      validateResult.NodeID,
		BlockNumber: int64(len(validateResult.Cids)),
		Status:      types.ValidateStatusSuccess,
		Bandwidth:   validateResult.Bandwidth,
		Duration:    validateResult.CostTime,
	}
	return v.nodeManager.NodeMgrDB.UpdateValidatedResultInfo(resultInfo)
}

// Result node validator result
func (v *Validation) Result(validatedResult *api.ValidatedResult) error {
	if validatedResult.RoundID != v.curRoundID {
		return xerrors.Errorf("round id does not match")
	}

	// log.Debugf("validator result : %+v", *validateResult)

	var status types.ValidateStatus

	defer func() {
		var err error
		if status == types.ValidateStatusSuccess {
			err = v.updateSuccessValidatedResult(validatedResult)
		} else {
			err = v.updateFailValidatedResult(validatedResult.NodeID, status)
		}
		if err != nil {
			log.Errorf("updateSuccessValidatedResult [%s] fail : %s", validatedResult.NodeID, err.Error())
		}

		err = v.nodeManager.NodeMgrDB.RemoveValidatedWithList(validatedResult.NodeID, v.nodeManager.ServerID)
		if err != nil {
			log.Errorf("RemoveValidatedWithList [%s] fail : %s", validatedResult.NodeID, err.Error())
			return
		}
	}()

	if validatedResult.IsCancel {
		status = types.ValidateStatusCancel
		return nil
	}

	if validatedResult.IsTimeout {
		status = types.ValidateStatusTimeOut
		return nil
	}

	hash, err := cidutil.CIDString2HashString(validatedResult.CarfileCID)
	if err != nil {
		status = types.ValidateStatusOther
		log.Errorf("CIDString2HashString %s, err:%s", validatedResult.CarfileCID, err.Error())
		return nil
	}

	candidates, err := v.nodeManager.CarfileDB.CandidatesWithHash(hash)
	if err != nil {
		status = types.ValidateStatusOther
		log.Errorf("Get candidates %s , err:%s", validatedResult.CarfileCID, err.Error())
		return nil
	}

	max := len(validatedResult.Cids)
	var cCidMap map[int]string
	for _, nodeID := range candidates {
		node := v.nodeManager.GetCandidateNode(nodeID)
		if node == nil {
			continue
		}

		cCidMap, err = node.API().GetBlocksOfCarfile(context.Background(), validatedResult.CarfileCID, v.seed, max)
		if err != nil {
			log.Errorf("candidate %s GetBlocksOfCarfile err:%s", nodeID, err.Error())
			continue
		}

		break
	}

	if len(cCidMap) <= 0 {
		status = types.ValidateStatusOther
		log.Errorf("handleValidateResult candidate map is nil , %s", validatedResult.CarfileCID)
		return nil
	}

	carfileRecord, err := v.nodeManager.CarfileDB.LoadCarfileInfo(hash)
	if err != nil {
		status = types.ValidateStatusOther
		log.Errorf("handleValidateResult GetCarfileInfo %s , err:%s", validatedResult.CarfileCID, err.Error())
		return nil
	}

	r := rand.New(rand.NewSource(v.seed))
	// do validator
	for i := 0; i < max; i++ {
		resultCid := validatedResult.Cids[i]
		randNum := v.getRandNum(carfileRecord.TotalBlocks, r)
		vCid := cCidMap[randNum]

		// TODO Penalize the candidate if vCid error

		if !v.compareCid(resultCid, vCid) {
			status = types.ValidateStatusBlockFail
			log.Errorf("round [%d] and nodeID [%s], validator fail resultCid:%s, vCid:%s,randNum:%d,index:%d", validatedResult.RoundID, validatedResult.NodeID, resultCid, vCid, randNum, i)
			return nil
		}
	}

	status = types.ValidateStatusSuccess
	return nil
}

func (v *Validation) compareCid(cidStr1, cidStr2 string) bool {
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
func (v *Validation) StartValidateOnceTask() error {
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
