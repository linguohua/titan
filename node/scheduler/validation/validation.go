package validation

import (
	"context"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/linguohua/titan/node/modules/dtypes"

	"golang.org/x/xerrors"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api/types"
	"github.com/linguohua/titan/node/cidutil"
	"github.com/linguohua/titan/node/scheduler/node"

	"github.com/linguohua/titan/api"
)

var log = logging.Logger("scheduler/validation")

const (
	duration         = 10              // Validation duration per node (Unit:Second)
	validateInterval = 5 * time.Minute // validate start-up time interval (Unit:minute)

	bandwidthRatio = 0.7 // The ratio of the total upstream bandwidth on edge nodes to the downstream bandwidth on validation nodes.
	validatorNum   = 0   // Percentage of nodes participating in validation (0 means that all validators participate)
)

// Validation Validation
type Validation struct {
	nodeMgr    *node.Manager
	seed       int64
	curRoundID string
	close      chan struct{}
	config     dtypes.GetSchedulerConfigFunc
}

// New return new validator instance
func New(manager *node.Manager, configFunc dtypes.GetSchedulerConfigFunc) *Validation {
	e := &Validation{
		nodeMgr: manager,
		config:  configFunc,
		close:   make(chan struct{}),
	}

	return e
}

// Start start validation task scheduled
func (v *Validation) Start(ctx context.Context) {
	ticker := time.NewTicker(validateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if enable, _ := v.enable(); !enable {
				continue
			}

			if err := v.start(); err != nil {
				log.Errorf("start new round: %v", err)
			}
		case <-v.close:
			return
		}
	}
}

func (v *Validation) Stop(ctx context.Context) error {
	close(v.close)
	return nil
}

func (v *Validation) enable() (bool, error) {
	cfg, err := v.config()
	if err != nil {
		return false, err
	}

	return cfg.EnableValidate, nil
}

func (v *Validation) start() error {
	if v.curRoundID != "" {
		// Set the timeout status of the previous verification
		err := v.nodeMgr.SetValidateResultsTimeout(v.curRoundID)
		if err != nil {
			log.Errorf("round:%s ValidatedTimeout err:%s", v.curRoundID, err.Error())
		}
	}

	roundID := uuid.NewString()
	v.curRoundID = roundID
	v.seed = time.Now().UnixNano() // TODO from filecoin

	validators, err := v.getEffectiveValidators()
	if err != nil {
		return err
	}

	log.Debug("validator list: ", validators)

	beValidateNodes := v.getBeValidateNodes()

	validatorMap := v.assignValidator(validators, beValidateNodes)
	if validatorMap == nil {
		return xerrors.New("assignValidator map is null")
	}

	for validatorID, reqList := range validatorMap {
		go v.sendValidateInfoToValidator(validatorID, reqList)
	}

	return nil
}

func (v *Validation) getBeValidateNodes() []*api.ValidateReq {
	var out []*api.ValidateReq
	// edge nodes
	v.nodeMgr.EdgeNodes.Range(func(key, value interface{}) bool {
		node := value.(*node.Edge)
		info := &validateNodeInfo{}
		info.nodeType = types.NodeEdge
		info.nodeID = node.NodeID
		info.addr = node.BaseInfo.RPCURL()
		info.bandwidth = node.BandwidthUp

		out = append(out, &api.ValidateReq{})
		return true
	})

	// candidate nodes
	v.nodeMgr.CandidateNodes.Range(func(key, value interface{}) bool {
		node := value.(*node.Candidate)
		info := &validateNodeInfo{}
		info.nodeType = types.NodeEdge
		info.nodeID = node.NodeID
		info.addr = node.BaseInfo.RPCURL()
		info.bandwidth = node.BandwidthUp

		out = append(out, &api.ValidateReq{})
		return true
	})

	return out
}

func (v *Validation) getEffectiveValidators() (map[string]*node.Candidate, error) {
	validatorList, err := v.nodeMgr.LoadValidators(v.nodeMgr.ServerID)
	if err != nil {
		return nil, err
	}

	effectiveValidators := make(map[string]*node.Candidate, 0)

	for _, nodeID := range validatorList {
		c := v.nodeMgr.GetCandidateNode(nodeID)
		if c == nil {
			continue
		}
		effectiveValidators[c.NodeID] = c

		if validatorNum > 0 && len(effectiveValidators) >= validatorNum {
			break
		}
	}

	if len(effectiveValidators) == 0 {
		return nil, xerrors.New("not found validator")
	}

	return effectiveValidators, nil
}

func (v *Validation) sendValidateInfoToValidator(validatorID string, reqList []api.ValidateReq) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	validator := v.nodeMgr.GetCandidateNode(validatorID)
	if validator == nil {
		log.Errorf("validator [%s] is null", validatorID)
		return
	}

	_, err := validator.API().ValidateNodes(ctx, reqList)
	if err != nil {
		log.Errorf("ValidateNodes [%s] err:%s", validatorID, err.Error())
		return
	}
}

func (v *Validation) assignValidator(validators map[string]*node.Candidate, beValidateNodes []*api.ValidateReq) map[string][]api.ValidateReq {
	validateReqs := make(map[string][]api.ValidateReq)

	infos := make([]*types.ValidateResultInfo, 0)

	// for i, vInfo := range edges {
	// 	reqValidate, err := v.getNodeReqValidate(vInfo)
	// 	if err != nil {
	// 		// log.Errorf("node:%s , getNodeReqValidate err:%s", validated.nodeID, err.Error())
	// 		continue
	// 	}

	// 	validatorID := validators[i%len(validators)]
	// 	list, exist := validateReqs[validatorID]
	// 	if !exist {
	// 		list = make([]api.ReqValidate, 0)
	// 	}
	// 	list = append(list, reqValidate)

	// 	validateReqs[validatorID] = list

	// 	info := &types.ValidateResultInfo{
	// 		RoundID:     v.curRoundID,
	// 		NodeID:      vInfo.nodeID,
	// 		ValidatorID: validatorID,
	// 		StartTime:   time.Now(),
	// 		Status:      types.ValidateStatusCreate,
	// 	}
	// 	infos = append(infos, info)
	// }

	err := v.nodeMgr.SetValidateResultInfos(infos)
	if err != nil {
		log.Errorf("AddValidateResultInfos err:%s", err.Error())
		return nil
	}

	return validateReqs
}

func (v *Validation) nodeReqValidate(nodeID string) (api.ValidateReq, error) {
	req := api.ValidateReq{
		NodeID:   nodeID,
		Duration: duration,
		RoundID:  v.curRoundID,
	}

	count, err := v.nodeMgr.LoadReplicaCountOfNode(nodeID)
	if err != nil {
		return req, err
	}

	if count < 1 {
		return req, xerrors.New("Node has no replica")
	}

	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
	// rand count
	offset := rand.Intn(count)

	cids, err := v.nodeMgr.LoadAssetCidsOfNode(nodeID, 1, offset)
	if err != nil {
		return req, err
	}

	if len(cids) < 1 {
		return req, xerrors.New("Node has no replica")
	}

	// req.CID = cids[0]

	return req, nil
}

type validateNodeInfo struct {
	nodeID    string
	nodeType  types.NodeType
	addr      string
	bandwidth float64
}

func (v *Validation) getRandNum(max int, r *rand.Rand) int {
	if max > 0 {
		return r.Intn(max)
	}

	return max
}

// updateFailValidatedResult update validator result info
func (v *Validation) updateFailValidatedResult(nodeID string, status types.ValidateStatus) error {
	resultInfo := &types.ValidateResultInfo{RoundID: v.curRoundID, NodeID: nodeID, Status: status}
	return v.nodeMgr.UpdateValidateResultInfo(resultInfo)
}

// updateSuccessValidatedResult update validator result info
func (v *Validation) updateSuccessValidatedResult(validateResult *api.ValidateResult) error {
	resultInfo := &types.ValidateResultInfo{
		RoundID:     validateResult.RoundID,
		NodeID:      validateResult.NodeID,
		BlockNumber: int64(len(validateResult.Cids)),
		Status:      types.ValidateStatusSuccess,
		Bandwidth:   validateResult.Bandwidth,
		Duration:    validateResult.CostTime,
	}

	return v.nodeMgr.UpdateValidateResultInfo(resultInfo)
}

// Result node validator result
func (v *Validation) Result(validatedResult *api.ValidateResult) error {
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
	}()

	if validatedResult.IsCancel {
		status = types.ValidateStatusCancel
		return nil
	}

	if validatedResult.IsTimeout {
		status = types.ValidateStatusTimeOut
		return nil
	}

	hash, err := cidutil.CIDString2HashString(validatedResult.CID)
	if err != nil {
		status = types.ValidateStatusOther
		log.Errorf("CIDString2HashString %s, err:%s", validatedResult.CID, err.Error())
		return nil
	}

	rows, err := v.nodeMgr.LoadReplicasOfHash(hash, []types.ReplicaStatus{types.ReplicaStatusSucceeded})
	if err != nil {
		status = types.ValidateStatusOther
		log.Errorf("Get candidates %s , err:%s", validatedResult.CID, err.Error())
		return nil
	}

	max := len(validatedResult.Cids)
	var cCidMap map[int]string

	for rows.Next() {
		rInfo := &types.ReplicaInfo{}
		err = rows.StructScan(rInfo)
		if err != nil {
			log.Errorf("replica StructScan err: %s", err.Error())
			continue
		}

		if !rInfo.IsCandidate {
			continue
		}

		if rInfo.Status != types.ReplicaStatusSucceeded {
			continue
		}

		nodeID := rInfo.NodeID
		node := v.nodeMgr.GetCandidateNode(nodeID)
		if node == nil {
			continue
		}

		cCidMap, err = node.API().GetBlocksOfCarfile(context.Background(), validatedResult.CID, v.seed, max)
		if err != nil {
			log.Errorf("candidate %s GetBlocksOfCarfile err:%s", nodeID, err.Error())
			continue
		}

		break
	}

	if len(cCidMap) <= 0 {
		status = types.ValidateStatusOther
		log.Errorf("handleValidateResult candidate map is nil , %s", validatedResult.CID)
		return nil
	}

	record, err := v.nodeMgr.LoadAssetRecord(hash)
	if err != nil {
		status = types.ValidateStatusOther
		log.Errorf("handleValidateResult asset record %s , err:%s", validatedResult.CID, err.Error())
		return nil
	}

	r := rand.New(rand.NewSource(v.seed))
	// do validator
	for i := 0; i < max; i++ {
		resultCid := validatedResult.Cids[i]
		randNum := v.getRandNum(int(record.TotalBlocks), r)
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
