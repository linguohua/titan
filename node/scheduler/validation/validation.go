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

var log = logging.Logger("validation")

const (
	duration         = 10              // Validation duration per node (Unit:Second)
	validateInterval = 5 * time.Minute // validate start-up time interval (Unit:minute)

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
			if enable := v.enable(); !enable {
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

func (v *Validation) enable() bool {
	cfg, err := v.config()
	if err != nil {
		log.Errorf("enable err:%s", err.Error())
		return false
	}

	return cfg.EnableValidate
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

	vrs := v.nodeMgr.Pairing()

	validateReqs, dbInfos := v.getValidateDetails(vrs)
	if validateReqs == nil {
		return xerrors.New("assignValidator map is null")
	}

	err := v.nodeMgr.SetValidateResultInfos(dbInfos)
	if err != nil {
		log.Errorf("SetValidateResultInfos err:%s", err.Error())
		return nil
	}

	for nodeID, reqs := range validateReqs {
		go v.sendValidateReqToNodes(nodeID, reqs)
	}

	return nil
}

func (v *Validation) sendValidateReqToNodes(nID string, req *api.BeValidateReq) {
	cNode := v.nodeMgr.GetNode(nID)
	if cNode != nil {
		err := cNode.BeValidate(context.Background(), req)
		if err != nil {
			log.Errorf("%s BeValidate err:%s", nID, err.Error())
		}
		return
	}

	log.Errorf("%s BeValidate Node not found", nID)
}

func (v *Validation) getValidateDetails(vrs []*node.ValidatorBwDnUnit) (map[string]*api.BeValidateReq, []*types.ValidateResultInfo) {
	bReqs := make(map[string]*api.BeValidateReq)
	vrInfos := make([]*types.ValidateResultInfo, 0)

	for _, vr := range vrs {
		vID := vr.NodeID
		vNode := v.nodeMgr.GetCandidateNode(vID)
		if vNode == nil {
			log.Errorf("%s validator not exist", vNode)
			continue
		}

		for nodeID := range vr.BeValidates {
			cid, err := v.getNodeValidateCID(nodeID)
			if err != nil {
				log.Errorf("%s getNodeValidateCID err:%s", nodeID, err.Error())
				continue
			}

			dbInfo := &types.ValidateResultInfo{
				RoundID:     v.curRoundID,
				NodeID:      nodeID,
				ValidatorID: vID,
				Status:      types.ValidateStatusCreate,
				Cid:         cid,
			}
			vrInfos = append(vrInfos, dbInfo)

			req := &api.BeValidateReq{
				CID:        cid,
				RandomSeed: v.seed,
				Duration:   duration,
				TCPSrvAddr: vNode.TCPAddr(),
			}

			bReqs[nodeID] = req
		}
	}

	return bReqs, vrInfos
}

func (v *Validation) getNodeValidateCID(nodeID string) (string, error) {
	count, err := v.nodeMgr.LoadReplicaCountOfNode(nodeID)
	if err != nil {
		return "", err
	}

	if count < 1 {
		return "", xerrors.New("Node has no replica")
	}

	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
	// rand count
	offset := rand.Intn(count)

	cids, err := v.nodeMgr.LoadAssetCidsOfNode(nodeID, 1, offset)
	if err != nil {
		return "", err
	}

	if len(cids) < 1 {
		return "", xerrors.New("Node has no replica")
	}

	return cids[0], nil
}

func (v *Validation) getRandNum(max int, r *rand.Rand) int {
	if max > 0 {
		return r.Intn(max)
	}

	return max
}

func (v *Validation) updateResultInfo(status types.ValidateStatus, validateResult *api.ValidateResult) error {
	resultInfo := &types.ValidateResultInfo{
		RoundID:     validateResult.RoundID,
		NodeID:      validateResult.NodeID,
		Status:      status,
		BlockNumber: int64(len(validateResult.Cids)),
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

	var status types.ValidateStatus
	nodeID := validatedResult.NodeID

	defer func() {
		err := v.updateResultInfo(status, validatedResult)
		if err != nil {
			log.Errorf("updateSuccessValidatedResult [%s] fail : %s", nodeID, err.Error())
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

	cid, err := v.nodeMgr.LoadNodeValidateCID(validatedResult.RoundID, nodeID)
	if err != nil {
		status = types.ValidateStatusOther
		log.Errorf("LoadNodeValidateCID %s , %s, err:%s", validatedResult.RoundID, nodeID, err.Error())
		return nil
	}

	hash, err := cidutil.CIDString2HashString(cid)
	if err != nil {
		status = types.ValidateStatusOther
		log.Errorf("CIDString2HashString %s, err:%s", cid, err.Error())
		return nil
	}

	rows, err := v.nodeMgr.LoadReplicasOfHash(hash, []types.ReplicaStatus{types.ReplicaStatusSucceeded})
	if err != nil {
		status = types.ValidateStatusOther
		log.Errorf("Get candidates %s , err:%s", hash, err.Error())
		return nil
	}
	defer rows.Close()

	max := len(validatedResult.Cids)
	var cCidMap map[int]string

	for rows.Next() {
		rInfo := &types.ReplicaInfo{}
		err = rows.StructScan(rInfo)
		if err != nil {
			log.Errorf("replica StructScan err: %s", err.Error())
			continue
		}

		cNodeID := rInfo.NodeID
		if cNodeID == nodeID {
			continue
		}

		node := v.nodeMgr.GetCandidateNode(cNodeID)
		if node == nil {
			continue
		}

		cCidMap, err = node.GetBlocksOfCarfile(context.Background(), cid, v.seed, max)
		if err != nil {
			log.Errorf("candidate %s GetBlocksOfCarfile err:%s", cNodeID, err.Error())
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
			log.Errorf("round [%d] and nodeID [%s], validator fail resultCid:%s, vCid:%s,randNum:%d,index:%d", validatedResult.RoundID, nodeID, resultCid, vCid, randNum, i)
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
