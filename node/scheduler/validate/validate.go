package validate

import (
	"context"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/types"
	"github.com/linguohua/titan/node/cidutil"
	"golang.org/x/xerrors"
)

const (
	duration         = 10              // Validation duration per node (Unit:Second)
	validateInterval = 5 * time.Minute // validate start-up time interval (Unit:minute)
)

// starts the validation process.
func (m *Manager) startValidation(ctx context.Context) {
	ticker := time.NewTicker(validateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if enable := m.isEnabled(); !enable {
				continue
			}

			if err := m.startNewRound(); err != nil {
				log.Errorf("start new round: %v", err)
			}
		case <-m.close:
			return
		}
	}
}

func (m *Manager) stopValidate(ctx context.Context) error {
	close(m.close)
	return nil
}

// returns whether or not validation is currently enabled.
func (m *Manager) isEnabled() bool {
	cfg, err := m.config()
	if err != nil {
		log.Errorf("enable err:%s", err.Error())
		return false
	}

	return cfg.EnableValidate
}

// startNewRound is a method of the Manager that starts a new validation round.
func (m *Manager) startNewRound() error {
	if m.curRoundID != "" {
		// Set the timeout status of the previous verification
		err := m.nodeMgr.SetValidateResultsTimeout(m.curRoundID)
		if err != nil {
			log.Errorf("round:%s ValidatedTimeout err:%s", m.curRoundID, err.Error())
		}
	}

	roundID := uuid.NewString()
	m.curRoundID = roundID
	m.seed = time.Now().UnixNano() // TODO from filecoin

	vrs := m.PairValidatorsAndBeValidates()

	validateReqs, dbInfos := m.getValidateDetails(vrs)
	if validateReqs == nil {
		return xerrors.New("assignValidator map is null")
	}

	err := m.nodeMgr.SetValidateResultInfos(dbInfos)
	if err != nil {
		log.Errorf("SetValidateResultInfos err:%s", err.Error())
		return nil
	}

	for nodeID, reqs := range validateReqs {
		go m.sendValidateReqToNodes(nodeID, reqs)
	}

	return nil
}

// sends a validation request to a node.
func (m *Manager) sendValidateReqToNodes(nID string, req *api.BeValidateReq) {
	cNode := m.nodeMgr.GetNode(nID)
	if cNode != nil {
		err := cNode.BeValidate(context.Background(), req)
		if err != nil {
			log.Errorf("%s BeValidate err:%s", nID, err.Error())
		}
		return
	}

	log.Errorf("%s BeValidate Node not found", nID)
}

// fetches validation details.
func (m *Manager) getValidateDetails(vrs []*VWindow) (map[string]*api.BeValidateReq, []*types.ValidateResultInfo) {
	bReqs := make(map[string]*api.BeValidateReq)
	vrInfos := make([]*types.ValidateResultInfo, 0)

	for _, vr := range vrs {
		vID := vr.NodeID
		vNode := m.nodeMgr.GetCandidateNode(vID)
		if vNode == nil {
			log.Errorf("%s validator not exist", vNode)
			continue
		}

		for nodeID := range vr.BeValidates {
			cid, err := m.getNodeValidationCID(nodeID)
			if err != nil {
				log.Errorf("%s getNodeValidateCID err:%s", nodeID, err.Error())
				continue
			}

			dbInfo := &types.ValidateResultInfo{
				RoundID:     m.curRoundID,
				NodeID:      nodeID,
				ValidatorID: vID,
				Status:      types.ValidateStatusCreate,
				Cid:         cid,
			}
			vrInfos = append(vrInfos, dbInfo)

			req := &api.BeValidateReq{
				CID:        cid,
				RandomSeed: m.seed,
				Duration:   duration,
				TCPSrvAddr: vNode.TCPAddr(),
			}

			bReqs[nodeID] = req
		}
	}

	return bReqs, vrInfos
}

// retrieves a random validation CID from the node with the given ID.
func (m *Manager) getNodeValidationCID(nodeID string) (string, error) {
	count, err := m.nodeMgr.FetchNodeReplicaCount(nodeID)
	if err != nil {
		return "", err
	}

	if count < 1 {
		return "", xerrors.New("Node has no replica")
	}

	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
	// rand count
	offset := rand.Intn(count)

	cids, err := m.nodeMgr.FetchAssetCIDsByNodeID(nodeID, 1, offset)
	if err != nil {
		return "", err
	}

	if len(cids) < 1 {
		return "", xerrors.New("Node has no replica")
	}

	return cids[0], nil
}

// generates a random number up to a given maximum value.
func (m *Manager) getRandNum(max int, r *rand.Rand) int {
	if max > 0 {
		return r.Intn(max)
	}

	return max
}

// updates the validation result information for a given node.
func (m *Manager) updateResultInfo(status types.ValidateStatus, validateResult *api.ValidateResult) error {
	resultInfo := &types.ValidateResultInfo{
		RoundID:     validateResult.RoundID,
		NodeID:      validateResult.NodeID,
		Status:      status,
		BlockNumber: int64(len(validateResult.Cids)),
		Bandwidth:   validateResult.Bandwidth,
		Duration:    validateResult.CostTime,
	}

	return m.nodeMgr.UpdateValidateResultInfo(resultInfo)
}

// Result handles the validation result for a given node.
func (m *Manager) Result(validatedResult *api.ValidateResult) error {
	if validatedResult.RoundID != m.curRoundID {
		return xerrors.Errorf("round id does not match %s:%s", m.curRoundID, validatedResult.RoundID)
	}

	var status types.ValidateStatus
	nodeID := validatedResult.NodeID

	defer func() {
		err := m.updateResultInfo(status, validatedResult)
		if err != nil {
			log.Errorf("updateSuccessValidatedResult [%s] fail : %s", nodeID, err.Error())
		}
	}()

	if validatedResult.IsCancel {
		status = types.ValidateStatusCancel
		return nil
	}

	if validatedResult.IsTimeout {
		status = types.ValidateStatusNodeTimeOut
		return nil
	}

	cid, err := m.nodeMgr.FetchNodeValidateCID(validatedResult.RoundID, nodeID)
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

	rows, err := m.nodeMgr.FetchReplicasByHash(hash, []types.ReplicaStatus{types.ReplicaStatusSucceeded})
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

		node := m.nodeMgr.GetCandidateNode(cNodeID)
		if node == nil {
			continue
		}

		cCidMap, err = node.GetBlocksOfCarfile(context.Background(), cid, m.seed, max)
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

	record, err := m.nodeMgr.FetchAssetRecord(hash)
	if err != nil {
		status = types.ValidateStatusOther
		log.Errorf("handleValidateResult asset record %s , err:%s", validatedResult.CID, err.Error())
		return nil
	}

	r := rand.New(rand.NewSource(m.seed))
	// do validator
	for i := 0; i < max; i++ {
		resultCid := validatedResult.Cids[i]
		randNum := m.getRandNum(int(record.TotalBlocks), r)
		vCid := cCidMap[randNum]

		// TODO Penalize the candidate if vCid error

		if !m.compareCid(resultCid, vCid) {
			status = types.ValidateStatusBlockFail
			log.Errorf("round [%d] and nodeID [%s], validator fail resultCid:%s, vCid:%s,randNum:%d,index:%d", validatedResult.RoundID, nodeID, resultCid, vCid, randNum, i)
			return nil
		}
	}

	status = types.ValidateStatusSuccess
	return nil
}

// compares two CID strings and returns true if they are equal, false otherwise
func (m *Manager) compareCid(cidStr1, cidStr2 string) bool {
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
