package scheduler

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/scheduler/db"
	"github.com/ouqiang/timewheel"
	"golang.org/x/xerrors"
)

// ElectionValidate ElectionValidate
type ElectionValidate struct {
	seed    int64
	fidsMap map[string][]string
	roundID string

	duration          int
	validateBlockMax  int // 每次抽查block个数上限
	verifiedNodeMax   int // 每个验证节点一次验证的被验证节点数
	timewheelElection *timewheel.TimeWheel
	electionTime      int // 选举时间间隔 (分钟)
	timewheelValidate *timewheel.TimeWheel
	validateTime      int // 抽查时间间隔 (分钟)
	// unassignedEdgeMap map[string]int // 未被分配到的边缘节点组
}

// InitValidateTimewheel init timer
func (e *ElectionValidate) initValidateTimewheel(scheduler *Scheduler) {
	// 选举定时器
	e.timewheelElection = timewheel.New(1*time.Second, 3600, func(_ interface{}) {
		err := e.electionValidators(scheduler)
		if err != nil {
			log.Panicf("electionValidators err:%v", err.Error())
		}
		// 继续添加定时器
		e.timewheelElection.AddTimer(time.Duration(e.electionTime)*60*time.Second, "election", nil)
	})
	e.timewheelElection.Start()
	// 开始一个事件处理
	e.timewheelElection.AddTimer(time.Duration(1)*60*time.Second, "election", nil)

	// 抽查定时器
	e.timewheelValidate = timewheel.New(1*time.Second, 3600, func(_ interface{}) {
		err := e.startValidate(scheduler)
		if err != nil {
			log.Panicf("startValidate err:%v", err.Error())
		}
		// 继续添加定时器
		e.timewheelValidate.AddTimer(time.Duration(e.validateTime)*60*time.Second, "validate", nil)
	})
	e.timewheelValidate.Start()
	// 开始一个事件处理
	e.timewheelValidate.AddTimer(time.Duration(2)*60*time.Second, "validate", nil)
}

func newElectionValidate() *ElectionValidate {
	e := &ElectionValidate{
		seed:     int64(1),
		duration: 10,

		validateBlockMax: 100, // 每次抽查block个数上限

		verifiedNodeMax: 10, // 每个验证节点一次验证的被验证节点数

		// unassignedEdgeMap: make(map[string]int), // 未被分配到的边缘节点组

		electionTime: 60,
		validateTime: 10,
	}

	return e
}

func (e *ElectionValidate) getReqValidate(scheduler *Scheduler, validatorID string, list []string) ([]api.ReqValidate, []string) {
	// validatorID := candidate.deviceInfo.DeviceId
	req := make([]api.ReqValidate, 0)

	errList := make([]string, 0)

	for _, deviceID := range list {
		addr := ""
		edgeNode := scheduler.nodeManager.getEdgeNode(deviceID)
		if edgeNode == nil {
			candidateNode := scheduler.nodeManager.getCandidateNode(deviceID)
			if candidateNode == nil {
				errList = append(errList, deviceID)
				continue
			}
			addr = candidateNode.Node.addr
		} else {
			addr = edgeNode.Node.addr
		}

		// 查看节点缓存了哪些数据
		datas, err := db.GetCacheDB().GetCacheDataInfos(deviceID)
		if err != nil {
			log.Warnf("validate GetCacheDataInfos err:%v,DeviceId:%v", err.Error(), deviceID)
			continue
		}

		if len(datas) <= 0 {
			continue
		}

		// max, err := db.GetCacheDB().GetNodeCacheTag(edgeID)
		// if err != nil {
		// 	log.Warnf("validate GetNodeCacheTag err:%v,DeviceId:%v", err.Error(), edgeID)
		// 	continue
		// }

		fids := make([]string, 0)
		for _, tag := range datas {
			fids = append(fids, tag)

			if len(fids) >= e.validateBlockMax {
				break
			}
		}

		req = append(req, api.ReqValidate{Seed: e.seed, EdgeURL: addr, Duration: e.duration, FIDs: fids, RoundID: e.roundID})

		e.fidsMap[deviceID] = fids
		//
		err = db.GetCacheDB().SetValidateResultInfo(e.roundID, deviceID, validatorID, "", db.ValidateStatusCreate)
		if err != nil {
			log.Warnf("validate SetValidateResultInfo err:%v,DeviceId:%v", err.Error(), deviceID)
			continue
		}

		err = db.GetCacheDB().SetNodeToValidateList(deviceID)
		if err != nil {
			log.Warnf("validate SetNodeToValidateList err:%v,DeviceId:%v", err.Error(), deviceID)
			continue
		}
	}

	return req, errList
}

func (e *ElectionValidate) getRandFid(max int, r *rand.Rand) int {
	if max > 0 {
		return r.Intn(max) // 过滤0
	}

	return max
}

// func toCidV0(c cid.Cid) (cid.Cid, error) {
// 	if c.Type() != cid.DagProtobuf {
// 		return cid.Cid{}, fmt.Errorf("can't convert non-dag-pb nodes to cidv0")
// 	}
// 	return cid.NewCidV0(c.Hash()), nil
// }

// func toCidV1(c cid.Cid) (cid.Cid, error) {
// 	return cid.NewCidV1(c.Type(), c.Hash()), nil
// }

func (e *ElectionValidate) saveValidateResult(sID string, deviceID string, validatorID string, msg string, status db.ValidateStatus) error {
	err := db.GetCacheDB().SetValidateResultInfo(sID, deviceID, "", msg, status)
	if err != nil {
		// log.Errorf("SetValidateResultInfo err:%v", err.Error())
		return err
	}

	err = db.GetCacheDB().DelNodeWithValidateList(deviceID)
	if err != nil {
		// log.Errorf("DelNodeWithValidateList err:%v", err.Error())
		return err
	}

	if msg != "" {
		// 扣罚
		err = db.GetCacheDB().SetNodeToValidateErrorList(sID, deviceID)
		if err != nil {
			// log.Errorf("SetNodeToValidateErrorList ,err:%v,deviceID:%v", err.Error(), deviceID)
			return err
		}
	}

	return nil
}

func (e *ElectionValidate) validateResult(validateResults *api.ValidateResults) error {
	if validateResults.RoundID != e.roundID {
		return xerrors.Errorf("roundID err")
	}

	deviceID := validateResults.DeviceID
	// varify Result

	status := db.ValidateStatusSuccess
	msg := ""

	// TODO 判断带宽 超时时间等等
	if validateResults.IsTimeout {
		status = db.ValidateStatusTimeOut
		msg = fmt.Sprint("Time out")
		return e.saveValidateResult(e.roundID, deviceID, "", msg, status)
	}

	r := rand.New(rand.NewSource(e.seed))
	rlen := len(validateResults.Results)

	if rlen <= 0 {
		status = db.ValidateStatusFail
		msg = fmt.Sprint("Results is nil")
		return e.saveValidateResult(e.roundID, deviceID, "", msg, status)
	}

	list := e.fidsMap[deviceID]
	max := len(list)

	log.Infof("validateResult:%v", deviceID)

	for i := 0; i < rlen; i++ {
		index := e.getRandFid(max, r)
		result := validateResults.Results[i]

		fidStr := list[index]
		// log.Infof("fidStr:%v,resultFid:%v,index:%v", fidStr, result.Fid, i)
		if fidStr != result.Fid {
			status = db.ValidateStatusFail
			msg = fmt.Sprintf("fidStr:%v,resultFid:%v,index:%v", fidStr, result.Fid, i)
			break
		}

		if result.Cid == "" {
			// cid, err := db.GetCacheDB().GetCacheDataTagInfo(edgeID, fidStr)
			// if err == nil && cid != "" {
			status = db.ValidateStatusFail
			msg = fmt.Sprintf("resultCid:%v,resultFid:%v", result.Cid, result.Fid)
			break
			// }

			// 确实没有这个cid
			// continue
		}

		tag, err := db.GetCacheDB().GetCacheDataInfo(deviceID, result.Cid)
		if err != nil {
			status = db.ValidateStatusFail
			msg = fmt.Sprintf("GetCacheDataInfo err:%v,deviceID:%v,resultCid:%v,resultFid:%v", err.Error(), deviceID, result.Cid, result.Fid)
			break
		}

		if tag != fidStr {
			status = db.ValidateStatusFail
			msg = fmt.Sprintf("tag:%v,fidStr:%v,Cid:%v", tag, fidStr, result.Cid)
			break
		}
	}

	return e.saveValidateResult(e.roundID, deviceID, "", msg, status)
}

// 检查有没有超时的抽查
func (e *ElectionValidate) checkValidateTimeOut() error {
	sID, err := db.GetCacheDB().GetValidateRoundID()
	if err != nil {
		return err
	}

	edgeIDs, err := db.GetCacheDB().GetNodesWithValidateList()
	if err != nil {
		return err
	}

	if len(edgeIDs) > 0 {
		log.Infof("checkValidateTimeOut list:%v", edgeIDs)

		for _, edgeID := range edgeIDs {
			err = db.GetCacheDB().SetValidateResultInfo(sID, edgeID, "", "", db.ValidateStatusTimeOut)
			if err != nil {
				log.Warnf("checkValidateTimeOut SetValidateResultInfo err:%v,DeviceId:%v", err.Error(), edgeID)
				continue
			}

			err = db.GetCacheDB().DelNodeWithValidateList(edgeID)
			if err != nil {
				log.Warnf("checkValidateTimeOut DelNodeWithValidateList err:%v,DeviceId:%v", err.Error(), edgeID)
				continue
			}
		}
	}

	return nil
}

// 重置 验证者数据
func (e *ElectionValidate) cleanValidators(scheduler *Scheduler) error {
	validators, err := db.GetCacheDB().GetValidatorsWithList()
	if err != nil {
		return err
	}

	for _, validator := range validators {
		err = db.GetCacheDB().DelValidatorGeoList(validator)
		if err != nil {
			log.Warnf("DelValidatorGeoList err:%v, validator:%v", err.Error(), validator)
		}

		node := scheduler.nodeManager.getCandidateNode(validator)
		if node != nil {
			node.isValidator = false
		}
	}

	err = db.GetCacheDB().DelValidatorList()
	if err != nil {
		return err
	}

	return nil
}

// 选举、分配验证者负责的区域
func (e *ElectionValidate) electionValidators(scheduler *Scheduler) error {
	// 每个城市 选出X个验证者
	// 每隔Y时间 重新选举
	err := e.cleanValidators(scheduler)
	if err != nil {
		return err
	}

	// 初始化池子
	scheduler.nodePool.nodesToPool()
	scheduler.nodePool.testPrintlnPoolMap()

	alreadyAssignValidatorMap := make(map[string]string) // 已被分配的验证者
	unAssignCandidates := make([]string, 0)              // 空闲的候选者
	// 缺验证节点数量
	lackValidatorMap := make(map[string]int)

	scheduler.nodePool.poolMap.Range(func(key, value interface{}) bool {
		geo := key.(string)
		pool := value.(*pool)
		if pool == nil {
			return true
		}

		// 初始化状态
		pool.resetVeriftors()

		edgeNum := len(pool.edgeNodes)
		candidateNum := len(pool.candidateNodes)
		if edgeNum <= 0 && candidateNum <= 0 {
			return true
		}

		nodeTotalNum := edgeNum + candidateNum
		n := 0
		if nodeTotalNum%(e.verifiedNodeMax+1) > 0 {
			n = 1
		}
		needVeriftorNum := nodeTotalNum/(e.verifiedNodeMax+1) + n

		if candidateNum >= needVeriftorNum {
			// 选出验证者 把多余的候选节点放入unAssignCandidates
			vn := 0
			for deviceID := range pool.candidateNodes {
				vn++
				if vn > needVeriftorNum {
					unAssignCandidates = append(unAssignCandidates, deviceID)
				} else {
					alreadyAssignValidatorMap[deviceID] = geo
				}
			}
		} else {
			// 候选节点不足的情况下 所有候选者都是验证者
			for deviceID := range pool.candidateNodes {
				alreadyAssignValidatorMap[deviceID] = geo
			}

			lackValidatorMap[geo] = needVeriftorNum - candidateNum
		}
		// log.Infof("选举 geo:%v,candidateNum:%v,edgeNum:%v,needVeriftorNum:%v", geo, candidateNum, edgeNum, needVeriftorNum)

		return true
	})

	// log.Infof("选举遗漏 lackValidatorMap:%v", lackValidatorMap)

	candidateNotEnough := false
	// 再次选举候选节点
	if len(lackValidatorMap) > 0 {
		for geo, num := range lackValidatorMap {
			if len(unAssignCandidates) == 0 {
				candidateNotEnough = true
				break
			}

			n := num
			if len(unAssignCandidates) < num {
				n = len(unAssignCandidates)
			}

			validatorIDs := unAssignCandidates[0:n]
			unAssignCandidates = unAssignCandidates[n:]

			for _, validatorID := range validatorIDs {
				alreadyAssignValidatorMap[validatorID] = geo
			}
		}

		if candidateNotEnough {
			log.Warnf("Candidate Not Enough  assignEdge: %v", lackValidatorMap)
		}
	}
	// log.Infof("选举结果 alreadyAssignValidatorMap:%v", alreadyAssignValidatorMap)

	// 记录验证者负责的区域到redis
	for validatorID, geo := range alreadyAssignValidatorMap {
		validator := scheduler.nodeManager.getCandidateNode(validatorID)
		if validator != nil {
			validator.isValidator = true
		}

		err = db.GetCacheDB().SetValidatorToList(validatorID)
		if err != nil {
			// log.Warnf("SetValidatorToList err:%v, validatorID : %s", err.Error(), validatorID)
			return err
		}

		err = db.GetCacheDB().SetGeoToValidatorList(validatorID, geo)
		if err != nil {
			// log.Warnf("SetGeoToValidatorList err:%v, validatorID : %s, geo : %s", err.Error(), validatorID, geo)
			return err
		}

		// 把池子里的验证节移到验证节点map
		validatorGeo, ok := scheduler.nodePool.poolIDMap.Load(validatorID)
		if ok && validatorGeo != nil {
			vGeo := validatorGeo.(string)
			nodePool := scheduler.nodePool.loadNodePoolMap(vGeo)
			if nodePool != nil {
				// nodePool.candidateNodes[validatorID] = nodeStatusValidator
				nodePool.setVeriftor(validatorID)
			}
		}
	}

	// reset count
	scheduler.nodeManager.resetCandidateAndValidatorCount()

	return nil
}

// Validate edges
func (e *ElectionValidate) startValidate(scheduler *Scheduler) error {
	// 新一轮的抽查
	err := db.GetCacheDB().DelValidateList()
	if err != nil {
		return err
	}

	sID, err := db.GetCacheDB().IncrValidateRoundID()
	if err != nil {
		return err
	}
	e.roundID = fmt.Sprintf("%d", sID)

	e.seed = time.Now().UnixNano()

	e.fidsMap = make(map[string][]string)

	// find validators
	validators, err := db.GetCacheDB().GetValidatorsWithList()
	if err != nil {
		return err
	}

	geoMap := make(map[string][]string)

	for _, validatorID := range validators {
		geos, err := db.GetCacheDB().GetGeoWithValidatorList(validatorID)
		if err != nil {
			log.Warnf("GetGeoWithValidatorList err:%v,validatorID:%v", err.Error(), validatorID)
			continue
		}

		for _, geo := range geos {
			if list, ok := geoMap[geo]; ok {
				geoMap[geo] = append(list, validatorID)
			} else {
				geoMap[geo] = []string{validatorID}
			}
		}
	}

	validatorMap := make(map[string][]string)

	for geo, validatorList := range geoMap {
		nodePool := scheduler.nodePool.loadNodePoolMap(geo)
		if nodePool == nil {
			log.Warnf("validates loadGroupMap is nil ,geo:%v", geo)
			continue
		}

		verifiedList := make([]string, 0)
		// rand group
		for deviceID := range nodePool.edgeNodes {
			verifiedList = append(verifiedList, deviceID)
		}

		for deviceID := range nodePool.candidateNodes {
			verifiedList = append(verifiedList, deviceID)
		}

		// 被验证者 平均分给 验证者
		verifiedLen := len(verifiedList)
		validatorLen := len(validatorList)
		num := verifiedLen / validatorLen // 每个验证者分配到的节点数量
		if verifiedLen%validatorLen > 0 {
			num++
		}

		for i := 0; i < validatorLen; i++ {
			validatorID := validatorList[i]

			end := (i * num) + num
			if end > verifiedLen {
				end = verifiedLen
			}
			newList := verifiedList[i*num : end]

			if list, ok := validatorMap[validatorID]; ok {
				validatorMap[validatorID] = append(list, newList...)
			} else {
				validatorMap[validatorID] = newList
			}
		}
	}

	for validatorID, list := range validatorMap {
		req, errList := e.getReqValidate(scheduler, validatorID, list)
		offline := false
		validator := scheduler.nodeManager.getCandidateNode(validatorID)
		if validator != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			// 请求抽查
			err := validator.nodeAPI.ValidateData(ctx, req)
			if err != nil {
				log.Warnf("ValidateData err:%v, DeviceId:%v", err.Error(), validatorID)
				offline = true
			}

		} else {
			offline = true
		}

		if offline {
			// 记录扣罚 验证者
			err = db.GetCacheDB().SetNodeToValidateErrorList(e.roundID, validatorID)
			if err != nil {
				log.Errorf("SetNodeToValidateErrorList ,err:%v,deviceID:%v", err.Error(), validatorID)
			}
		}

		for _, deviceID := range errList {
			// 记录扣罚 被验证者
			err = db.GetCacheDB().SetNodeToValidateErrorList(e.roundID, deviceID)
			if err != nil {
				log.Errorf("SetNodeToValidateErrorList ,err:%v,deviceID:%v", err.Error(), deviceID)
			}
		}

		log.Infof("validatorID :%v, List:%v", validatorID, list)
	}

	t := time.NewTimer(time.Duration(e.duration*2) * time.Second)

	for {
		select {
		case <-t.C:
			return e.checkValidateTimeOut()
		}
	}
}
