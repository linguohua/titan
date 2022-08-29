package scheduler

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/scheduler/db"
	"github.com/ouqiang/timewheel"
	"golang.org/x/xerrors"
)

var (
	seed     = int64(1)
	duration = 10

	verifyMax = 100 // 每次抽查block个数上限

	verifiedMax = 20 // 每个验证节点一次验证的被验证节点数

	roundID string
	fidsMap map[string][]string

	timewheelElection *timewheel.TimeWheel
	electionTime      = 60 * 24 // 选举时间间隔 (分钟)

	timewheelVerify *timewheel.TimeWheel
	verifyTime      = 60 // 抽查时间间隔 (分钟)

	unassignedEdgeMap = make(map[string]int) // 未被分配到的边缘节点组
)

// 边缘节点登录的时候
// 1.同个区域边缘节点组合成集群,每个集群的上行带宽为1G
// 选举过程
// 2.选举验证节点的时候,根据区域的边缘节点集群,看看这个区域需要多少个验证节点(要考虑下行宽带)
// 3.如果某个区域的验证节点不足,则需要再选出附近空闲的验证节点
// 验证过程
// 4.每个候选节点根据下行带宽,一次验证N个集群

// InitVerifyTimewheel init timer
func InitVerifyTimewheel() {
	// 选举定时器
	timewheelElection = timewheel.New(1*time.Second, 3600, func(_ interface{}) {
		electionValidators()
		// 继续添加定时器
		timewheelElection.AddTimer(time.Duration(electionTime)*60*time.Second, "election", nil)
	})
	timewheelElection.Start()
	// 开始一个事件处理
	timewheelElection.AddTimer(time.Duration(1)*60*time.Second, "election", nil)

	// 抽查定时器
	timewheelVerify = timewheel.New(1*time.Second, 3600, func(_ interface{}) {
		startVerify()
		// 继续添加定时器
		timewheelVerify.AddTimer(time.Duration(verifyTime)*60*time.Second, "verify", nil)
	})
	timewheelVerify.Start()
	// 开始一个事件处理
	timewheelVerify.AddTimer(time.Duration(2)*60*time.Second, "verify", nil)
}

func getReqVerify(validatorID string, list []string) ([]api.ReqVerify, []string) {
	// validatorID := candidate.deviceInfo.DeviceId

	req := make([]api.ReqVerify, 0)

	errList := make([]string, 0)

	for _, deviceID := range list {
		addr := ""
		edgeNode := getEdgeNode(deviceID)
		if edgeNode == nil {
			candidateNode := getCandidateNode(deviceID)
			if candidateNode == nil {
				errList = append(errList, deviceID)
				continue
			}
			addr = candidateNode.addr
		} else {
			addr = edgeNode.addr
		}

		// 查看节点缓存了哪些数据
		datas, err := db.GetCacheDB().GetCacheDataInfos(deviceID)
		if err != nil {
			log.Warnf("verify GetCacheDataInfos err:%v,DeviceId:%v", err.Error(), deviceID)
			continue
		}

		if len(datas) <= 0 {
			continue
		}

		// max, err := db.GetCacheDB().GetNodeCacheTag(edgeID)
		// if err != nil {
		// 	log.Warnf("verify GetNodeCacheTag err:%v,DeviceId:%v", err.Error(), edgeID)
		// 	continue
		// }

		fids := make([]string, 0)
		for _, tag := range datas {
			fids = append(fids, tag)

			if len(fids) >= verifyMax {
				break
			}
		}

		req = append(req, api.ReqVerify{Seed: seed, EdgeURL: addr, Duration: duration, FIDs: fids, RoundID: roundID})

		fidsMap[deviceID] = fids
		//
		err = db.GetCacheDB().SetVerifyResultInfo(roundID, deviceID, validatorID, "", db.VerifyStatusCreate)
		if err != nil {
			log.Warnf("verify SetVerifyResultInfo err:%v,DeviceId:%v", err.Error(), deviceID)
			continue
		}

		err = db.GetCacheDB().SetNodeToVerifyList(deviceID)
		if err != nil {
			log.Warnf("verify SetNodeToVerifyList err:%v,DeviceId:%v", err.Error(), deviceID)
			continue
		}
	}

	return req, errList
}

func getRandFid(max int, r *rand.Rand) int {
	if max > 0 {
		return r.Intn(max) // 过滤0
	}

	return max
}

func toCidV0(c cid.Cid) (cid.Cid, error) {
	if c.Type() != cid.DagProtobuf {
		return cid.Cid{}, fmt.Errorf("can't convert non-dag-pb nodes to cidv0")
	}
	return cid.NewCidV0(c.Hash()), nil
}

func toCidV1(c cid.Cid) (cid.Cid, error) {
	return cid.NewCidV1(c.Type(), c.Hash()), nil
}

func verifyResult(verifyResults api.VerifyResults) error {
	if verifyResults.RoundID != roundID {
		return xerrors.Errorf("roundID err")
	}

	edgeID := verifyResults.DeviceID
	// varify Result

	status := db.VerifyStatusSuccess
	msg := ""

	defer func() {
		err := db.GetCacheDB().SetVerifyResultInfo(roundID, edgeID, "", msg, status)
		if err != nil {
			log.Warnf("SetVerifyResultInfo err:%v", err.Error())
		}

		err = db.GetCacheDB().DelNodeWithVerifyList(edgeID)
		if err != nil {
			log.Warnf("DelNodeWithVerifyList err:%v", err.Error())
		}
	}()

	// TODO 判断带宽 超时时间等等
	if verifyResults.IsTimeout {
		status = db.VerifyStatusTimeOut
		msg = fmt.Sprint("Time out")
		return nil
	}

	r := rand.New(rand.NewSource(seed))
	rlen := len(verifyResults.Results)

	if rlen <= 0 {
		status = db.VerifyStatusFail
		msg = fmt.Sprint("Results is nil")
		return nil
	}

	list := fidsMap[edgeID]
	max := len(list)

	log.Infof("verifyResult:%v", edgeID)

	for i := 0; i < rlen; i++ {
		index := getRandFid(max, r)
		result := verifyResults.Results[i]

		fidStr := list[index]
		// log.Infof("fidStr:%v,resultFid:%v,index:%v", fidStr, result.Fid, i)
		if fidStr != result.Fid {
			status = db.VerifyStatusFail
			msg = fmt.Sprintf("fidStr:%v,resultFid:%v,index:%v", fidStr, result.Fid, i)
			break
		}

		if result.Cid == "" {
			// cid, err := db.GetCacheDB().GetCacheDataTagInfo(edgeID, fidStr)
			// if err == nil && cid != "" {
			status = db.VerifyStatusFail
			msg = fmt.Sprintf("resultCid:%v,resultFid:%v", result.Cid, result.Fid)
			break
			// }

			// 确实没有这个cid
			// continue
		}

		tag, err := db.GetCacheDB().GetCacheDataInfo(edgeID, result.Cid)
		if err != nil {
			status = db.VerifyStatusFail
			msg = fmt.Sprintf("GetCacheDataInfo err:%v,edgeID:%v,resultCid:%v,resultFid:%v", err.Error(), edgeID, result.Cid, result.Fid)
			break
		}

		if tag != fidStr {
			status = db.VerifyStatusFail
			msg = fmt.Sprintf("tag:%v,fidStr:%v,Cid:%v", tag, fidStr, result.Cid)
			break
		}
	}

	return nil
}

// 检查有没有超时的抽查
func checkVerifyTimeOut() error {
	sID, err := db.GetCacheDB().GetVerifyID()
	if err != nil {
		return err
	}

	edgeIDs, err := db.GetCacheDB().GetNodesWithVerifyList()
	if err != nil {
		return err
	}

	if len(edgeIDs) > 0 {
		log.Infof("checkVerifyTimeOut list:%v", edgeIDs)

		for _, edgeID := range edgeIDs {
			err = db.GetCacheDB().SetVerifyResultInfo(sID, edgeID, "", "", db.VerifyStatusTimeOut)
			if err != nil {
				log.Warnf("checkVerifyTimeOut SetVerifyResultInfo err:%v,DeviceId:%v", err.Error(), edgeID)
				continue
			}

			err = db.GetCacheDB().DelNodeWithVerifyList(edgeID)
			if err != nil {
				log.Warnf("checkVerifyTimeOut DelNodeWithVerifyList err:%v,DeviceId:%v", err.Error(), edgeID)
				continue
			}
		}
	}

	return nil
}

// 重置 验证者数据
func cleanValidators() error {
	validators, err := db.GetCacheDB().GetValidatorsWithList()
	if err != nil {
		return err
	}

	for _, validator := range validators {
		err = db.GetCacheDB().DelValidatorGeoList(validator)
		if err != nil {
			log.Warnf("DelValidatorGeoList err:%v, validator:%v", err.Error(), validator)
		}

		node := getCandidateNode(validator)
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
func electionValidators() error {
	// 每个城市 选出X个验证者
	// 每隔Y时间 重新选举
	err := cleanValidators()
	if err != nil {
		return err
	}

	// 初始化池子
	nodesToPool()
	testPrintlnPoolMap()

	alreadyAssignValidatorMap := make(map[string]string) // 已被分配的验证者
	unAssignCandidates := make([]string, 0)              // 空闲的候选者
	// 缺验证节点数量
	lackValidatorMap := make(map[string]int)

	poolMap.Range(func(key, value interface{}) bool {
		geo := key.(string)
		pool := value.(*NodePool)
		if pool == nil {
			return true
		}

		edgeNum := len(pool.edgeNodes)
		candidateNum := len(pool.candidateNodes)
		if edgeNum <= 0 && candidateNum <= 0 {
			return true
		}

		// 初始化状态
		pool.resetNodeStataus()

		nodeTotalNum := edgeNum + candidateNum
		n := 0
		if nodeTotalNum%(verifiedMax+1) > 0 {
			n = 1
		}
		needVeriftorNum := nodeTotalNum/(verifiedMax+1) + n

		if candidateNum == needVeriftorNum {
			for deviceID := range pool.candidateNodes {
				alreadyAssignValidatorMap[deviceID] = geo
			}
		} else if candidateNum > needVeriftorNum {
			// 选出验证者 把多余的候选节点标记成被验证者
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
		validator := getCandidateNode(validatorID)
		if validator != nil {
			validator.isValidator = true
		}

		err = db.GetCacheDB().SetValidatorToList(validatorID)
		if err != nil {
			log.Warnf("SetValidatorToList err:%v, validatorID : %s", err.Error(), validatorID)
		}

		err = db.GetCacheDB().SetGeoToValidatorList(validatorID, geo)
		if err != nil {
			log.Warnf("SetGeoToValidatorList err:%v, validatorID : %s, geo : %s", err.Error(), validatorID, geo)
		}

		// 把池子里的验证节点做个标记
		validatorGeo, ok := poolIDMap.Load(validatorID)
		if ok && validatorGeo != nil {
			vGeo := validatorGeo.(string)
			nodePool := loadNodePoolMap(vGeo)
			if nodePool != nil {
				nodePool.candidateNodes[validatorID] = nodeStatusValidator
			}
		}
	}

	// reset count
	resetCandidateAndValidatorCount()

	return nil
}

// Verify edges
func startVerify() error {
	// 新一轮的抽查
	err := db.GetCacheDB().DelVerifyList()
	if err != nil {
		return err
	}

	sID, err := db.GetCacheDB().IncrVerifyID()
	if err != nil {
		return err
	}
	roundID = fmt.Sprintf("%d", sID)

	seed = sID

	fidsMap = make(map[string][]string)

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
		nodePool := loadNodePoolMap(geo)
		if nodePool == nil {
			log.Warnf("verifys loadGroupMap is nil ,geo:%v", geo)
			continue
		}

		verifiedList := make([]string, 0)
		// rand group
		for deviceID, strtus := range nodePool.edgeNodes {
			if strtus == nodeStatusValidator {
				continue
			}

			verifiedList = append(verifiedList, deviceID)
		}

		for deviceID, strtus := range nodePool.candidateNodes {
			if strtus == nodeStatusValidator {
				continue
			}

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
		req, errList := getReqVerify(validatorID, list)
		offline := false
		validator := getCandidateNode(validatorID)
		if validator != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			// 请求抽查
			err := validator.nodeAPI.VerifyData(ctx, req)
			if err != nil {
				log.Warnf("VerifyData err:%v, DeviceId:%v", err.Error(), validatorID)
				offline = true
			}

		} else {
			offline = true
		}

		if offline {
			// 记录扣罚 验证者
			err = db.GetCacheDB().SetNodeToVerifyOfflineList(roundID, validatorID)
			if err != nil {
				log.Warnf("SetNodeToVerifyOfflineList ,err:%v,deviceID:%v", err.Error(), validatorID)
			}
		}

		for _, deviceID := range errList {
			// 记录扣罚 被验证者
			err = db.GetCacheDB().SetNodeToVerifyOfflineList(roundID, deviceID)
			if err != nil {
				log.Warnf("SetNodeToVerifyOfflineList ,err:%v,deviceID:%v", err.Error(), deviceID)
			}
		}

		log.Infof("validatorID :%v, List:%v", validatorID, list)
	}

	t := time.NewTimer(time.Duration(duration*2) * time.Second)

	for {
		select {
		case <-t.C:
			return checkVerifyTimeOut()
		}
	}
}
