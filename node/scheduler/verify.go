package scheduler

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/scheduler/db"
	"github.com/linguohua/titan/region"
	"github.com/ouqiang/timewheel"
	"golang.org/x/xerrors"
)

var (
	seed     = int64(1)
	duration = 10

	roundID     string
	maxRangeMap map[string]int

	timewheelElection *timewheel.TimeWheel
	electionTime      = 60 // 选举时间间隔 (分钟)

	timewheelSpotCheck *timewheel.TimeWheel
	spotCheckTime      = 5 // 抽查时间间隔 (分钟)
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
	timewheelSpotCheck = timewheel.New(1*time.Second, 3600, func(_ interface{}) {
		startSpotCheck()
		// 继续添加定时器
		timewheelSpotCheck.AddTimer(time.Duration(spotCheckTime)*60*time.Second, "spotCheck", nil)
	})
	timewheelSpotCheck.Start()
	// 开始一个事件处理
	timewheelSpotCheck.AddTimer(time.Duration(2)*60*time.Second, "spotCheck", nil)
}

func spotCheck(candidate *CandidateNode, edges []*EdgeNode) {
	validatorID := candidate.deviceInfo.DeviceId

	req := make([]api.ReqVerify, 0)

	for _, edge := range edges {
		// 查看节点缓存了哪些数据
		edgeID := edge.deviceInfo.DeviceId

		datas, err := db.GetCacheDB().GetCacheDataInfos(edgeID)
		if err != nil {
			log.Warnf("spotCheck GetCacheDataInfos err:%v,DeviceId:%v", err.Error(), edgeID)
			continue
		}

		if len(datas) <= 0 {
			continue
		}

		max, err := db.GetCacheDB().GetNodeCacheTag(edgeID)
		if err != nil {
			log.Warnf("spotCheck GetNodeCacheTag err:%v,DeviceId:%v", err.Error(), edgeID)
			continue
		}

		req = append(req, api.ReqVerify{Seed: seed, EdgeURL: edge.addr, Duration: duration, MaxRange: int(max), RoundID: roundID})

		maxRangeMap[edgeID] = int(max)
		//
		err = db.GetCacheDB().SetSpotCheckResultInfo(roundID, edgeID, validatorID, "", db.SpotCheckStatusCreate)
		if err != nil {
			log.Warnf("spotCheck SetSpotCheckResultInfo err:%v,DeviceId:%v", err.Error(), edgeID)
			continue
		}

		err = db.GetCacheDB().SetNodeToSpotCheckList(edgeID)
		if err != nil {
			log.Warnf("spotCheck SetNodeToSpotCheckList err:%v,DeviceId:%v", err.Error(), edgeID)
			continue
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// 请求抽查
	err := candidate.nodeAPI.VerifyData(ctx, req)
	if err != nil {
		log.Errorf("VerifyData err:%v, DeviceId:%v", err.Error(), validatorID)
		// TODO 记录到数据库
		return
	}
}

func getRandFid(max int, r *rand.Rand) int {
	if max > 1 {
		return r.Intn(max-1) + 1 // 过滤0
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

func spotCheckResult(verifyResults api.VerifyResults) error {
	if verifyResults.RoundID != roundID {
		return xerrors.Errorf("roundID err")
	}

	edgeID := verifyResults.DeviceID
	// varify Result

	status := db.SpotCheckStatusSuccess
	msg := ""
	// TODO 判断带宽 超时时间等等
	r := rand.New(rand.NewSource(seed))
	rlen := len(verifyResults.Results)

	if rlen <= 0 {
		status = db.SpotCheckStatusFail
		msg = fmt.Sprint("Results is nil")
	}

	max := maxRangeMap[edgeID]

	log.Infof("spotCheckResult:%v", edgeID)

	for i := 0; i < rlen; i++ {
		fid := getRandFid(max, r)
		result := verifyResults.Results[i]

		fidStr := fmt.Sprintf("%d", fid)
		if fidStr != result.Fid {
			status = db.SpotCheckStatusFail
			msg = fmt.Sprintf("fidStr:%v,resultFid:%v", fidStr, result.Fid)
			break
		}

		if result.Cid == "" {
			cid, err := db.GetCacheDB().GetCacheDataTagInfo(edgeID, fidStr)
			if err == nil && cid != "" {
				status = db.SpotCheckStatusFail
				msg = fmt.Sprintf("GetCacheDataTagInfo err:%v,edgeID:%v,resultCid:%v,resultFid:%v", err, edgeID, result.Cid, result.Fid)
				break
			}
		}

		tag, err := db.GetCacheDB().GetCacheDataInfo(edgeID, result.Cid)
		if err != nil {
			status = db.SpotCheckStatusFail
			msg = fmt.Sprintf("GetCacheDataInfo err:%v,edgeID:%v,resultCid:%v,resultFid:%v", err.Error(), edgeID, result.Cid, result.Fid)
			break
		}

		if tag != fidStr {
			status = db.SpotCheckStatusFail
			msg = fmt.Sprintf("tag:%v,fidStr:%v,Cid:%v", tag, fidStr, result.Cid)
			break
		}
	}

	err := db.GetCacheDB().SetSpotCheckResultInfo(roundID, edgeID, "", msg, status)
	if err != nil {
		return err
	}

	err = db.GetCacheDB().DelNodeWithSpotCheckList(edgeID)
	if err != nil {
		return err
	}

	return nil
}

// 检查有没有超时的抽查
func checkSpotCheckTimeOut() error {
	sID, err := db.GetCacheDB().GetSpotCheckID()
	if err != nil {
		return err
	}

	edgeIDs, err := db.GetCacheDB().GetNodesWithSpotCheckList()
	if err != nil {
		return err
	}

	log.Infof("checkSpotCheckTimeOut list:%v", edgeIDs)

	if len(edgeIDs) > 0 {
		for _, edgeID := range edgeIDs {
			err = db.GetCacheDB().SetSpotCheckResultInfo(sID, edgeID, "", "", db.SpotCheckStatusTimeOut)
			if err != nil {
				log.Warnf("checkSpotCheckTimeOut SetSpotCheckResultInfo err:%v,DeviceId:%v", err.Error(), edgeID)
				continue
			}

			err = db.GetCacheDB().DelNodeWithSpotCheckList(edgeID)
			if err != nil {
				log.Warnf("checkSpotCheckTimeOut DelNodeWithSpotCheckList err:%v,DeviceId:%v", err.Error(), edgeID)
				continue
			}
		}
	}

	return nil
}

// spot check edges
func startSpotCheck() error {
	// 新一轮的抽查
	err := db.GetCacheDB().DelSpotCheckList()
	if err != nil {
		return err
	}

	sID, err := db.GetCacheDB().IncrSpotCheckID()
	if err != nil {
		// log.Errorf("NotifyNodeCacheData getTagWithNode err:%v", err)
		return err
	}
	roundID = fmt.Sprintf("%d", sID)

	maxRangeMap = make(map[string]int)

	// log.Infof("validatorCount:%v,candidateCount:%v", validatorCount, candidateCount)
	// find validators
	validators, err := db.GetCacheDB().GetValidatorsWithList()
	if err != nil {
		return err
	}

	usedGroupID := make([]string, 0)

	for _, validatorID := range validators {
		geos, err := db.GetCacheDB().GetGeoWithValidatorList(validatorID)
		if err != nil {
			log.Warnf("GetGeoWithValidatorList err:%v,validatorID:%v", err.Error(), validatorID)
			continue
		}

		// edge list
		edges := make([]*EdgeNode, 0)

		log.Infof("validator id:%v", validatorID)
		// find edge
		for _, geo := range geos {
			groups := loadGeoGroupMap(geo)
			if groups != nil {
				// rand group
				uIDs, group := getUnassignedGroup(groups, usedGroupID)
				for deviceID := range group.edgeNodeMap {
					edge := getEdgeNode(deviceID)
					if edge != nil {
						log.Infof("edge id:%v", edge.deviceInfo.DeviceId)
						edges = append(edges, edge)
					}
				}
				usedGroupID = uIDs
			} else {
				log.Warnf("spotChecks loadGroupMap is nil ,geo:%v", geo)
			}
		}

		validator := getCandidateNode(validatorID)
		if validator != nil {
			spotCheck(validator, edges)
		}
	}

	t := time.NewTimer(time.Duration(duration*2) * time.Second)

	for {
		select {
		case <-t.C:
			return checkSpotCheckTimeOut()
		}
	}
}

func getUnassignedGroup(groups []string, usedIDs []string) ([]string, *Group) {
	for _, groupID := range groups {
		used := false
		for _, gID := range usedIDs {
			if groupID == gID {
				used = true
			}
		}

		if !used {
			usedIDs = append(usedIDs, groupID)
			group := loadGroupMap(groupID)
			return usedIDs, group
		}
	}

	return usedIDs, nil
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
	testPrintlnEdgeGroupMap()

	// 每个城市 选出X个验证者
	// 每隔Y时间 重新选举
	err := cleanValidators()
	if err != nil {
		return err
	}

	alreadyAssignValidatorMap := make(map[string]string) // 已被分配的验证者
	alreadyAssignValidatorList := make([]string, 0)      // 带宽已经占满的验证者
	// validatorBandwidthMap := make(map[string]int)   // 已用带宽
	// 未被分配到的边缘节点组数
	unassignedEdgeMap := make(map[string]int)

	geoGroupMap.Range(func(key, value interface{}) bool {
		geo := key.(string)
		groups := value.([]string)
		if groups == nil || len(groups) <= 0 {
			return true
		}

		gInfo := region.StringGeoToGeoInfo(geo)
		if gInfo == nil {
			log.Warnf("StringGeoToGeoInfo geo:%v", geo)
			return true
		}

		// 找出这个区域 所有候选节点
		cns, gLevel := findCandidateNodeWithGeo(*gInfo, []string{}, []string{})
		if gLevel == cityLevel && len(cns) > 0 {
			// 这里考虑 验证节点的下行带宽
			totalNum := len(groups)
			for _, c := range randomNums(0, len(cns), len(groups)) {
				totalNum--
				dID := cns[c].deviceInfo.DeviceId
				alreadyAssignValidatorMap[dID] = geo

				// TODO 满了才加入这个列表 (目前有一组就算是满了)
				alreadyAssignValidatorList = append(alreadyAssignValidatorList, dID)
			}

			if totalNum > 0 {
				unassignedEdgeMap[geo] = totalNum
			}
		} else {
			unassignedEdgeMap[geo] = len(groups)
			return true
		}

		return true
	})

	candidateNotEnough := false
	// 处理第一轮未匹配到的边缘节点
	for len(unassignedEdgeMap) > 0 {
		for geo, num := range unassignedEdgeMap {
			gInfo := region.StringGeoToGeoInfo(geo)
			if gInfo == nil {
				log.Warnf("StringGeoToGeoInfo geo:%v", geo)
				continue
			}

			cns, _ := findCandidateNodeWithGeo(*gInfo, []string{}, alreadyAssignValidatorList)
			if len(cns) > 0 {
				for _, c := range randomNums(0, len(cns), num) {
					num--
					dID := cns[c].deviceInfo.DeviceId
					alreadyAssignValidatorMap[dID] = geo

					// TODO 满了才加入这个列表 (目前有一组就算是满了)
					alreadyAssignValidatorList = append(alreadyAssignValidatorList, dID)
				}
				unassignedEdgeMap[geo] = num
				if num == 0 {
					delete(unassignedEdgeMap, geo)
				}
			} else {
				candidateNotEnough = true
			}
		}

		if candidateNotEnough {
			log.Warnf("Candidate Not Enough  assignEdge: %v", unassignedEdgeMap)
			break
		}
	}

	// 记录验证者负责的区域到redis
	for validator, geo := range alreadyAssignValidatorMap {
		getCandidateNode(validator).isValidator = true

		err = db.GetCacheDB().SetValidatorToList(validator)
		if err != nil {
			log.Warnf("SetValidatorToList err:%v, validator : %s", err.Error(), validator)
		}

		err = db.GetCacheDB().SetGeoToValidatorList(validator, geo)
		if err != nil {
			log.Warnf("SetGeoToValidatorList err:%v, validator : %s, geo : %s", err.Error(), validator, geo)
		}
	}

	// reset count
	resetCandidateAndValidatorCount()

	return nil
}
