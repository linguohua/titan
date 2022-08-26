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

	verifyMax = 100 // 每次抽查个数上限

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

func getReqVerify(validatorID string, edges []*EdgeNode) []api.ReqVerify {
	// validatorID := candidate.deviceInfo.DeviceId

	req := make([]api.ReqVerify, 0)

	for _, edge := range edges {
		// 查看节点缓存了哪些数据
		edgeID := edge.deviceInfo.DeviceId

		datas, err := db.GetCacheDB().GetCacheDataInfos(edgeID)
		if err != nil {
			log.Warnf("verify GetCacheDataInfos err:%v,DeviceId:%v", err.Error(), edgeID)
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

		req = append(req, api.ReqVerify{Seed: seed, EdgeURL: edge.addr, Duration: duration, FIDs: fids, RoundID: roundID})

		fidsMap[edgeID] = fids
		//
		err = db.GetCacheDB().SetVerifyResultInfo(roundID, edgeID, validatorID, "", db.VerifyStatusCreate)
		if err != nil {
			log.Warnf("verify SetVerifyResultInfo err:%v,DeviceId:%v", err.Error(), edgeID)
			continue
		}

		err = db.GetCacheDB().SetNodeToVerifyList(edgeID)
		if err != nil {
			log.Warnf("verify SetNodeToVerifyList err:%v,DeviceId:%v", err.Error(), edgeID)
			continue
		}
	}

	return req
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
		// fidStr := fmt.Sprintf("%d", fid)
		if fidStr != result.Fid {
			status = db.VerifyStatusFail
			msg = fmt.Sprintf("fidStr:%v,resultFid:%v", fidStr, result.Fid)
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

	log.Infof("checkVerifyTimeOut list:%v", edgeIDs)

	if len(edgeIDs) > 0 {
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

// Verify edges
func startVerify() error {
	// 新一轮的抽查
	err := db.GetCacheDB().DelVerifyList()
	if err != nil {
		return err
	}

	sID, err := db.GetCacheDB().IncrVerifyID()
	if err != nil {
		// log.Errorf("NotifyNodeCacheData getTagWithNode err:%v", err)
		return err
	}
	roundID = fmt.Sprintf("%d", sID)

	seed = sID

	fidsMap = make(map[string][]string)

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
					} else {
						// 记录扣罚 edge
						err = db.GetCacheDB().SetNodeToVerifyOfflineList(roundID, deviceID)
						if err != nil {
							log.Warnf("SetNodeToVerifyOfflineList ,err:%v,deviceID:%v", err.Error(), deviceID)
						}
					}
				}
				usedGroupID = uIDs
			} else {
				log.Warnf("verifys loadGroupMap is nil ,geo:%v", geo)
			}
		}

		offline := false

		validator := getCandidateNode(validatorID)
		if validator != nil {
			req := getReqVerify(validatorID, edges)

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
			// 记录扣罚 validatorID
			err = db.GetCacheDB().SetNodeToVerifyOfflineList(roundID, validatorID)
			if err != nil {
				log.Warnf("SetNodeToVerifyOfflineList ,err:%v,deviceID:%v", err.Error(), validatorID)
			}
		}
	}

	t := time.NewTimer(time.Duration(duration*2) * time.Second)

	for {
		select {
		case <-t.C:
			return checkVerifyTimeOut()
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
	unassignedEdgeMap = make(map[string]int)

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
