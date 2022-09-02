package scheduler

import (
	"fmt"
	"math/rand"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/scheduler/db"

	"golang.org/x/xerrors"
)

// 检查缓存失败的cid
func (n *Node) getCacheFailCids() ([]string, error) {
	deviceID := n.deviceInfo.DeviceId

	infos, err := db.GetCacheDB().GetCacheDataInfos(deviceID)
	if err != nil {
		return nil, err
	}

	if len(infos) <= 0 {
		return nil, nil
	}

	cs := make([]string, 0)

	for cid, tag := range infos {
		// isInCacheList, err := db.GetCacheDB().IsNodeInCacheList(cid, deviceID)
		if tag == fmt.Sprintf("%d", -1) {
			cs = append(cs, cid)
		}
	}

	return cs, nil
}

// NotifyNodeCacheData Cache Data
// func (n *Node) cacheDataOfNode(scheduler *Scheduler, cids []string) ([]string, error) {
// 	deviceID := n.deviceInfo.DeviceId
// 	// 判断device是什么节点
// 	edge := scheduler.nodeManager.getEdgeNode(deviceID)
// 	candidate := scheduler.nodeManager.getCandidateNode(deviceID)
// 	if edge == nil && candidate == nil {
// 		return nil, xerrors.Errorf("node not find:%v", deviceID)
// 	}
// 	if edge != nil && candidate != nil {
// 		return nil, xerrors.New(fmt.Sprintf("node error ,deviceID:%v", deviceID))
// 	}

// 	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
// 	defer cancel()

// 	errList := make([]string, 0)

// 	if edge != nil {
// 		reqDatas := n.getReqCacheData(scheduler, cids, true)

// 		for _, reqData := range reqDatas {
// 			err := edge.nodeAPI.CacheData(ctx, reqData)
// 			if err != nil {
// 				log.Errorf("edge CacheData err:%v,url:%v,cids:%v", err.Error(), reqData.CandidateURL, reqData.Cids)
// 				errList = append(errList, reqData.CandidateURL)
// 			}
// 		}

// 		return errList, nil
// 	}

// 	if candidate != nil {
// 		reqDatas := n.getReqCacheData(scheduler, cids, false)

// 		for _, reqData := range reqDatas {
// 			err := candidate.nodeAPI.CacheData(ctx, reqData)
// 			if err != nil {
// 				log.Errorf("candidate CacheData err:%v,url:%v,cids:%v", err.Error(), reqData.CandidateURL, reqData.Cids)
// 				errList = append(errList, reqData.CandidateURL)
// 			}
// 		}

// 		return errList, nil
// 	}

// 	return nil, nil
// }

func (n *Node) deleteDataRecord(cids []string) (map[string]string, error) {
	deviceID := n.deviceInfo.DeviceId

	errList := make(map[string]string, 0)
	for _, cid := range cids {
		err := db.GetCacheDB().DelNodeWithCacheList(deviceID, cid)
		if err != nil {
			errList[cid] = err.Error()
			continue
		}

		tag, err := db.GetCacheDB().GetCacheDataInfo(deviceID, cid)
		if err != nil {
			if db.GetCacheDB().IsNilErr(err) {
				continue
			}
			errList[cid] = fmt.Sprintf("GetCacheDataInfo err : %v", err.Error())
			continue
		}

		err = db.GetCacheDB().DelCacheDataInfo(deviceID, cid)
		if err != nil {
			errList[cid] = fmt.Sprintf("DelCacheDataInfo err : %v", err.Error())
			// TODO 如果这里出错 上面的记录要回滚或者要再次尝试删info
			continue
		}

		err = db.GetCacheDB().DelCacheDataTagInfo(deviceID, tag)
		if err != nil {
			errList[cid] = fmt.Sprintf("DelCacheDataTagInfo err : %v", err.Error())
			// TODO 如果这里出错 上面的记录要回滚或者要再次尝试删info
			continue
		}
	}

	return errList, nil
}

func (n *Node) getReqCacheData(scheduler *Scheduler, cids []string, isEdge bool) []api.ReqCacheData {
	geoInfo := &n.geoInfo

	alreadyCacheCids := make([]string, 0)
	notFindNodeCids := make([]string, 0)
	reqList := make([]api.ReqCacheData, 0)

	if !isEdge {
		cs := make([]string, 0)

		for _, cid := range cids {
			err := n.nodeCacheReady(cid)
			if err != nil {
				// log.Warnf("cacheDataOfNode nodeCacheReady err:%v,cid:%v", err, cid)
				alreadyCacheCids = append(alreadyCacheCids, cid)
				continue
			}

			cs = append(cs, cid)
		}

		reqList = append(reqList, api.ReqCacheData{Cids: cs})

		return reqList
	}

	// 如果是边缘节点的话 要在候选节点里面找资源
	csMap := make(map[string][]string)

	for _, cid := range cids {
		// 看看哪个候选节点有这个cid
		candidates, err := scheduler.nodeManager.getCandidateNodesWithData(cid, geoInfo)
		if err != nil || len(candidates) < 1 {
			// log.Warnf("cacheDataOfNode getCandidateNodesWithData err:%v,len:%v,cid:%v", err, len(candidates), cid)
			notFindNodeCids = append(notFindNodeCids, cid)
			continue
		}

		err = n.nodeCacheReady(cid)
		if err != nil {
			// log.Warnf("cacheDataOfNode nodeCacheReady err:%v,cid:%v", err, cid)
			alreadyCacheCids = append(alreadyCacheCids, cid)
			continue
		}

		candidate := candidates[randomNum(0, len(candidates))]

		list := csMap[candidate.deviceInfo.DeviceId]
		if list == nil {
			list = make([]string, 0)
		}
		list = append(list, cid)

		csMap[candidate.deviceInfo.DeviceId] = list
	}

	for deviceID, list := range csMap {
		node := scheduler.nodeManager.getCandidateNode(deviceID)
		if node != nil {
			reqList = append(reqList, api.ReqCacheData{Cids: list, CandidateURL: node.addr})
		}

	}

	return reqList
}

// NodeCacheResult Device Cache Result
func (n *Node) nodeCacheResult(info *api.CacheResultInfo) (string, error) {
	deviceID := n.deviceInfo.DeviceId

	log.Infof("nodeCacheResult deviceID:%v,info:%v", deviceID, info)
	v, err := db.GetCacheDB().GetCacheDataInfo(deviceID, info.Cid)
	if err == nil && v != dataDefaultTag {
		return v, nil
	}

	if !info.IsOK {
		// return "", db.GetCacheDB().DelCacheDataInfo(deviceID, cid)
		return "", nil
	}

	tag, err := db.GetCacheDB().IncrNodeCacheTag(deviceID)
	if err != nil {
		// log.Errorf("NotifyNodeCacheData getTagWithNode err:%v", err)
		return "", err
	}

	tagStr := fmt.Sprintf("%d", tag)

	err = db.GetCacheDB().SetCacheDataInfo(deviceID, info.Cid, tagStr)
	if err != nil {
		return "", err
	}

	err = db.GetCacheDB().SetCacheDataTagInfo(deviceID, info.Cid, tagStr)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%d", tag), db.GetCacheDB().SetNodeToCacheList(deviceID, info.Cid)
}

func (n *Node) newCacheDataTag(cid string) (string, error) {
	deviceID := n.deviceInfo.DeviceId

	v, err := db.GetCacheDB().GetCacheDataInfo(deviceID, cid)
	if err == nil && v != dataDefaultTag {
		return v, xerrors.Errorf("already cache")
	}

	tag, err := db.GetCacheDB().IncrNodeCacheTag(deviceID)
	if err != nil {
		// log.Errorf("NotifyNodeCacheData getTagWithNode err:%v", err)
		return "", err
	}

	return fmt.Sprintf("%d", tag), nil
}

// Node Cache ready
func (n *Node) nodeCacheReady(cid string) error {
	deviceID := n.deviceInfo.DeviceId

	v, err := db.GetCacheDB().GetCacheDataInfo(deviceID, cid)
	if err == nil && v != dataDefaultTag {
		return xerrors.Errorf("already cache")
	}

	return db.GetCacheDB().SetCacheDataInfo(deviceID, cid, dataDefaultTag)
}

// 生成[start,end)结束的随机数
func randomNum(start, end int) int {
	// rand.Seed(time.Now().UnixNano())

	max := end - start
	if max <= 0 {
		return start
	}

	x := rand.Intn(max)
	return start + x
}
