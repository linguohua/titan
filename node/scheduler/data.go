package scheduler

import (
	"fmt"
	"math/rand"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/scheduler/db"

	"golang.org/x/xerrors"
)

// get all cache fail cid
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
		if tag == dataDefaultTag {
			cs = append(cs, cid)
		}
	}

	return cs, nil
}

// delete data records
func (n *Node) deleteDataRecords(cids []string) (map[string]string, error) {
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
			continue
		}

		err = db.GetCacheDB().DelCacheDataTagInfo(deviceID, tag)
		if err != nil {
			errList[cid] = fmt.Sprintf("DelCacheDataTagInfo err : %v", err.Error())
			continue
		}
	}

	return errList, nil
}

func (n *Node) getReqCacheDatas(scheduler *Scheduler, cids []string, isEdge bool) ([]api.ReqCacheData, []string) {
	geoInfo := &n.geoInfo
	reqList := make([]api.ReqCacheData, 0)

	if !isEdge {
		cs := make([]string, 0)

		for _, cid := range cids {
			err := n.nodeCacheReady(cid)
			if err != nil {
				// already Cache
				continue
			}
			cs = append(cs, cid)
		}

		reqList = append(reqList, api.ReqCacheData{Cids: cs})
		return reqList, nil
	}

	notFindCandidateData := make([]string, 0)
	// if node is edge , find data whit candidate
	csMap := make(map[string][]string)
	for _, cid := range cids {
		candidates, err := scheduler.nodeManager.getCandidateNodesWithData(cid, geoInfo)
		if err != nil || len(candidates) < 1 {
			// not find candidate
			notFindCandidateData = append(notFindCandidateData, cid)
			continue
		}

		err = n.nodeCacheReady(cid)
		if err != nil {
			// already Cache
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

	return reqList, notFindCandidateData
}

// node Cache Result
func (n *Node) nodeCacheResult(info *api.CacheResultInfo) (string, error) {
	deviceID := n.deviceInfo.DeviceId
	log.Infof("nodeCacheResult deviceID:%v,info:%v", deviceID, info)

	v, err := db.GetCacheDB().GetCacheDataInfo(deviceID, info.Cid)
	if err == nil && v != dataDefaultTag {
		return v, nil
	}

	if !info.IsOK {
		return "", nil
	}

	tag, err := db.GetCacheDB().IncrNodeCacheTag(deviceID)
	if err != nil {
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

	return tagStr, db.GetCacheDB().SetNodeToCacheList(deviceID, info.Cid)
}

func (n *Node) newCacheDataTag(cid string) (string, error) {
	deviceID := n.deviceInfo.DeviceId

	v, err := db.GetCacheDB().GetCacheDataInfo(deviceID, cid)
	if err == nil && v != dataDefaultTag {
		return v, xerrors.Errorf("already cache")
	}

	tag, err := db.GetCacheDB().IncrNodeCacheTag(deviceID)
	if err != nil {
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

func randomNum(start, end int) int {
	// rand.Seed(time.Now().UnixNano())
	max := end - start
	if max <= 0 {
		return start
	}

	x := rand.Intn(max)
	return start + x
}
