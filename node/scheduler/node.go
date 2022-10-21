package scheduler

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/scheduler/db/cache"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"github.com/linguohua/titan/region"

	"github.com/filecoin-project/go-jsonrpc"
)

var dataDefaultTag = "-1"

// EdgeNode Edge node
type EdgeNode struct {
	nodeAPI api.Edge
	closer  jsonrpc.ClientCloser

	Node
}

// CandidateNode Candidate node
type CandidateNode struct {
	nodeAPI api.Candidate
	closer  jsonrpc.ClientCloser
	// isValidator bool

	Node
}

// Node Common
type Node struct {
	deviceInfo api.DevicesInfo

	geoInfo *region.GeoInfo

	addr string

	lastRequestTime time.Time
}

// node online
func (n *Node) setNodeOnline(typeName api.NodeTypeName) error {
	deviceID := n.deviceInfo.DeviceId
	geoInfo := n.geoInfo

	// oldNodeInfo, err := persistent.GetDB().GetNodeInfo(deviceID)
	// if err == nil {
	// 	if oldNodeInfo.Geo != geoInfo.Geo {
	// 		err = cache.GetDB().RemoveNodeWithGeoList(deviceID, oldNodeInfo.Geo)
	// 		if err != nil {
	// 			log.Errorf("RemoveNodeWithGeoList err:%v,deviceID:%v,Geo:%v", err.Error(), deviceID, oldNodeInfo.Geo)
	// 		}
	// 	}
	// } else {
	// 	if !persistent.GetDB().IsNilErr(err) {
	// 		log.Warnf("GetNodeInfo err:%v,deviceID:%v", err.Error(), deviceID)
	// 	}
	// }
	// log.Infof("oldgeo:%v,newgeo:%v,err:%v", nodeInfo.Geo, geoInfo.Geo, err)

	lastTime := time.Now().Format("2006-01-02 15:04:05")
	err := persistent.GetDB().SetNodeInfo(deviceID, &persistent.NodeInfo{
		Geo:      geoInfo.Geo,
		LastTime: lastTime, IsOnline: 1,
		NodeType: string(typeName),
		Address:  n.addr,
	})
	if err != nil {
		return err
	}

	// err = cache.GetDB().SetNodeToGeoList(deviceID, geoInfo.Geo)
	// if err != nil {
	// 	log.Errorf("SetNodeToGeoList err:%v,deviceID:%v,Geo:%v", err.Error(), deviceID, geoInfo.Geo)
	// 	return err
	// }

	// err = cache.GetDB().SetGeoToList(geoInfo.Geo)
	// if err != nil {
	// 	log.Errorf("SetGeoToList err:%v,Geo:%v", err.Error(), geoInfo.Geo)
	// 	return err
	// }

	return nil
}

// node offline
func (n *Node) setNodeOffline(deviceID string, geoInfo *region.GeoInfo, nodeType api.NodeTypeName, lastTime time.Time) {
	// err := cache.GetDB().RemoveNodeWithGeoList(deviceID, geoInfo.Geo)
	// if err != nil {
	// 	log.Warnf("node offline RemoveNodeWithGeoList err : %v ,deviceID : %v", err.Error(), deviceID)
	// }

	err := persistent.GetDB().SetNodeInfo(deviceID, &persistent.NodeInfo{
		Geo:      geoInfo.Geo,
		LastTime: lastTime.Format("2006-01-02 15:04:05"),
		IsOnline: 0,
		NodeType: string(nodeType),
		Address:  n.addr,
	})
	if err != nil {
		log.Errorf("node offline SetNodeInfo err : %v ,deviceID : %v", err.Error(), deviceID)
	}
}

// getNodeInfo  get node information
func (n *Node) getNodeInfo(deviceID string) (*persistent.NodeInfo, error) {
	node, err := persistent.GetDB().GetNodeInfo(deviceID)
	if err != nil {
		log.Errorf("getNodeInfo: %v ,deviceID : %v", err.Error(), deviceID)
	}
	return node, nil
}

// get all cache fail cid
func (n *Node) getCacheFailCids() []string {
	deviceID := n.deviceInfo.DeviceId

	infos, err := persistent.GetDB().GetBlockInfos(deviceID)
	if err != nil {
		return nil
	}

	if len(infos) <= 0 {
		return nil
	}

	cs := make([]string, 0)
	for cid, tag := range infos {
		if tag == dataDefaultTag {
			cs = append(cs, cid)
		}
	}

	return cs

	// cids, err := cache.GetDB().GetBlocksWithNodeFailList(deviceID)
	// if err != nil {
	// 	return nil
	// }

	// return cids
}

// delete block records
func (n *Node) deleteBlockRecords(cids []string) (map[string]string, error) {
	deviceID := n.deviceInfo.DeviceId

	errList := make(map[string]string, 0)
	for _, cid := range cids {
		err := cache.GetDB().RemoveNodeWithCacheList(deviceID, cid)
		if err != nil {
			errList[cid] = err.Error()
			continue
		}

		err = persistent.GetDB().RemoveBlockInfo(deviceID, cid)
		if err != nil {
			errList[cid] = fmt.Sprintf("RemoveBlockInfo err : %v", err.Error())
			continue
		}

	}

	return errList, nil
}

// filter cached blocks and find download url from candidate
func (n *Node) getReqCacheDatas(nodeManager *NodeManager, cids []string) ([]api.ReqCacheData, []string) {
	reqList := make([]api.ReqCacheData, 0)

	// if !isEdge {
	// 	reqList = append(reqList, api.ReqCacheData{Cids: cids})
	// 	return reqList, nil
	// }

	notFindCandidateData := make([]string, 0)
	// if node is edge , find data with candidate
	csMap := make(map[string][]string)
	for _, cid := range cids {
		candidates, err := nodeManager.getCandidateNodesWithData(cid, n.geoInfo)
		if err != nil || len(candidates) < 1 {
			// not find candidate
			notFindCandidateData = append(notFindCandidateData, cid)
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
		node := nodeManager.getCandidateNode(deviceID)
		if node != nil {
			reqList = append(reqList, api.ReqCacheData{Cids: list, CandidateURL: node.addr})
		}
	}

	if len(notFindCandidateData) > 0 {
		reqList = append(reqList, api.ReqCacheData{Cids: notFindCandidateData})
	}

	return reqList, notFindCandidateData
}

// cache block Result
// TODO save to sql
func (n *Node) cacheBlockResult(info *api.CacheResultInfo) (string, error) {
	deviceID := n.deviceInfo.DeviceId
	log.Infof("nodeCacheResult deviceID:%v,info:%v", deviceID, info)

	isExist := false
	v, err := persistent.GetDB().GetBlockFidWithCid(deviceID, info.Cid)
	if err == nil {
		if v != dataDefaultTag {
			return v, nil
		}

		isExist = true
	}

	// defer func() {
	// 	if err != nil {
	// 		err = cache.GetDB().SetBlockToNodeFailList(deviceID, info.Cid)
	// 		if err != nil {
	// 			log.Warnf("nodeCacheResult SetBlockToNodeFailList err:%v,deviceID:%v,cid:%v", err.Error(), deviceID, info.Cid)
	// 		}
	// 	} else {
	// 		err = cache.GetDB().RemoveBlockWithNodeFailList(deviceID, info.Cid)
	// 		if err != nil {
	// 			log.Warnf("nodeCacheResult SetBlockToNodeFailList err:%v,deviceID:%v,cid:%v", err.Error(), deviceID, info.Cid)
	// 		}
	// 	}
	// }()

	if !info.IsOK {
		return "", nil
	}

	fid, err := cache.GetDB().IncrNodeCacheFid(deviceID)
	if err != nil {
		return "", err
	}

	fidStr := fmt.Sprintf("%d", fid)

	err = persistent.GetDB().SetBlockInfo(deviceID, info.Cid, fidStr, isExist)
	if err != nil {
		return "", err
	}

	// err = cache.GetDB().SetBlockCidWithFid(deviceID, info.Cid, fidStr)
	// if err != nil {
	// 	return "", err
	// }

	return fidStr, cache.GetDB().SetNodeToCacheList(deviceID, info.Cid)
}

// func (n *Node) cacheBlockReady(cid string) error {
// 	deviceID := n.deviceInfo.DeviceId

// 	isExist := false
// 	v, err := persistent.GetDB().GetBlockFidWithCid(deviceID, cid)
// 	if err == nil {
// 		if v != dataDefaultTag {
// 			return xerrors.Errorf("already cache")
// 		}

// 		isExist = true
// 	}

// 	return persistent.GetDB().SetBlockInfo(deviceID, cid, dataDefaultTag, isExist)
// }

func randomNum(start, end int) int {
	// rand.Seed(time.Now().UnixNano())
	max := end - start
	if max <= 0 {
		return start
	}

	x := rand.Intn(max)
	return start + x
}
