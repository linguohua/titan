package scheduler

import (
	"fmt"
	"time"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/lib/token"
	"github.com/linguohua/titan/node/helper"
	"github.com/linguohua/titan/node/scheduler/db/cache"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"github.com/linguohua/titan/region"

	"github.com/filecoin-project/go-jsonrpc"
)

// var dataDefaultTag = "-1"

// Location Edge node
type Location struct {
	nodeAPI   api.Locator
	closer    jsonrpc.ClientCloser
	locatorID string
}

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
	deviceInfo      api.DevicesInfo
	geoInfo         *region.GeoInfo
	addr            string
	lastRequestTime time.Time
}

// node online
func (n *Node) setNodeOnline(typeName api.NodeTypeName) error {
	deviceID := n.deviceInfo.DeviceId
	geoInfo := n.geoInfo

	lastTime := time.Now().Format("2006-01-02 15:04:05")
	err := persistent.GetDB().SetNodeInfo(deviceID, &persistent.NodeInfo{
		Geo:      geoInfo.Geo,
		LastTime: lastTime,
		IsOnline: 1,
		NodeType: string(typeName),
		Address:  n.addr,
	})
	if err != nil {
		return err
	}

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
		log.Errorf("node offline SetNodeInfo err : %s ,deviceID : %s", err.Error(), deviceID)
	}
}

// getNodeInfo  get node information
func (n *Node) getNodeInfo(deviceID string) (*persistent.NodeInfo, error) {
	node, err := persistent.GetDB().GetNodeInfo(deviceID)
	if err != nil {
		log.Errorf("getNodeInfo: %s ,deviceID : %s", err.Error(), deviceID)
	}
	return node, nil
}

// filter cached blocks and find download url from candidate
func (n *Node) getReqCacheDatas(nodeManager *NodeManager, cids []string, carFileCid, cacheID string) []api.ReqCacheData {
	reqList := make([]api.ReqCacheData, 0)
	notFindCandidateDatas := make([]api.BlockInfo, 0)

	fidMax, err := cache.GetDB().IncrNodeCacheFid(n.deviceInfo.DeviceId, len(cids))
	if err != nil {
		log.Errorf("deviceID:%s,IncrNodeCacheFid:%s", n.deviceInfo.DeviceId, err.Error())
		return reqList
	}

	csMap := make(map[string][]api.BlockInfo)
	for i, cid := range cids {
		candidates, err := nodeManager.getCandidateNodesWithData(cid)
		if err != nil || len(candidates) < 1 {
			// not find candidate
			notFindCandidateDatas = append(notFindCandidateDatas, api.BlockInfo{Cid: cid, Fid: fmt.Sprintf("%d", fidMax-i)})
			continue
		}

		candidate := candidates[randomNum(0, len(candidates))]

		deviceID := candidate.deviceInfo.DeviceId

		list := csMap[deviceID]
		if list == nil {
			list = make([]api.BlockInfo, 0)
		}

		list = append(list, api.BlockInfo{Cid: cid, Fid: fmt.Sprintf("%d", fidMax-i)})

		csMap[deviceID] = list
	}

	for deviceID, list := range csMap {
		// node := nodeManager.getCandidateNode(deviceID)
		info, err := persistent.GetDB().GetNodeAuthInfo(deviceID)
		if err == nil {
			tk, err := token.GenerateToken(info.SecurityKey, time.Now().Add(helper.DownloadTokenExpireAfter).Unix())
			if err == nil {
				reqList = append(reqList, api.ReqCacheData{BlockInfos: list, DownloadURL: info.URL, DownloadToken: tk, CardFileCid: carFileCid, CacheID: cacheID})

				continue
			}
		}

		notFindCandidateDatas = append(notFindCandidateDatas, list...)
	}

	if len(notFindCandidateDatas) > 0 {
		reqList = append(reqList, api.ReqCacheData{BlockInfos: notFindCandidateDatas, CardFileCid: carFileCid, CacheID: cacheID})
	}

	return reqList
}

func (n *Node) updateAccessAuth(access *api.DownloadServerAccessAuth) error {
	return persistent.GetDB().SetNodeAuthInfo(access)
}
