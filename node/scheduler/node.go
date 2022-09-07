package scheduler

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/scheduler/db"
	"github.com/linguohua/titan/region"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-jsonrpc"
)

// EdgeNode Edge node
type EdgeNode struct {
	nodeAPI api.Edge
	closer  jsonrpc.ClientCloser

	Node
}

// CandidateNode Candidate node
type CandidateNode struct {
	nodeAPI     api.Candidate
	closer      jsonrpc.ClientCloser
	isValidator bool

	Node
}

// Node Common
type Node struct {
	deviceInfo api.DevicesInfo

	geoInfo region.GeoInfo

	addr string

	lastRequestTime time.Time
}

// node online
func (n *Node) online(deviceID string, onlineTime int64, geoInfo *region.GeoInfo, typeName api.NodeTypeName) error {
	oldNodeInfo, err := db.GetCacheDB().GetNodeInfo(deviceID)
	if err == nil {
		if oldNodeInfo.Geo != geoInfo.Geo {
			err = db.GetCacheDB().RemoveNodeWithGeoList(deviceID, oldNodeInfo.Geo)
			if err != nil {
				log.Errorf("RemoveNodeWithGeoList err:%v,deviceID:%v,Geo:%v", err.Error(), deviceID, oldNodeInfo.Geo)
			}
		}
	} else {
		if err.Error() != db.NotFind {
			log.Warnf("GetNodeInfo err:%v,deviceID:%v", err.Error(), deviceID)
		}
	}
	// log.Infof("oldgeo:%v,newgeo:%v,err:%v", nodeInfo.Geo, geoInfo.Geo, err)

	lastTime := time.Now().Format("2006-01-02 15:04:05")
	err = db.GetCacheDB().SetNodeInfo(deviceID, &db.NodeInfo{Geo: geoInfo.Geo, LastTime: lastTime, IsOnline: true, NodeType: typeName})
	if err != nil {
		return err
	}

	err = db.GetCacheDB().SetNodeToGeoList(deviceID, geoInfo.Geo)
	if err != nil {
		log.Errorf("SetNodeToGeoList err:%v,deviceID:%v,Geo:%v", err.Error(), deviceID, geoInfo.Geo)
		return err
	}

	err = db.GetCacheDB().SetGeoToList(geoInfo.Geo)
	if err != nil {
		log.Errorf("SetGeoToList err:%v,Geo:%v", err.Error(), geoInfo.Geo)
		return err
	}

	return nil
}

// node offline
func (n *Node) offline(deviceID string, geoInfo *region.GeoInfo, nodeType api.NodeTypeName, lastTime time.Time) {
	err := db.GetCacheDB().RemoveNodeWithGeoList(deviceID, geoInfo.Geo)
	if err != nil {
		log.Warnf("node offline RemoveNodeWithGeoList err : %v ,deviceID : %v", err.Error(), deviceID)
	}

	err = db.GetCacheDB().SetNodeInfo(deviceID, &db.NodeInfo{Geo: geoInfo.Geo, LastTime: lastTime.Format("2006-01-02 15:04:05"), IsOnline: false, NodeType: nodeType})
	if err != nil {
		log.Warnf("node offline SetNodeInfo err : %v ,deviceID : %v", err.Error(), deviceID)
	}
}

// get all cache fail cid
func (n *Node) getCacheFailCids() []string {
	deviceID := n.deviceInfo.DeviceId

	cids, err := db.GetCacheDB().GetBlocksWithNodeFailList(deviceID)
	if err != nil {
		return nil
	}

	return cids
}

// delete block records
func (n *Node) deleteBlockRecords(cids []string) (map[string]string, error) {
	deviceID := n.deviceInfo.DeviceId

	errList := make(map[string]string, 0)
	for _, cid := range cids {
		err := db.GetCacheDB().RemoveNodeWithCacheList(deviceID, cid)
		if err != nil {
			errList[cid] = err.Error()
			continue
		}

		_, err = db.GetCacheDB().GetCacheBlockInfo(deviceID, cid)
		if err != nil {
			if db.GetCacheDB().IsNilErr(err) {
				continue
			}
			errList[cid] = fmt.Sprintf("GetCacheBlockInfo err : %v", err.Error())
			continue
		}

		err = db.GetCacheDB().RemoveCacheBlockInfo(deviceID, cid)
		if err != nil {
			errList[cid] = fmt.Sprintf("RemoveCacheBlockInfo err : %v", err.Error())
			continue
		}

	}

	return errList, nil
}

// filter cached blocks and find download url from candidate
func (n *Node) getReqCacheDatas(scheduler *Scheduler, cids []string, isEdge bool) ([]api.ReqCacheData, []string) {
	geoInfo := &n.geoInfo
	reqList := make([]api.ReqCacheData, 0)

	if !isEdge {
		reqList = append(reqList, api.ReqCacheData{Cids: cids})
		return reqList, nil
	}

	notFindCandidateData := make([]string, 0)
	// if node is edge , find data with candidate
	csMap := make(map[string][]string)
	for _, cid := range cids {
		candidates, err := scheduler.nodeManager.getCandidateNodesWithData(cid, geoInfo)
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
		node := scheduler.nodeManager.getCandidateNode(deviceID)
		if node != nil {
			reqList = append(reqList, api.ReqCacheData{Cids: list, CandidateURL: node.addr})
		}
	}

	return reqList, notFindCandidateData
}

// cache block Result
// TODO save to sql
func (n *Node) cacheBlockResult(info *api.CacheResultInfo) error {
	deviceID := n.deviceInfo.DeviceId
	log.Infof("nodeCacheResult deviceID:%v,info:%v", deviceID, info)

	var err error
	defer func() {
		if err != nil {
			err = db.GetCacheDB().SetBlockToNodeFailList(deviceID, info.Cid)
			if err != nil {
				log.Warnf("nodeCacheResult SetBlockToNodeFailList err:%v,deviceID:%v,cid:%v", err.Error(), deviceID, info.Cid)
			}
		} else {
			err = db.GetCacheDB().RemoveBlockWithNodeFailList(deviceID, info.Cid)
			if err != nil {
				log.Warnf("nodeCacheResult SetBlockToNodeFailList err:%v,deviceID:%v,cid:%v", err.Error(), deviceID, info.Cid)
			}
		}
	}()

	if !info.IsOK {
		err = xerrors.New("not ok")
		return nil
	}

	err = db.GetCacheDB().SetCacheBlockInfo(deviceID, info.Cid)
	if err != nil {
		return err
	}

	err = db.GetCacheDB().SetNodeToCacheList(deviceID, info.Cid)
	if err != nil {
		return err
	}

	return nil
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
