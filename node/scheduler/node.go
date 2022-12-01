package scheduler

import (
	"context"
	"crypto/rsa"
	"time"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"github.com/linguohua/titan/region"

	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/go-jsonrpc/auth"
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
	scheduler *Scheduler

	deviceInfo      api.DevicesInfo
	geoInfo         *region.GeoInfo
	addr            string
	lastRequestTime time.Time

	cacheStat                 api.CacheStat
	cacheTimeoutTimeStamp     int64 // TimeStamp of cache timeout
	cacheNextTimeoutTimeStamp int64 // TimeStamp of next cache timeout
	privateKey                *rsa.PrivateKey
	downloadSrvURL            string
}

// node online
func (n *Node) setNodeOnline(typeName api.NodeTypeName) error {
	deviceID := n.deviceInfo.DeviceId
	geoInfo := n.geoInfo

	lastTime := time.Now().Format("2006-01-02 15:04:05")
	err := persistent.GetDB().SetNodeInfo(deviceID, &persistent.NodeInfo{
		Geo:        geoInfo.Geo,
		LastTime:   lastTime,
		IsOnline:   1,
		NodeType:   string(typeName),
		Address:    n.addr,
		PrivateKey: privateKey2Pem(n.privateKey),
		URL:        n.downloadSrvURL,
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
func (n *Node) findDownloadinfoForBlocks(blocks []api.BlockInfo, carfileHash, cacheID string) []api.ReqCacheData {
	reqList := make([]api.ReqCacheData, 0)
	notFindCandidateBlocks := make([]api.BlockInfo, 0)

	csMap := make(map[string][]api.BlockInfo)
	for _, block := range blocks {
		deviceID := block.From

		list, ok := csMap[deviceID]
		if !ok {
			list = make([]api.BlockInfo, 0)
		}
		list = append(list, block)

		csMap[deviceID] = list
	}

	tk, err := n.scheduler.AuthNew(context.Background(), []auth.Permission{api.PermRead, api.PermWrite})
	if err != nil {
		log.Errorf("findDownloadinfoForBlocks AuthNew err:%s", err.Error())
		return reqList
	}

	for deviceID, list := range csMap {
		// info, err := persistent.GetDB().GetNodeAuthInfo(deviceID)

		node := n.scheduler.nodeManager.getCandidateNode(deviceID)
		if node != nil {
			reqList = append(reqList, api.ReqCacheData{BlockInfos: list, DownloadURL: node.addr, DownloadToken: string(tk), CardFileHash: carfileHash, CacheID: cacheID})

			// tk, err := token.GenerateToken(info.PrivateKey, time.Now().Add(helper.DownloadTokenExpireAfter).Unix())
			// if err == nil {
			// 	reqList = append(reqList, api.ReqCacheData{BlockInfos: list, DownloadURL: info.URL, DownloadToken: tk, CardFileHash: carfileHash, CacheID: cacheID})

			// 	continue
			// }
		}

		notFindCandidateBlocks = append(notFindCandidateBlocks, list...)
	}

	if len(notFindCandidateBlocks) > 0 {
		reqList = append(reqList, api.ReqCacheData{BlockInfos: notFindCandidateBlocks, CardFileHash: carfileHash, CacheID: cacheID})
	}

	return reqList
}

func (n *Node) updateAccessAuth(access *api.DownloadServerAccessAuth) error {
	return persistent.GetDB().SetNodeAuthInfo(access)
}

func (n *Node) updateCacheStat(info api.CacheStat) {
	n.cacheStat = info

	num := info.WaitCacheBlockNum + info.DoingCacheBlockNum

	timeStamp := time.Now().Unix()
	n.cacheTimeoutTimeStamp = timeStamp + int64(num*info.DownloadTimeout*info.RetryNum)

	n.cacheNextTimeoutTimeStamp = n.cacheTimeoutTimeStamp + int64(info.DownloadTimeout*info.RetryNum)
}
