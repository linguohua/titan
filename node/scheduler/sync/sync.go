package sync

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"sync"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/types"
	"github.com/linguohua/titan/node/scheduler/node"
)

var log = logging.Logger("data-sync")

const (
	groupCheckCount = 1000
	dbLoadCount     = 500
)

type DataSync struct {
	nodeList    []string
	lock        *sync.Mutex
	waitChannel chan bool
	nodeManager *node.Manager
}

func NewDataSync(nodeManager *node.Manager) *DataSync {
	dataSync := &DataSync{
		nodeList:    make([]string, 0),
		lock:        &sync.Mutex{},
		waitChannel: make(chan bool),
		nodeManager: nodeManager,
	}

	go dataSync.run()

	return dataSync
}

func (ds *DataSync) Add2List(nodeID string) {
	ds.lock.Lock()
	defer ds.lock.Unlock()

	for _, id := range ds.nodeList {
		if id == nodeID {
			return
		}
	}

	ds.nodeList = append(ds.nodeList, nodeID)

	ds.notifyRunner()
}

func (ds *DataSync) run() {
	for {
		<-ds.waitChannel
		ds.syncData()
	}
}

func (ds *DataSync) syncData() {
	for len(ds.nodeList) > 0 {
		nodeID := ds.removeFirstNode()
		ds.doDataSync(nodeID)
	}
}

func (ds *DataSync) notifyRunner() {
	select {
	case ds.waitChannel <- true:
	default:
	}
}

func (ds *DataSync) removeFirstNode() string {
	ds.lock.Lock()
	defer ds.lock.Unlock()

	if len(ds.nodeList) == 0 {
		return ""
	}

	nodeID := ds.nodeList[0]
	ds.nodeList = ds.nodeList[1:]
	return nodeID
}

func (ds *DataSync) getNodeDataSyncAPI(nodeID string) api.DataSync {
	if edgeNode := ds.nodeManager.GetEdgeNode(nodeID); edgeNode != nil {
		return edgeNode.API()
	}
	if candidateNode := ds.nodeManager.GetCandidateNode(nodeID); candidateNode != nil {
		return candidateNode.API()
	}
	return nil
}

func (ds *DataSync) doDataSync(nodeID string) {
	dataSyncAPI := ds.getNodeDataSyncAPI(nodeID)
	if dataSyncAPI == nil {
		return
	}

	nodeCacheStatusList, err := ds.loadCarfileInfosBy(nodeID)
	if err != nil {
		log.Errorf("doDataSync, loadCarfileInfosByNode error:%s", err.Error())
		return
	}

	// sort the storage hash, that match to the node file system
	sort.Slice(nodeCacheStatusList, func(i, j int) bool {
		return nodeCacheStatusList[i].CarfileHash < nodeCacheStatusList[j].CarfileHash
	})

	// split storage to succeeded and unsucceeded
	succeededCarfiles, unsucceededCarfiles := ds.splitCacheStatusListByStatus(nodeCacheStatusList)

	succeededChecksum, err := ds.caculateChecksum(succeededCarfiles)
	if err != nil {
		log.Errorf("doDataSync, caculate succeededCarfiles checksum error:%s", err.Error())
		return
	}

	unsucceededChecksum, err := ds.caculateChecksum(unsucceededCarfiles)
	if err != nil {
		log.Errorf("doDataSync, caculate unsucceededCarfiles checksum error:%s", err.Error())
		return
	}

	checkSummaryResult, err := dataSyncAPI.CompareChecksum(context.Background(), succeededChecksum, unsucceededChecksum)
	if err != nil {
		log.Errorf("doDataSync,checkSummary error:%s", err.Error())
		return
	}

	if !checkSummaryResult.IsSusseedCarfilesOk {
		err = ds.checkCarfiles(dataSyncAPI, succeededCarfiles, succeededChecksum, true)
		if err != nil {
			log.Errorf("check succeed storage error:%s", err)
		}
	}

	if !checkSummaryResult.IsUnsusseedCarfilesOk {
		err = ds.checkCarfiles(dataSyncAPI, unsucceededCarfiles, unsucceededChecksum, false)
		if err != nil {
			log.Errorf("check unsucceed storage error:%s", err)
		}
	}
}

func (ds *DataSync) caculateChecksum(carfileHashes []string) (string, error) {
	hash := sha256.New()

	for _, h := range carfileHashes {
		data := []byte(h)
		_, err := hash.Write(data)
		if err != nil {
			return "", err
		}
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func (ds *DataSync) checkCarfiles(dataSyncAPI api.DataSync, carfileHashes []string, checksum string, isSucceedCarfileCheck bool) error {
	err := dataSyncAPI.BeginCheckCarfiles(context.Background())
	if err != nil {
		return err
	}

	for i := 0; i < len(carfileHashes); {
		start := i
		i = i + groupCheckCount
		end := i

		if end > len(carfileHashes) {
			end = len(carfileHashes)
		}

		err = dataSyncAPI.PrepareCarfiles(context.Background(), carfileHashes[start:end])
		if err != nil {
			return err
		}
	}

	return dataSyncAPI.DoCheckCarfiles(context.Background(), checksum, isSucceedCarfileCheck)
}

func (ds *DataSync) splitCacheStatusListByStatus(cacheStatusList []*types.NodeCacheStatus) (succeededCarfiles, unsucceededCarfiles []string) {
	succeededCarfiles = make([]string, 0)
	unsucceededCarfiles = make([]string, 0)

	for _, cacheStatus := range cacheStatusList {
		if cacheStatus.Status == types.CacheStatusSucceeded {
			succeededCarfiles = append(succeededCarfiles, cacheStatus.CarfileHash)
		} else if cacheStatus.Status == types.CacheStatusFailed {
			unsucceededCarfiles = append(unsucceededCarfiles, cacheStatus.CarfileHash)
		}
	}

	return succeededCarfiles, unsucceededCarfiles
}

func (ds *DataSync) loadCarfileInfosBy(nodeID string) ([]*types.NodeCacheStatus, error) {
	index := 0
	cacheStates := make([]*types.NodeCacheStatus, 0)
	for {
		nodeCacheRsp, err := ds.nodeManager.CarfileDB.GetCacheInfosWithNode(nodeID, index, dbLoadCount)
		if err != nil {
			log.Errorf("GetCacheInfosWithNode %s, index:%d, count:%d, error:%s", nodeID, index, dbLoadCount)
			return nil, err
		}

		cacheStates = append(cacheStates, nodeCacheRsp.Caches...)
		if len(cacheStates) == nodeCacheRsp.TotalCount {
			return cacheStates, nil
		}

		index = len(cacheStates)
	}
}
