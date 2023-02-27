package sync

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"sync"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"github.com/linguohua/titan/node/scheduler/node"
)

var log = logging.Logger("data-sync")

const checkGroupNumber = 1000

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

func (ds *DataSync) doDataSync(nodeID string) {
	nodeCacheStatusList, err := ds.loadCarfileInfosByNode(nodeID)
	if err != nil {
		log.Errorf("doDataSync, loadCarfileInfosByNode error:%s", err.Error())
		return
	}

	// sort the carfile hash, that match to the node file system
	sort.Slice(nodeCacheStatusList, func(i, j int) bool {
		return nodeCacheStatusList[i].CarfileHash < nodeCacheStatusList[j].CarfileHash
	})

	// split carfile to succeeded and unsucceeded
	succeededCarfileList, unsucceededCarfileList := ds.splitCacheStatusListByStatus(nodeCacheStatusList)
	checkSummaryResult, err := ds.checkSummary(nodeID, succeededCarfileList, unsucceededCarfileList)
	if err != nil {
		log.Errorf("doDataSync,checkSummary error:%s", err.Error())
		return
	}

	if !checkSummaryResult.IsSusseedCarfilesOk {
		err = ds.checkCarfiles(nodeID, succeededCarfileList, true)
		if err != nil {
			log.Errorf("check succeed carfile error:%s", err)
		}
	}

	if !checkSummaryResult.IsUnsusseedCarfilesOk {
		err = ds.checkCarfiles(nodeID, unsucceededCarfileList, false)
		if err != nil {
			log.Errorf("check unsucceed carfile error:%s", err)
		}
	}
}

// quickly check device's carfiles if same as scheduler
func (ds *DataSync) checkSummary(nodeID string, succeedCarfileList, failedCarfileList []string) (*api.CheckSummaryResult, error) {
	var mergeSucceedCarfileHash string
	var mergeFailedCarfileHash string

	for _, succeedCarfileHash := range succeedCarfileList {
		mergeSucceedCarfileHash += succeedCarfileHash
	}

	for _, failedCarfileHash := range failedCarfileList {
		mergeFailedCarfileHash += failedCarfileHash
	}

	hash := sha256.New()
	hash.Write([]byte(mergeSucceedCarfileHash))
	succeedCarfilesHash := hex.EncodeToString(hash.Sum(nil))

	hash.Reset()
	hash.Write([]byte(mergeFailedCarfileHash))
	failedCarfilesHash := hex.EncodeToString(hash.Sum(nil))

	if edgeNode := ds.nodeManager.GetEdgeNode(nodeID); edgeNode != nil {
		return edgeNode.GetAPI().CheckSummary(succeedCarfilesHash, failedCarfilesHash)
	}

	if candidateNode := ds.nodeManager.GetCandidateNode(nodeID); candidateNode != nil {
		return candidateNode.GetAPI().CheckSummary(succeedCarfilesHash, failedCarfilesHash)
	}

	return nil, fmt.Errorf("Node %s not online", nodeID)
}

func (ds *DataSync) checkCarfiles(nodeID string, carfileList []string, isSucceedCarfileCheck bool) error {
	// TODO: implemnet check carfiles

	return nil
}

func (ds *DataSync) splitCacheStatusListByStatus(cacheStatusList []*api.NodeCacheStatus) (succeededCarfileList, unsucceededCarfileList []string) {
	succeededCarfileList = make([]string, 0)
	unsucceededCarfileList = make([]string, 0)

	for _, cacheStatus := range cacheStatusList {
		if cacheStatus.Status == api.CacheStatusSucceeded {
			succeededCarfileList = append(succeededCarfileList, cacheStatus.CarfileHash)
		} else if cacheStatus.Status == api.CacheStatusFailed {
			unsucceededCarfileList = append(unsucceededCarfileList, cacheStatus.CarfileHash)
		}
	}

	return succeededCarfileList, unsucceededCarfileList
}

func (ds *DataSync) loadCarfileInfosByNode(nodeID string) ([]*api.NodeCacheStatus, error) {
	index := 0
	count := 500
	cacheStates := make([]*api.NodeCacheStatus, 0)
	for {
		nodeCacheRsp, err := persistent.GetCacheInfosWithNode(nodeID, index, count)
		if err != nil {
			log.Errorf("GetCacheInfosWithNode %s, index:%d, count:%d, error:%s", nodeID, index, count)
			return nil, err
		}

		cacheStates = append(cacheStates, nodeCacheRsp.Caches...)
		if len(cacheStates) == nodeCacheRsp.TotalCount {
			return cacheStates, nil
		}

		index = len(cacheStates)
	}
}
