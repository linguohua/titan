package sync

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"hash/fnv"
	"sync"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/types"
	"github.com/linguohua/titan/node/scheduler/node"
	mh "github.com/multiformats/go-multihash"
)

var log = logging.Logger("data-sync")

const (
	bucketCount = 100
	dbLoadCount = 500
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
		log.Errorf("load carfile infos error:%s", err.Error())
		return
	}

	multihashes := ds.multihashSort(nodeCacheStatusList)

	checksums, err := ds.calculateChecksums(multihashes)
	if err != nil {
		log.Errorf("calculate checksums error:%s", err.Error())
		return
	}

	keys, err := dataSyncAPI.CompareChecksums(context.Background(), bucketCount, checksums)
	if err != nil {
		log.Errorf("compare checksums error:%s", err.Error())
		return
	}

	// TODO: merge multi key to compare together
	for _, key := range keys {
		err := dataSyncAPI.CompareCarfiles(context.Background(), bucketCount, map[uint32][]string{key: multihashes[key]})
		if err != nil {
			log.Errorf("compare carfiles error:%s", err.Error())
		}
	}
}

func (ds *DataSync) multihashSort(statuses []*types.NodeReplicaStatus) map[uint32][]string {
	multihashes := make(map[uint32][]string)
	// append carfile hash by hash code
	for _, status := range statuses {
		multihash, err := mh.FromHexString(status.Hash)
		if err != nil {
			log.Errorf("decode multihash error:%s", err.Error())
			continue
		}

		h := fnv.New32a()
		h.Write(multihash)
		k := h.Sum32() % bucketCount

		multihashes[k] = append(multihashes[k], status.Hash)
	}

	return multihashes
}

func (ds *DataSync) calculateChecksums(multihashes map[uint32][]string) (map[uint32]string, error) {
	checksums := make(map[uint32]string)
	for k, v := range multihashes {
		checksum, err := ds.calculateChecksum(v)
		if err != nil {
			return nil, err
		}

		checksums[k] = checksum
	}
	return checksums, nil
}

func (ds *DataSync) calculateChecksum(carfileHashes []string) (string, error) {
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

func (ds *DataSync) loadCarfileInfosBy(nodeID string) ([]*types.NodeReplicaStatus, error) {
	index := 0
	cacheStates := make([]*types.NodeReplicaStatus, 0)
	for {
		nodeCacheRsp, err := ds.nodeManager.LoadReplicaInfosOfNode(nodeID, index, dbLoadCount)
		if err != nil {
			log.Errorf("GetCacheInfosWithNode %s, index:%d, count:%d, error:%s", nodeID, index, dbLoadCount)
			return nil, err
		}

		cacheStates = append(cacheStates, nodeCacheRsp.Replica...)
		if len(cacheStates) == nodeCacheRsp.TotalCount {
			return cacheStates, nil
		}

		index = len(cacheStates)
	}
}
