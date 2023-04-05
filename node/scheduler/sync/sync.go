package sync

import (
	"context"
	"sync"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/scheduler/node"
	"golang.org/x/xerrors"
)

var log = logging.Logger("data-sync")

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
		err := ds.doDataSync(nodeID)
		if err != nil {
			log.Errorf("do data sync error:%s", err.Error())
		}
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
		return edgeNode.API
	}
	if candidateNode := ds.nodeManager.GetCandidateNode(nodeID); candidateNode != nil {
		return candidateNode.API
	}
	return nil
}

func (ds *DataSync) doDataSync(nodeID string) error {
	dataSyncAPI := ds.getNodeDataSyncAPI(nodeID)
	if dataSyncAPI == nil {
		return xerrors.Errorf("could not get node %s data sync api", nodeID)
	}
	topChecksum, err := ds.getTopHash(nodeID)
	if err != nil {
		return xerrors.Errorf("get top hash %w", err)
	}

	if len(topChecksum) == 0 {
		log.Warnf("node %s no assets exist", nodeID)
		return nil
	}

	ctx, cancle := context.WithCancel(context.Background())
	defer cancle()

	if ok, err := dataSyncAPI.CompareTopHash(ctx, topChecksum); err != nil {
		return err
	} else if ok {
		return xerrors.Errorf("compare top hash %w", err)
	}

	checksums, err := ds.getHashesOfBuckets(nodeID)
	if err != nil {
		return xerrors.Errorf("get hashes of buckets %w", err)
	}

	mismatchBuckets, err := dataSyncAPI.CompareBucketHashes(ctx, checksums)
	if err != nil {
		return xerrors.Errorf("compare bucket hashes %w", err)
	}

	log.Warnf("mismatch buckets len:%d", len(mismatchBuckets))
	return nil
}

func (ds *DataSync) getTopHash(nodeID string) (string, error) {
	return ds.nodeManager.LoadTopHash(nodeID)
}

func (ds *DataSync) getHashesOfBuckets(nodeID string) (map[uint32]string, error) {
	return ds.nodeManager.LoadBucketHashes(nodeID)
}
