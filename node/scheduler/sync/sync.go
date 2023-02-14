package sync

import (
	"sync"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/node/scheduler/node"
)

var log = logging.Logger("web")

const (
	maxGroupNum        = 10
	maxNumOfScrubBlock = 1000
)

type DataSync struct {
	maxGroupNum        int
	maxNumOfScrubBlock int
	nodeList           []string
	lock               *sync.Mutex
	waitChannel        chan bool
	nodeManager        *node.Manager
}

// type node struct {
// 	nodeID string
// }

type blockItem struct {
	fid int
	cid string
}

type inconformityBlocks struct {
	blocks []*blockItem
	// startFid is not same as block first item
	startFid int
	// endFid is not same as block last item
	endFid int
}

//maxGroupNum: max group for device sync data
//maxNumOfScrubBlock: max number of block on scrub block
func NewDataSync(nodeManager *node.Manager) *DataSync {
	dataSync := &DataSync{
		maxGroupNum:        maxGroupNum,
		maxNumOfScrubBlock: maxNumOfScrubBlock,
		nodeList:           make([]string, 0),
		lock:               &sync.Mutex{},
		waitChannel:        make(chan bool),
		nodeManager:        nodeManager,
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
	// TODO: do sync data
}
