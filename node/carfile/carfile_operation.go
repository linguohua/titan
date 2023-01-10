package carfile

import (
	"context"
	"sync"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	legacy "github.com/ipfs/go-ipld-legacy"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipfs/go-merkledag"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/blockstore"
	"github.com/linguohua/titan/node/device"
)

var log = logging.Logger("carfile")

type CarfileOperation struct {
	ds         datastore.Batching
	blockStore blockstore.BlockStore
	scheduler  api.Scheduler
	// carfile list
	carfileWaitList []*carfile
	waitListLock    *sync.Mutex
	// key is carfile hash
	// carfileMap    map[string]*list.Element
	// cachingList   []*delayReq
	// saveBlockLock *sync.Mutex
	downloader BlockDownloader
	// device        *device.Device
	// exchange      exchange.Interface
	carfileCacheCh chan bool
	// ipfsApi       *ipfsApi.HttpApi
	// ipfsGateway   string
}

type BlockDownloader interface {
	// scheduler request cache carfile
	downloadBlocks(cids []string, sources []*api.DowloadSource) ([]blocks.Block, error)
	// // local sync miss data
	// syncData(block *Block, reqs map[int]string) error
}

func NewCarfileOperation(ds datastore.Batching, blockStore blockstore.BlockStore, scheduler api.Scheduler, blockDownloader BlockDownloader, device *device.Device) *CarfileOperation {
	carfileOperation := &CarfileOperation{
		ds:              ds,
		blockStore:      blockStore,
		scheduler:       scheduler,
		downloader:      blockDownloader,
		carfileWaitList: make([]*carfile, 0),
		waitListLock:    &sync.Mutex{},
	}

	go carfileOperation.startCarfileDownloader()

	legacy.RegisterCodec(cid.DagProtobuf, dagpb.Type.PBNode, merkledag.ProtoNodeConverter)
	legacy.RegisterCodec(cid.Raw, basicnode.Prototype.Bytes, merkledag.RawNodeConverter)

	return carfileOperation
}

func (carfileOperation *CarfileOperation) startCarfileDownloader() {
	if carfileOperation.downloader == nil {
		log.Panic("block.block == nil")
	}

	for {
		<-carfileOperation.carfileCacheCh
		carfileOperation.doDownloadCarfile()
	}
}

func (carfileOperation *CarfileOperation) notifyCarfileDownloader() {
	select {
	case carfileOperation.carfileCacheCh <- true:
	default:
	}
}

func (carfileOperation *CarfileOperation) addCarfile2WaitList(carfile *carfile) {
	carfileOperation.waitListLock.Lock()
	defer carfileOperation.waitListLock.Unlock()

	for _, carfile := range carfileOperation.carfileWaitList {
		if carfile.carfileCID == carfile.carfileCID {
			return
		}
	}

	carfileOperation.carfileWaitList = append(carfileOperation.carfileWaitList, carfile)
}

func (carfileOperation *CarfileOperation) getFirstCarfileFromWaitList() *carfile {
	carfileOperation.waitListLock.Lock()
	defer carfileOperation.waitListLock.Unlock()

	if len(carfileOperation.carfileWaitList) == 0 {
		return nil
	}

	return carfileOperation.carfileWaitList[0]
}

func (carfileOperation *CarfileOperation) removeFirstCarfileFromWaitList() *carfile {
	carfileOperation.waitListLock.Lock()
	defer carfileOperation.waitListLock.Unlock()

	if len(carfileOperation.carfileWaitList) == 0 {
		return nil
	}

	carfile := carfileOperation.carfileWaitList[0]
	carfileOperation.carfileWaitList = carfileOperation.carfileWaitList[1:]

	return carfile
}

func (carfileOperation *CarfileOperation) doDownloadCarfile() {
	for len(carfileOperation.carfileWaitList) > 0 {
		carfile := carfileOperation.getFirstCarfileFromWaitList()
		err := carfile.downloadCarfile(carfileOperation)
		if err != nil {
			log.Errorf("downloadCarfile error:%s", err)
		}

		// TODO: Result to scheduler
		// TODO: saveRecord to local

		carfileOperation.removeFirstCarfileFromWaitList()
	}
}

func (carfileOperation *CarfileOperation) CacheCarfile(ctx context.Context, carfileCID string, sources []*api.DowloadSource) (api.CacheCarfileResult, error) {
	carfile := &carfile{
		carfileCID:                carfileCID,
		blocksWaitList:            []string{carfileCID},
		blocksDownloadSuccessList: make([]string, 0),
		blocksDownloadFailedList:  make([]string, 0),
		downloadSources:           sources,
		// waitListLock:              &sync.Mutex{},
	}

	carfileOperation.addCarfile2WaitList(carfile)
	carfileOperation.notifyCarfileDownloader()

	return api.CacheCarfileResult{}, nil
}

func (carfileOperation *CarfileOperation) DeleteCarfile(ctx context.Context, carfileCID string) error {
	return nil

}
func (carfileOperation *CarfileOperation) DeleteAllCarfiles(ctx context.Context) error {
	return nil
}
func (carfileOperation *CarfileOperation) DeleteWaitCacheCarfile(ctx context.Context, carfileCID string) error {
	return nil

}
