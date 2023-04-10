package cache

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-libipfs/blocks"
	"github.com/ipld/go-car/v2/index"
	"github.com/linguohua/titan/api/types"
	"github.com/linguohua/titan/node/asset/fetcher"
	titanindex "github.com/linguohua/titan/node/asset/index"
	"github.com/linguohua/titan/node/asset/storage"
	"github.com/linguohua/titan/node/validate"
	"golang.org/x/xerrors"
)

const maxSizeOfCache = 1024

type CachedResulter interface {
	CacheResult(result *types.PullResult) error
}

type assetWaiter struct {
	Root  cid.Cid
	Dss   []*types.CandidateDownloadInfo
	cache *assetCache
}
type Manager struct {
	// root cid of asset
	waitList      []*assetWaiter
	waitListLock  *sync.Mutex
	downloadCh    chan bool
	downloadBatch int
	bFetcher      fetcher.BlockFetcher
	lru           *lruCache
	storage.Storage
}

type ManagerOptions struct {
	Storage       storage.Storage
	BFetcher      fetcher.BlockFetcher
	DownloadBatch int
}

func NewManager(opts *ManagerOptions) (*Manager, error) {
	lru, err := newLRUCache(opts.Storage, maxSizeOfCache)
	if err != nil {
		return nil, err
	}

	m := &Manager{
		waitList:      make([]*assetWaiter, 0),
		waitListLock:  &sync.Mutex{},
		downloadCh:    make(chan bool),
		Storage:       opts.Storage,
		bFetcher:      opts.BFetcher,
		lru:           lru,
		downloadBatch: opts.DownloadBatch,
	}

	m.restoreWaitListFromStore()

	go m.start()

	return m, nil
}

func (m *Manager) startTick() {
	for {
		time.Sleep(10 * time.Second)

		if len(m.waitList) > 0 {
			cache := m.CachingAsset()
			if cache != nil {
				if err := m.saveAssetCache(cache); err != nil {
					log.Error("saveAssetCache error:%s", err.Error())
				}

				log.Debugf("total block %d, done block %d, total size %d, done size %d",
					len(cache.blocksDownloadSuccessList)+len(cache.blocksWaitList),
					len(cache.blocksDownloadSuccessList),
					cache.totalSize,
					cache.totalSize)
			}
		}

	}
}

func (m *Manager) triggerDownload() {
	select {
	case m.downloadCh <- true:
	default:
	}
}

func (m *Manager) start() {
	if m.bFetcher == nil {
		log.Panic("m.bFetcher == nil")
	}

	go m.startTick()

	// delay 15 second to do download asset if exist waitList
	time.AfterFunc(15*time.Second, m.triggerDownload)

	for {
		<-m.downloadCh
		m.doDownloadAssets()
	}
}

func (m *Manager) doDownloadAssets() {
	for len(m.waitList) > 0 {
		m.doDownloadAsset()
	}
}

func (m *Manager) doDownloadAsset() {
	cw := m.headFromWaitList()
	if cw == nil {
		return
	}
	defer m.removeAssetFromWaitList(cw.Root)

	assetCache, err := m.restoreAssetCacheOrNew(&options{cw.Root, cw.Dss, m.Storage, m.bFetcher, m.downloadBatch})
	if err != nil {
		log.Errorf("restore asset cache error:%s", err)
		return
	}

	cw.cache = assetCache
	err = assetCache.downloadAsset()
	if err != nil {
		log.Errorf("doDownloadAsset, download asset error:%s", err)
	}

	m.ondownloadAssetFinish(assetCache)
}

func (m *Manager) headFromWaitList() *assetWaiter {
	m.waitListLock.Lock()
	defer m.waitListLock.Unlock()

	if len(m.waitList) == 0 {
		return nil
	}
	return m.waitList[0]
}

func (m *Manager) removeAssetFromWaitList(root cid.Cid) *assetWaiter {
	m.waitListLock.Lock()
	defer m.waitListLock.Unlock()

	if len(m.waitList) == 0 {
		return nil
	}

	for i, cw := range m.waitList {
		if cw.Root.Hash().String() == root.Hash().String() {
			m.waitList = append(m.waitList[:i], m.waitList[i+1:]...)

			if err := m.saveWaitList(); err != nil {
				log.Errorf("save wait list error: %s", err.Error())
			}
			return cw
		}
	}

	return nil
}

func (m *Manager) AddToWaitList(root cid.Cid, dss []*types.CandidateDownloadInfo) {
	m.waitListLock.Lock()
	defer m.waitListLock.Unlock()

	for _, waiter := range m.waitList {
		if waiter.Root.Hash().String() == root.Hash().String() {
			return
		}
	}

	cw := &assetWaiter{Root: root, Dss: dss}
	m.waitList = append(m.waitList, cw)

	if err := m.saveWaitList(); err != nil {
		log.Errorf("save wait list error: %s", err.Error())
	}

	m.triggerDownload()
}

func (m *Manager) saveAssetCache(cf *assetCache) error {
	if cf == nil || cf.isDownloadComplete() {
		return nil
	}

	buf, err := cf.encode()
	if err != nil {
		return err
	}
	return m.PutAssetCache(cf.Root(), buf)
}

func (m *Manager) ondownloadAssetFinish(cf *assetCache) {
	log.Debugf("ondownloadAssetFinish, asset %s", cf.root.String())
	if cf.isDownloadComplete() {
		if err := m.RemoveAssetCache(cf.root); err != nil && !os.IsNotExist(err) {
			log.Errorf("remove asset cache error:%s", err.Error())
		}

		blockCountOfAsset := uint32(len(cf.blocksDownloadSuccessList))
		if err := m.SetBlockCountOfAsset(context.Background(), cf.root, blockCountOfAsset); err != nil {
			log.Errorf("set block count error:%s", err.Error())
		}

		if err := m.PutAsset(context.Background(), cf.root); err != nil {
			log.Errorf("put asset error: %s", err.Error())
		}

	} else {
		if err := m.saveAssetCache(cf); err != nil {
			log.Errorf("saveAssetCache error:%s", err.Error())
		}
	}
}

func (m *Manager) saveWaitList() error {
	data, err := encode(&m.waitList)
	if err != nil {
		return err
	}

	return m.PutWaitList(data)
}

func (m *Manager) restoreWaitListFromStore() {
	data, err := m.GetWaitList()
	if err != nil {
		if err != datastore.ErrNotFound {
			log.Errorf("restoreWaitListFromStore error:%s", err)
		}
		return
	}

	if len(data) == 0 {
		return
	}

	err = decode(data, &m.waitList)
	if err != nil {
		log.Errorf("restoreWaitListFromStore error:%s", err)
		return
	}

	log.Debugf("restoreWaitListFromStore:%#v", m.waitList)
}

func (m *Manager) WaitListLen() int {
	return len(m.waitList)
}

func (m *Manager) CachingAsset() *assetCache {
	for _, cw := range m.waitList {
		if cw.cache != nil {
			return cw.cache
		}
	}
	return nil
}

func (m *Manager) DeleteAsset(root cid.Cid) error {
	// remove lru cache
	m.lru.remove(root)

	ok, err := m.deleteAssetFromWaitList(root)
	if err != nil {
		return err
	}

	if ok {
		return nil
	}

	if err := m.RemoveAsset(root); err != nil {
		return err
	}

	return nil
}

func (m *Manager) restoreAssetCacheOrNew(opts *options) (*assetCache, error) {
	data, err := m.GetAssetCache(opts.root)
	if err != nil && !os.IsNotExist(err) {
		log.Errorf("load asset cache error %s", err.Error())
		return nil, err
	}

	cc := newAssetCache(opts)
	if len(data) > 0 {
		err = cc.decode(data)
		if err != nil {
			return nil, err
		}

		// cover new download sources
		if len(opts.dss) > 0 {
			cc.downloadSources = opts.dss
		}
	}
	return cc, nil
}

// return true if exist in waitList
func (m *Manager) deleteAssetFromWaitList(root cid.Cid) (bool, error) {
	if c := m.removeAssetFromWaitList(root); c != nil {
		if c.cache != nil {
			err := c.cache.CancelDownload()
			if err != nil {
				return false, err
			}
		}

		return true, nil
	}
	return false, nil
}

func (m *Manager) Progresses() []*types.AssetPullProgress {
	progresses := make([]*types.AssetPullProgress, 0, len(m.waitList))

	for _, cw := range m.waitList {
		progress := &types.AssetPullProgress{CID: cw.Root.String(), Status: types.ReplicaStatusWaiting}
		if cw.cache != nil {
			progress = cw.cache.Progress()
		}

		progresses = append(progresses, progress)
	}

	return progresses
}

func (m *Manager) CachedStatus(root cid.Cid) (types.ReplicaStatus, error) {
	if ok, err := m.HasAsset(root); err == nil && ok {
		return types.ReplicaStatusSucceeded, nil
	}

	for _, cw := range m.waitList {
		if cw.Root.Hash().String() == root.Hash().String() {
			if cw.cache != nil {
				return types.ReplicaStatusPulling, nil
			}
			return types.ReplicaStatusWaiting, nil
		}
	}

	return types.ReplicaStatusFailed, nil
}

func (m *Manager) ProgressForFailedAsset(root cid.Cid) (*types.AssetPullProgress, error) {
	progress := &types.AssetPullProgress{
		CID:    root.String(),
		Status: types.ReplicaStatusFailed,
	}

	data, err := m.GetAssetCache(root)
	if os.IsNotExist(err) {
		return progress, nil
	}

	if err != nil {
		return nil, err
	}

	cc := &assetCache{}
	err = cc.decode(data)
	if err != nil {
		return nil, err
	}

	progress.BlocksCount = len(cc.blocksDownloadSuccessList) + len(cc.blocksWaitList)
	progress.DoneBlocksCount = len(cc.blocksDownloadSuccessList)
	progress.Size = cc.TotalSize()
	progress.DoneSize = cc.DoneSize()

	return progress, nil
}

func (m *Manager) GetBlock(ctx context.Context, root, block cid.Cid) (blocks.Block, error) {
	return m.lru.getBlock(ctx, root, block)
}

func (m *Manager) HasBlock(ctx context.Context, root, block cid.Cid) (bool, error) {
	return m.lru.hasBlock(ctx, root, block)
}

func (m *Manager) iterableIndex(ctx context.Context, root cid.Cid) (index.IterableIndex, error) {
	idx, err := m.lru.assetIndex(root)
	if err != nil {
		return nil, err
	}

	iterableIdx, ok := idx.(index.IterableIndex)
	if !ok {
		return nil, xerrors.Errorf("idx is not IterableIndex")
	}

	return iterableIdx, nil
}

func (m *Manager) GetBlocksOfAsset(root cid.Cid, randomSeed int64, randomCount int) (map[int]string, error) {
	rand := rand.New(rand.NewSource(randomSeed))

	idx, err := m.lru.assetIndex(root)
	if err != nil {
		return nil, err
	}

	multiIndex, ok := idx.(*titanindex.MultiIndexSorted)
	if !ok {
		return nil, xerrors.Errorf("idx is not MultiIndexSorted")
	}

	sizeOfBuckets := multiIndex.BucketSize()
	ret := make(map[int]string, 0)

	for i := 0; i < randomCount; i++ {
		index := rand.Intn(int(sizeOfBuckets))
		records, err := multiIndex.GetBucket(uint32(index))
		if err != nil {
			return nil, err
		}

		if len(records) == 0 {
			return nil, xerrors.Errorf("record is empty")
		}

		index = rand.Intn(len(records))
		record := records[index]
		ret[i] = record.Cid.String()
	}

	return ret, nil
}

// AddLostAsset implement data sync interface
func (m *Manager) AddLostAsset(root cid.Cid) error {
	if has, err := m.HasAsset(root); err != nil {
		return err
	} else if has {
		return nil
	}

	switch types.RunningNodeType {
	case types.NodeCandidate:
		m.AddToWaitList(root, nil)
	case types.NodeEdge:
		return fmt.Errorf("not implement")
	default:
		return fmt.Errorf("not support node type:%s", types.RunningNodeType)
	}

	return nil
}

// GetAssetsOfBucket data sync interface
func (m *Manager) GetAssetsOfBucket(ctx context.Context, bucketID uint32, isRemote bool) ([]cid.Cid, error) {
	if !isRemote {
		return m.Storage.GetAssetsOfBucket(ctx, bucketID)
	}
	return nil, nil
}

// GetChecker validate asset by random seed
func (m *Manager) GetChecker(ctx context.Context, randomSeed int64) (validate.RandomChecker, error) {
	return NewRandomCheck(randomSeed, m.Storage, nil), nil
}
