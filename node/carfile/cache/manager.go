package cache

import (
	"os"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/linguohua/titan/api/types"
	"github.com/linguohua/titan/node/carfile/fetcher"
	carfilestore "github.com/linguohua/titan/node/carfile/store"
)

type CachedResulter interface {
	CacheResult(result *types.CacheResult) error
}

type carWaiter struct {
	Root  cid.Cid
	Dss   []*types.DownloadSource
	cache *carfileCache
}
type Manager struct {
	// root cid of car
	waitList      []*carWaiter
	waitListLock  *sync.Mutex
	downloadCh    chan bool
	carfileStore  *carfilestore.CarfileStore
	bFetcher      fetcher.BlockFetcher
	downloadBatch int
}

type ManagerOptions struct {
	CarfileStore  *carfilestore.CarfileStore
	BFetcher      fetcher.BlockFetcher
	DownloadBatch int
}

func NewManager(opts *ManagerOptions) *Manager {
	m := &Manager{
		waitList:      make([]*carWaiter, 0),
		waitListLock:  &sync.Mutex{},
		downloadCh:    make(chan bool),
		carfileStore:  opts.CarfileStore,
		bFetcher:      opts.BFetcher,
		downloadBatch: opts.DownloadBatch,
	}

	m.restoreWaitListFromStore()

	go m.start()

	return m
}

func (m *Manager) startTick() {
	for {
		time.Sleep(10 * time.Second)

		if len(m.waitList) > 0 {
			cache := m.CachingCar()
			if cache != nil {
				if err := m.saveIncompleteCarfileCache(cache); err != nil {
					log.Error("saveIncompleteCarfileCache error:%s", err.Error())
				}
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

	// delay 15 second to do download cars if exist waitList
	time.AfterFunc(15*time.Second, m.triggerDownload)

	for {
		<-m.downloadCh
		m.doDownloadCars()
	}
}

func (m *Manager) doDownloadCars() {
	for len(m.waitList) > 0 {
		m.doDownloadCar()
	}
}

func (m *Manager) doDownloadCar() {
	cw := m.headFromWaitList()
	if cw == nil {
		return
	}
	defer m.removeCarFromWaitList(cw.Root)

	carfileCache, err := m.restoreCarfileCacheOrNew(&options{cw.Root, cw.Dss, m.carfileStore, m.bFetcher, m.downloadBatch})
	if err != nil {
		log.Errorf("restore carfile cache error:%s", err)
		return
	}

	cw.cache = carfileCache
	err = carfileCache.downloadCar()
	if err != nil {
		log.Errorf("doDownloadCar, download car error:%s", err)
	}

	m.onDownloadCarFinish(carfileCache)
}

func (m *Manager) headFromWaitList() *carWaiter {
	m.waitListLock.Lock()
	defer m.waitListLock.Unlock()

	if len(m.waitList) == 0 {
		return nil
	}
	return m.waitList[0]
}

func (m *Manager) removeCarFromWaitList(root cid.Cid) *carWaiter {
	m.waitListLock.Lock()
	defer m.waitListLock.Unlock()

	if len(m.waitList) == 0 {
		return nil
	}

	for i, cw := range m.waitList {
		if cw.Root.Hash().String() == root.Hash().String() {
			if i == 0 {
				m.waitList = m.waitList[1:]
			} else {
				m.waitList = append(m.waitList[:i], m.waitList[i+1:]...)
			}

			if err := m.saveWaitList(); err != nil {
				log.Errorf("save wait list error: %s", err.Error())
			}
			return cw
		}
	}

	return nil
}

func (m *Manager) AddToWaitList(root cid.Cid, dss []*types.DownloadSource) {
	m.waitListLock.Lock()
	defer m.waitListLock.Unlock()

	for _, waiter := range m.waitList {
		if waiter.Root.Hash().String() == root.Hash().String() {
			return
		}
	}

	cw := &carWaiter{Root: root, Dss: dss}
	m.waitList = append(m.waitList, cw)

	if err := m.saveWaitList(); err != nil {
		log.Errorf("save wait list error: %s", err.Error())
	}

	m.triggerDownload()
}

func (m *Manager) saveIncompleteCarfileCache(cf *carfileCache) error {
	if cf == nil || cf.isDownloadComplete() {
		return nil
	}

	buf, err := cf.encode()
	if err != nil {
		return err
	}
	return m.carfileStore.SaveIncompleteCarfileCache(cf.Root(), buf)
}

func (m *Manager) onDownloadCarFinish(cf *carfileCache) {
	log.Debugf("onDownloadCarFinish, carfile %s", cf.root.String())
	if cf.isDownloadComplete() {
		err := m.carfileStore.RegisterShared(cf.root)
		if err != nil {
			log.Errorf("RegisterShared error:%s", err.Error())
		}

		if err = m.carfileStore.DeleteIncompleteCarfileCache(cf.root); err != nil && !os.IsNotExist(err) {
			log.Errorf("DeleteIncompleteCarfileCache error:%s", err.Error())
		}

	} else {
		if err := m.saveIncompleteCarfileCache(cf); err != nil {
			log.Errorf("saveIncompleteCarfileCache error:%s", err.Error())
		}
	}
}

func (m *Manager) saveWaitList() error {
	data, err := encode(&m.waitList)
	if err != nil {
		return err
	}

	return m.carfileStore.SaveWaitList(data)
}

func (m *Manager) restoreWaitListFromStore() {
	data, err := m.carfileStore.WaitList()
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

func (m *Manager) CachingCar() *carfileCache {
	for _, cw := range m.waitList {
		if cw.cache != nil {
			return cw.cache
		}
	}
	return nil
}

func (m *Manager) restoreCarfileCacheOrNew(opts *options) (*carfileCache, error) {
	data, err := m.carfileStore.IncompleteCarfileCacheData(opts.root)
	if err != nil && !os.IsNotExist(err) {
		log.Errorf("CacheCarfile load incomplete carfile error %s", err.Error())
		return nil, err
	}

	cc := newCarfileCache(opts)
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
func (m *Manager) DeleteCarFromWaitList(root cid.Cid) (bool, error) {
	if c := m.removeCarFromWaitList(root); c != nil {
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

func (m *Manager) Progresses() []*types.AssetCacheProgress {
	progresses := make([]*types.AssetCacheProgress, 0, len(m.waitList))

	for _, cw := range m.waitList {
		progress := &types.AssetCacheProgress{CID: cw.Root.String(), Status: types.CacheStatusWaiting}
		if cw.cache != nil {
			progress = cw.cache.Progress()
		}

		progresses = append(progresses, progress)
	}

	return progresses
}

func (m *Manager) CachedStatus(root cid.Cid) (types.CacheStatus, error) {
	if m.carfileStore.HasCarfile(root) {
		return types.CacheStatusSucceeded, nil
	}

	for _, cw := range m.waitList {
		if cw.Root.Hash().String() == root.Hash().String() {
			if cw.cache != nil {
				return types.CacheStatusCaching, nil
			}
			return types.CacheStatusWaiting, nil
		}
	}

	return types.CacheStatusFailed, nil
}

func (m *Manager) ProgressForFailedCar(root cid.Cid) (*types.AssetCacheProgress, error) {
	progress := &types.AssetCacheProgress{
		CID:    root.String(),
		Status: types.CacheStatusFailed,
	}

	data, err := m.carfileStore.IncompleteCarfileCacheData(root)
	if os.IsNotExist(err) {
		return progress, nil
	}

	if err != nil {
		return nil, err
	}

	cc := &carfileCache{}
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
