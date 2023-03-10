package cache

import (
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
	root cid.Cid
	dss  []*types.DownloadSource
}
type Manager struct {
	// root cid of car
	waitList      []*carWaiter
	waitListLock  *sync.Mutex
	downloadCh    chan bool
	carfileStore  *carfilestore.CarfileStore
	bFetcher      fetcher.BlockFetcher
	cResulter     CachedResulter
	cachingCar    *carfileCache
	downloadBatch int
}

type ManagerOptions struct {
	CarfileStore  *carfilestore.CarfileStore
	BFetcher      fetcher.BlockFetcher
	CResulter     CachedResulter
	DownloadBatch int
}

func NewManager(opts *ManagerOptions) *Manager {
	m := &Manager{
		waitList:      make([]*carWaiter, 0),
		waitListLock:  &sync.Mutex{},
		downloadCh:    make(chan bool),
		carfileStore:  opts.CarfileStore,
		bFetcher:      opts.BFetcher,
		cResulter:     opts.CResulter,
		downloadBatch: opts.DownloadBatch,
	}
	m.restoreWaitListFromStore()

	go m.startCarfileDownloader()
	go m.startTick()

	return m
}

func (m *Manager) startTick() {
	ticker := time.NewTicker(10 * time.Second)
	for {
		select {
		case <-ticker.C:
			if m.cachingCar != nil {
				err := m.CachedResult(m.cachingCar)
				if err != nil {
					log.Errorf("startTickForDownloadResult, downloadResult error:%s", err.Error())
				}

				m.saveIncompleteCarfileCache(m.cachingCar)
			}

			if len(m.waitList) > 0 {
				m.saveWaitList()
			}
		}
	}
}

func (m *Manager) notifyCarfileDownloader() {
	select {
	case m.downloadCh <- true:
	default:
	}
}

func (m *Manager) startCarfileDownloader() {
	if m.bFetcher == nil {
		log.Panic("m.dBlockser == nil")
	}

	for {
		m.doDownloadCars()
		<-m.downloadCh
	}
}

func (m *Manager) doDownloadCars() {
	for {
		cw := m.headFromWaitList()
		if cw == nil {
			return
		}

		m.downloadCar(cw)
	}
}

func (m *Manager) downloadCar(cw *carWaiter) {
	defer m.removeCarFromWaitList(cw.root)

	bsrw, err := m.carfileStore.NewCarfileWriter(cw.root)
	if err != nil {
		log.Errorf("doDownloadCar, new car error:%s", err)
		return
	}

	carfileCache, err := m.restoreCarfileCacheOrNew(&options{cw.root, cw.dss, bsrw, m.bFetcher, m.downloadBatch})
	if err != nil {
		log.Errorf("restore carfile cache error:%s", err)
		return
	}

	m.cachingCar = carfileCache
	err = carfileCache.downloadCar()
	if err != nil {
		log.Errorf("doDownloadCarfile, downloadCarfile error:%s", err)
	}

	m.cachingCar = nil
	m.onDownloadCarComplete(carfileCache)
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
		if cw.root.Hash().String() == root.Hash().String() {
			if i == 0 {
				m.waitList = append(m.waitList[1:])
			} else {
				m.waitList = append(m.waitList[:i], m.waitList[i+1:]...)
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
		if waiter.root.Hash().String() == root.Hash().String() {
			return
		}
	}

	cw := &carWaiter{root: root, dss: dss}
	m.waitList = append(m.waitList, cw)

	m.saveWaitList()
	m.notifyCarfileDownloader()
}

func (m *Manager) saveIncompleteCarfileCache(cf *carfileCache) error {
	buf, err := encode(cf)
	if err != nil {
		return err
	}
	return m.carfileStore.SaveIncompleteCarfileCache(cf.root.Hash().String(), buf)
}

func (m *Manager) onDownloadCarComplete(cf *carfileCache) {
	log.Debugf("onDownloadCarfileComplete, carfile %s", cf.root.Hash().String())
	m.carfileStore.RegisterShared(cf.root)

	if cf.doneSize != cf.totalSize {
		m.saveIncompleteCarfileCache(cf)
	} else {
		m.carfileStore.DeleteIncompleteCarfileCache(cf.root.Hash().String())
	}

	err := m.saveWaitList()
	if err != nil {
		log.Errorf("onDownloadCarfileComplete, saveCarfileTable error:%s", err)
	}

	err = m.CachedResult(cf)
	if err != nil {
		log.Errorf("onDownloadCarfileComplete, downloadResult error:%s, carfileCID:%s", err, cf.root.Hash().String())
	}
}

func (m *Manager) saveWaitList() error {
	data, err := encode(m.waitList)
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

	log.Debugf("restoreWaitListFromStore:%v", m.waitList)
}

func (m *Manager) WaitListLen() int {
	return len(m.waitList)
}

func (m *Manager) CachingCar() *carfileCache {
	return m.cachingCar
}

func (m *Manager) restoreCarfileCacheOrNew(opts *options) (*carfileCache, error) {
	data, err := m.carfileStore.IncompleteCarfileCacheData(opts.root.Hash().String())
	if err != nil && err != datastore.ErrNotFound {
		log.Errorf("CacheCarfile load incomplete carfile error %s", err.Error())
		return nil, err
	}

	cc := newCarfileCache(opts)
	if len(data) > 0 {
		err = cc.decode(data)
		if err != nil {
			return nil, err
		}
	}
	return cc, nil
}

func (m *Manager) CachedResult(cachingCar *carfileCache) error {
	if m.cResulter == nil {
		log.Panicf("cResulter == nil")
	}

	ret := &types.CacheResult{
		CarfileBlockCount: len(cachingCar.blocksDownloadSuccessList) + len(cachingCar.blocksWaitList),
		DoneBlockCount:    len(cachingCar.blocksDownloadSuccessList),
		CarfileSize:       int64(cachingCar.TotalSize()),
		DoneSize:          int64(cachingCar.DoneSize()),
		CarfileHash:       cachingCar.Root().Hash().String(),
	}

	return m.cResulter.CacheResult(ret)
}

// return true if exist in waitList
func (m *Manager) DeleteCarFromWaitList(root cid.Cid) (bool, error) {
	if root.Hash().String() == string(m.cachingCar.Root().Hash()) {
		m.cachingCar.CancelDownload()
	}
	if c := m.removeCarFromWaitList(root); c != nil {
		return true, nil
	}
	return false, nil
}
