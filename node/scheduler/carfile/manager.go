package carfile

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/cidutil"
	"github.com/linguohua/titan/node/scheduler/db/cache"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
	"github.com/linguohua/titan/node/scheduler/node"
	"golang.org/x/xerrors"
)

var log = logging.Logger("carfile")

const (
	checkCacheTimeoutInterval    = 60      //  set to check node cache timeout timer (Unit:Second)
	cacheTimeoutTime             = 65      //  expiration set to redis (Unit:Second)
	startTaskInterval            = 10      //  time interval (Unit:Second)
	checkExpirationTimerInterval = 60 * 30 //  time interval (Unit:Second)
	downloadingCarfileMaxCount   = 10      // It needs to be changed to the number of caches
	diskUsageMax                 = 90.0    // If the node disk size is greater than this value, caching will not continue
	rootCacheCount               = 1       // The number of caches in the first stage
)

var candidateReplicaCacheCount = 0 // Cache to the number of candidate nodes （does not contain 'rootCacheCount'）

// Manager carfile
type Manager struct {
	nodeManager                 *node.Manager
	DownloadingCarfileRecordMap sync.Map // caching carfile map
	latelyExpirationTime        time.Time
	writeToken                  []byte
	downloadingTaskCount        int
}

// NewManager new
func NewManager(nodeManager *node.Manager, writeToken []byte) *Manager {
	d := &Manager{
		nodeManager:          nodeManager,
		latelyExpirationTime: time.Now(),
		writeToken:           writeToken,
	}

	d.resetSystemBaseInfo()
	d.initCarfileMap()
	go d.carfileTaskTicker()
	go d.checkExpirationTicker()

	return d
}

func (m *Manager) initCarfileMap() {
	carfileHashs, err := cache.GetCachingCarfiles()
	if err != nil {
		log.Errorf("initCacheMap GetCachingCarfiles err:%s", err.Error())
		return
	}

	for _, hash := range carfileHashs {
		cr, err := loadCarfileRecord(hash, m)
		if err != nil {
			log.Errorf("initCacheMap loadCarfileRecord hash:%s , err:%s", hash, err.Error())
			continue
		}
		cr.initStep()

		m.carfileCacheStart(cr)

		isDownloading := false
		// start timout check
		cr.ReplicaMap.Range(func(key, value interface{}) bool {
			c := value.(*Replica)
			if c.status != api.CacheStatusDownloading {
				return true
			}

			isDownloading = true
			go c.startTimeoutTimer()

			return true
		})

		if !isDownloading {
			m.carfileCacheEnd(cr, nil)
		}
	}
}

func (m *Manager) carfileTaskTicker() {
	ticker := time.NewTicker(time.Duration(startTaskInterval) * time.Second)
	defer ticker.Stop()

	for {
		<-ticker.C
		m.startCarfileReplicaTasks()
	}
}

func (m *Manager) checkExpirationTicker() {
	ticker := time.NewTicker(time.Duration(checkExpirationTimerInterval) * time.Second)
	defer ticker.Stop()

	for {
		<-ticker.C
		m.checkCachesExpiration()
	}
}

func (m *Manager) resetSystemBaseInfo() {
	count, err := persistent.GetSucceededCachesCount()
	if err != nil {
		log.Errorf("resetSystemBaseInfo GetSucceededCachesCount err:%s", err.Error())
		return
	}

	err = cache.UpdateSystemBaseInfo(cache.CarFileCountField, count)
	if err != nil {
		log.Errorf("resetSystemBaseInfo UpdateSystemBaseInfo err:%s", err.Error())
	}
}

// GetCarfileRecord get a carfileRecord from map or db
func (m *Manager) GetCarfileRecord(hash string) (*Record, error) {
	dI, exist := m.DownloadingCarfileRecordMap.Load(hash)
	if exist && dI != nil {
		return dI.(*Record), nil
	}

	return loadCarfileRecord(hash, m)
}

func (m *Manager) cacheCarfileToNode(info *api.CacheCarfileInfo) error {
	carfileRecord, err := loadCarfileRecord(info.CarfileHash, m)
	if err != nil {
		return err
	}

	// only execute once
	carfileRecord.step = cachedStep

	m.carfileCacheStart(carfileRecord)

	err = carfileRecord.dispatchCache(info.DeviceID)
	if err != nil {
		m.carfileCacheEnd(carfileRecord, err)
	}

	return nil
}

func (m *Manager) doCarfileReplicaTask(info *api.CacheCarfileInfo) error {
	exist, err := persistent.CarfileRecordExisted(info.CarfileHash)
	if err != nil {
		log.Errorf("%s CarfileRecordExist err:%s", info.CarfileCid, err.Error())
		return err
	}

	var carfileRecord *Record
	if exist {
		carfileRecord, err = loadCarfileRecord(info.CarfileHash, m)
		if err != nil {
			return err
		}

		carfileRecord.replica = info.Replica
		carfileRecord.expirationTime = info.ExpirationTime

		carfileRecord.initStep()
	} else {
		carfileRecord = newCarfileRecord(m, info.CarfileCid, info.CarfileHash)
		carfileRecord.replica = info.Replica
		carfileRecord.expirationTime = info.ExpirationTime
	}

	err = persistent.CreateOrUpdateCarfileRecordInfo(&api.CarfileRecordInfo{
		CarfileCid:     carfileRecord.carfileCid,
		Replica:        carfileRecord.replica,
		ExpirationTime: carfileRecord.expirationTime,
		CarfileHash:    carfileRecord.carfileHash,
	})
	if err != nil {
		return xerrors.Errorf("cid:%s,CreateOrUpdateCarfileRecordInfo err:%s", carfileRecord.carfileCid, err.Error())
	}

	m.carfileCacheStart(carfileRecord)

	err = carfileRecord.dispatchCaches()
	if err != nil {
		m.carfileCacheEnd(carfileRecord, err)
	}

	return nil
}

// CacheCarfile new carfile task
func (m *Manager) CacheCarfile(info *api.CacheCarfileInfo) error {
	if info.DeviceID == "" {
		log.Infof("carfile event %s , add carfile,replica:%d,expiration:%s", info.CarfileCid, info.Replica, info.ExpirationTime.String())
	} else {
		log.Infof("carfile event %s , add carfile,deviceID:%s", info.CarfileCid, info.DeviceID)
	}

	return cache.PushCarfileToWaitList(info)
}

// RemoveCarfileRecord remove a carfile
func (m *Manager) RemoveCarfileRecord(carfileCid, hash string) error {
	cInfos, err := persistent.CarfileReplicaInfosWithHash(hash, false)
	if err != nil {
		return xerrors.Errorf("GetCarfileReplicaInfosWithHash: %s,err:%s", carfileCid, err.Error())
	}

	err = persistent.RemoveCarfileRecord(hash)
	if err != nil {
		return xerrors.Errorf("RemoveCarfileRecord err:%s ", err.Error())
	}

	log.Infof("carfile event %s , remove carfile record", carfileCid)

	count := int64(0)
	for _, cInfo := range cInfos {
		go m.notifyNodeRemoveCarfile(cInfo.DeviceID, carfileCid)

		if cInfo.Status == api.CacheStatusSucceeded {
			count++
		}
	}

	// update record to redis
	return cache.IncrBySystemBaseInfo(cache.CarFileCountField, -count)
}

// RemoveCache remove a cache
func (m *Manager) RemoveCache(carfileCid, deviceID string) error {
	hash, err := cidutil.CIDString2HashString(carfileCid)
	if err != nil {
		return err
	}

	dI, exist := m.DownloadingCarfileRecordMap.Load(hash)
	if exist && dI != nil {
		return xerrors.Errorf("task %s is downloading, please wait", carfileCid)
	}

	cacheInfo, err := persistent.GetReplicaInfo(replicaID(hash, deviceID))
	if err != nil {
		return xerrors.Errorf("GetReplicaInfo: %s,err:%s", carfileCid, err.Error())
	}

	// delete cache and update carfile info
	err = persistent.RemoveCarfileReplica(cacheInfo.DeviceID, cacheInfo.CarfileHash)
	if err != nil {
		return err
	}

	log.Infof("carfile event %s , remove cache task:%s", carfileCid, deviceID)

	if cacheInfo.Status == api.CacheStatusSucceeded {
		err = cache.IncrBySystemBaseInfo(cache.CarFileCountField, -1)
		if err != nil {
			log.Errorf("removeCache IncrBySystemBaseInfo err:%s", err.Error())
		}
	}

	go m.notifyNodeRemoveCarfile(cacheInfo.DeviceID, carfileCid)

	return nil
}

// CacheCarfileResult block cache result
func (m *Manager) CacheCarfileResult(deviceID string, info *api.CacheResultInfo) (err error) {
	log.Debugf("carfileCacheResult :%s , %d , %s", deviceID, info.Status, info.CarfileHash)
	// log.Debugf("carfileCacheResult :%v", info)

	var carfileRecord *Record
	dI, exist := m.DownloadingCarfileRecordMap.Load(info.CarfileHash)
	if exist && dI != nil {
		carfileRecord = dI.(*Record)
	} else {
		err = xerrors.Errorf("task not downloading : %s,%s ,err:%v", deviceID, info.CarfileHash, err)
		return
	}

	if !carfileRecord.candidateCacheExisted() {
		carfileRecord.totalSize = info.CarfileSize
		carfileRecord.totalBlocks = info.CarfileBlockCount
	}

	if info.Status == api.CacheStatusCreate {
		info.Status = api.CacheStatusDownloading
	}

	err = carfileRecord.carfileCacheResult(deviceID, info)
	return
}

func (m *Manager) startCarfileReplicaTasks() {
	doLen := downloadingCarfileMaxCount - m.downloadingTaskCount
	if doLen <= 0 {
		return
	}

	for i := 0; i < doLen; i++ {
		info, err := cache.GetWaitCarfile()
		if err != nil {
			if cache.IsNilErr(err) {
				return
			}
			log.Errorf("GetWaitCarfile err:%s", err.Error())
			continue
		}

		if _, exist := m.DownloadingCarfileRecordMap.Load(info.CarfileHash); exist {
			log.Errorf("carfileRecord %s is downloading, please wait", info.CarfileCid)
			continue
		}

		if info.DeviceID != "" {
			err = m.cacheCarfileToNode(info)
		} else {
			err = m.doCarfileReplicaTask(info)
		}
		if err != nil {
			log.Errorf("carfile %s do caches err:%s", info.CarfileCid, err.Error())
		}
	}
}

func (m *Manager) carfileCacheStart(cr *Record) {
	_, exist := m.DownloadingCarfileRecordMap.LoadOrStore(cr.carfileHash, cr)
	if !exist {
		m.downloadingTaskCount++
	}

	log.Infof("carfile %s cache task start ----- cur downloading count : %d", cr.carfileCid, m.downloadingTaskCount)
}

func (m *Manager) carfileCacheEnd(cr *Record, err error) {
	_, exist := m.DownloadingCarfileRecordMap.LoadAndDelete(cr.carfileHash)
	if exist {
		m.downloadingTaskCount--
	}

	log.Infof("carfile %s cache task end ----- cur downloading count : %d", cr.carfileCid, m.downloadingTaskCount)

	m.resetLatelyExpirationTime(cr.expirationTime)

	info := &api.CarfileRecordCacheResult{
		NodeErrs:             cr.nodeCacheErrs,
		EdgeNodeCacheSummary: cr.edgeNodeCacheSummary,
	}
	if err != nil {
		info.ErrMsg = err.Error()
	}

	// save result msg
	err = cache.SetCarfileRecordCacheResult(cr.carfileHash, info)
	if err != nil {
		log.Errorf("SetCarfileRecordCacheResult err:%s", err.Error())
	}
}

// ResetCacheExpirationTime reset expiration time
func (m *Manager) ResetCacheExpirationTime(cid string, t time.Time) error {
	hash, err := cidutil.CIDString2HashString(cid)
	if err != nil {
		return err
	}

	log.Infof("carfile event %s , reset carfile expiration time:%s", cid, t.String())

	dI, exist := m.DownloadingCarfileRecordMap.Load(hash)
	if exist && dI != nil {
		carfileRecord := dI.(*Record)
		carfileRecord.expirationTime = t
	}

	err = persistent.ResetCarfileExpirationTime(hash, t)
	if err != nil {
		return err
	}

	m.resetLatelyExpirationTime(t)

	return nil
}

// NodesQuit clean node caches info and restore caches
func (m *Manager) NodesQuit(deviceIDs []string) {
	carfileMap, err := persistent.UpdateCacheInfoOfQuitNode(deviceIDs)
	if err != nil {
		log.Errorf("CleanNodeAndRestoreCaches err:%s", err.Error())
		return
	}

	log.Infof("node event , nodes quit:%v", deviceIDs)

	m.resetSystemBaseInfo()

	// recache
	for _, info := range carfileMap {
		log.Infof("need restore carfile :%s", info.CarfileCid)
	}
}

// check expiration caches
func (m *Manager) checkCachesExpiration() {
	if m.latelyExpirationTime.After(time.Now()) {
		return
	}

	carfileRecords, err := persistent.ExpiredCarfiles()
	if err != nil {
		log.Errorf("ExpiredCarfiles err:%s", err.Error())
		return
	}

	for _, carfileRecord := range carfileRecords {
		// do remove
		err = m.RemoveCarfileRecord(carfileRecord.CarfileCid, carfileRecord.CarfileHash)
		log.Infof("cid:%s, expired,remove it ; %v", carfileRecord.CarfileCid, err)
	}

	// reset expiration time
	latelyExpirationTime, err := persistent.MinExpirationTime()
	if err != nil {
		return
	}

	m.resetLatelyExpirationTime(latelyExpirationTime)
}

func (m *Manager) resetLatelyExpirationTime(t time.Time) {
	if m.latelyExpirationTime.After(t) {
		m.latelyExpirationTime = t
	}
}

// Notify node to delete all carfile
func (m *Manager) notifyNodeRemoveCarfiles(deviceID string) error {
	edge := m.nodeManager.GetEdgeNode(deviceID)
	if edge != nil {
		return edge.GetAPI().DeleteAllCarfiles(context.Background())
	}

	candidate := m.nodeManager.GetCandidateNode(deviceID)
	if candidate != nil {
		return candidate.GetAPI().DeleteAllCarfiles(context.Background())
	}

	return nil
}

// Notify node to delete carfile
func (m *Manager) notifyNodeRemoveCarfile(deviceID, cid string) error {
	edge := m.nodeManager.GetEdgeNode(deviceID)
	if edge != nil {
		return edge.GetAPI().DeleteCarfile(context.Background(), cid)
	}

	candidate := m.nodeManager.GetCandidateNode(deviceID)
	if candidate != nil {
		return candidate.GetAPI().DeleteCarfile(context.Background(), cid)
	}

	return nil
}

// ResetReplicaCount reset candidate replica count
func (m *Manager) ResetReplicaCount(count int) {
	candidateReplicaCacheCount = count
}

// GetCandidateReplicaCount get candidta replica count
func (m *Manager) GetCandidateReplicaCount() int {
	return candidateReplicaCacheCount
}

func replicaID(hash, deviceID string) string {
	input := fmt.Sprintf("%s%s", hash, deviceID)

	c := sha1.New()
	c.Write([]byte(input))
	bytes := c.Sum(nil)
	return hex.EncodeToString(bytes)
}

// GetCarfileRecordInfo get carfile record info of cid
func (m *Manager) GetCarfileRecordInfo(cid string) (*api.CarfileRecordInfo, error) {
	hash, err := cidutil.CIDString2HashString(cid)
	if err != nil {
		return nil, err
	}

	cr, err := m.GetCarfileRecord(hash)
	if err != nil {
		return nil, err
	}

	dInfo := carfileRecord2Info(cr)

	result, err := cache.GetCarfileRecordCacheResult(hash)
	if err == nil {
		dInfo.ResultInfo = result
	}

	return dInfo, nil
}

// GetDownloadingCarfileInfos get all downloading carfiles
func (m *Manager) GetDownloadingCarfileInfos() []*api.CarfileRecordInfo {
	infos := make([]*api.CarfileRecordInfo, 0)

	m.DownloadingCarfileRecordMap.Range(func(key, value interface{}) bool {
		if value != nil {
			data := value.(*Record)
			if data != nil {
				cInfo := carfileRecord2Info(data)
				infos = append(infos, cInfo)
			}
		}

		return true
	})

	return infos
}

func carfileRecord2Info(d *Record) *api.CarfileRecordInfo {
	info := &api.CarfileRecordInfo{}
	if d != nil {
		info.CarfileCid = d.carfileCid
		info.CarfileHash = d.carfileHash
		info.TotalSize = d.totalSize
		info.Replica = d.replica
		info.EdgeReplica = d.edgeReplica
		info.TotalBlocks = d.totalBlocks
		info.ExpirationTime = d.expirationTime

		caches := make([]*api.ReplicaInfo, 0)

		d.ReplicaMap.Range(func(key, value interface{}) bool {
			c := value.(*Replica)

			cc := &api.ReplicaInfo{
				Status:      c.status,
				DoneSize:    c.doneSize,
				DoneBlocks:  c.doneBlocks,
				IsCandidate: c.isCandidate,
				DeviceID:    c.deviceID,
				CreateTime:  c.createTime,
				EndTime:     c.endTime,
			}

			caches = append(caches, cc)
			return true
		})

		info.ReplicaInfos = caches
	}

	return info
}
