package carfile

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"github.com/linguohua/titan/node/common"
	"sync"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/cidutil"
	"github.com/linguohua/titan/node/scheduler/db/cache"
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

var candidateReplicaCacheCount = 0 // nodeMgrCache to the number of candidate nodes （does not contain 'rootCacheCount'）

// Manager carfile
type Manager struct {
	nodeManager               *node.Manager
	DownloadingCarfileRecords sync.Map // caching carfile map
	latelyExpirationTime      time.Time
	writeToken                []byte
	downloadingTaskCount      int
}

// NewManager return new carfile manager instance
func NewManager(nodeManager *node.Manager, writeToken common.PermissionWriteToken) *Manager {
	m := &Manager{
		nodeManager:          nodeManager,
		latelyExpirationTime: time.Now(),
		writeToken:           writeToken,
	}

	m.initCarfileMap()
	go m.carfileTaskTicker()
	go m.checkExpirationTicker()

	return m
}

func (m *Manager) initCarfileMap() {
	hashs, err := m.nodeManager.CarfileCache.GetCachingCarfiles()
	if err != nil {
		log.Errorf("initCacheMap GetCachingCarfiles err:%s", err.Error())
		return
	}

	for _, hash := range hashs {
		cr, err := m.loadCarfileRecord(hash, m)
		if err != nil {
			log.Errorf("initCacheMap loadCarfileRecord hash:%s , err:%s", hash, err.Error())
			continue
		}
		cr.initStep()

		m.carfileCacheStart(cr)

		isDownloading := false
		// start timout check
		cr.Replicas.Range(func(key, value interface{}) bool {
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

// GetCarfileRecord get a carfileRecord from map or db
func (m *Manager) GetCarfileRecord(hash string) (*CarfileRecord, error) {
	dI, exist := m.DownloadingCarfileRecords.Load(hash)
	if exist && dI != nil {
		return dI.(*CarfileRecord), nil
	}

	return m.loadCarfileRecord(hash, m)
}

func (m *Manager) cacheCarfileToNode(info *api.CacheCarfileInfo) error {
	carfileRecord, err := m.loadCarfileRecord(info.CarfileHash, m)
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
	exist, err := m.nodeManager.CarfileDB.CarfileRecordExisted(info.CarfileHash)
	if err != nil {
		log.Errorf("%s CarfileRecordExist err:%s", info.CarfileCid, err.Error())
		return err
	}

	var carfileRecord *CarfileRecord
	if exist {
		carfileRecord, err = m.loadCarfileRecord(info.CarfileHash, m)
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

	err = m.nodeManager.CarfileDB.CreateOrUpdateCarfileRecordInfo(&api.CarfileRecordInfo{
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

	return m.nodeManager.CarfileCache.PushCarfileToWaitList(info)
}

// RemoveCarfileRecord remove a carfile
func (m *Manager) RemoveCarfileRecord(carfileCid, hash string) error {
	cInfos, err := m.nodeManager.CarfileDB.CarfileReplicaInfosWithHash(hash, false)
	if err != nil {
		return xerrors.Errorf("GetCarfileReplicaInfosWithHash: %s,err:%s", carfileCid, err.Error())
	}

	err = m.nodeManager.CarfileDB.RemoveCarfileRecord(hash)
	if err != nil {
		return xerrors.Errorf("RemoveCarfileRecord err:%s ", err.Error())
	}

	log.Infof("carfile event %s , remove carfile record", carfileCid)

	for _, cInfo := range cInfos {
		go m.notifyNodeRemoveCarfile(cInfo.DeviceID, carfileCid)
	}

	return nil
}

// RemoveCache remove a cache
func (m *Manager) RemoveCache(carfileCid, deviceID string) error {
	hash, err := cidutil.CIDString2HashString(carfileCid)
	if err != nil {
		return err
	}

	dI, exist := m.DownloadingCarfileRecords.Load(hash)
	if exist && dI != nil {
		return xerrors.Errorf("task %s is downloading, please wait", carfileCid)
	}

	cacheInfo, err := m.nodeManager.CarfileDB.LoadReplicaInfo(replicaID(hash, deviceID))
	if err != nil {
		return xerrors.Errorf("GetReplicaInfo: %s,err:%s", carfileCid, err.Error())
	}

	// delete cache and update carfile info
	err = m.nodeManager.CarfileDB.RemoveCarfileReplica(cacheInfo.DeviceID, cacheInfo.CarfileHash)
	if err != nil {
		return err
	}

	log.Infof("carfile event %s , remove cache task:%s", carfileCid, deviceID)

	go m.notifyNodeRemoveCarfile(cacheInfo.DeviceID, carfileCid)

	return nil
}

// CacheCarfileResult block cache result
func (m *Manager) CacheCarfileResult(deviceID string, info *api.CacheResultInfo) (err error) {
	log.Debugf("carfileCacheResult :%s , %d , %s", deviceID, info.Status, info.CarfileHash)
	// log.Debugf("carfileCacheResult :%v", info)

	var carfileRecord *CarfileRecord
	dI, exist := m.DownloadingCarfileRecords.Load(info.CarfileHash)
	if exist && dI != nil {
		carfileRecord = dI.(*CarfileRecord)
	} else {
		err = xerrors.Errorf("task not downloading : %s,%s ,err:%v", deviceID, info.CarfileHash, err)
		return
	}

	if carfileRecord.step == rootCandidateCacheStep {
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
		info, err := m.nodeManager.CarfileCache.GetWaitCarfile()
		if err != nil {
			if cache.IsNilErr(err) {
				return
			}
			log.Errorf("GetWaitCarfile err:%s", err.Error())
			continue
		}

		if _, exist := m.DownloadingCarfileRecords.Load(info.CarfileHash); exist {
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

func (m *Manager) carfileCacheStart(cr *CarfileRecord) {
	_, exist := m.DownloadingCarfileRecords.LoadOrStore(cr.carfileHash, cr)
	if !exist {
		m.downloadingTaskCount++
	}

	log.Infof("carfile %s cache task start ----- cur downloading count : %d", cr.carfileCid, m.downloadingTaskCount)
}

func (m *Manager) carfileCacheEnd(cr *CarfileRecord, err error) {
	_, exist := m.DownloadingCarfileRecords.LoadAndDelete(cr.carfileHash)
	if exist {
		m.downloadingTaskCount--
	}

	log.Infof("carfile %s cache task end ----- cur downloading count : %d", cr.carfileCid, m.downloadingTaskCount)

	m.resetLatelyExpirationTime(cr.expirationTime)

	// save result msg
	info := &api.CarfileRecordCacheResult{
		NodeErrs:             cr.nodeCacheErrs,
		EdgeNodeCacheSummary: cr.findNodesDetails,
	}
	if err != nil {
		info.ErrMsg = err.Error()
	}

	// err = cache.SetCarfileRecordCacheResult(cr.carfileHash, info)
	// if err != nil {
	// 	log.Errorf("SetCarfileRecordCacheResult err:%s", err.Error())
	// }
}

// ResetCacheExpirationTime reset expiration time
func (m *Manager) ResetCacheExpirationTime(cid string, t time.Time) error {
	hash, err := cidutil.CIDString2HashString(cid)
	if err != nil {
		return err
	}

	log.Infof("carfile event %s , reset carfile expiration time:%s", cid, t.String())

	dI, exist := m.DownloadingCarfileRecords.Load(hash)
	if exist && dI != nil {
		carfileRecord := dI.(*CarfileRecord)
		carfileRecord.expirationTime = t
	}

	err = m.nodeManager.CarfileDB.ResetCarfileExpirationTime(hash, t)
	if err != nil {
		return err
	}

	m.resetLatelyExpirationTime(t)

	return nil
}

// check expiration caches
func (m *Manager) checkCachesExpiration() {
	if m.latelyExpirationTime.After(time.Now()) {
		return
	}

	carfileRecords, err := m.nodeManager.CarfileDB.ExpiredCarfiles()
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
	latelyExpirationTime, err := m.nodeManager.CarfileDB.MinExpirationTime()
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
		return edge.API().DeleteAllCarfiles(context.Background())
	}

	candidate := m.nodeManager.GetCandidateNode(deviceID)
	if candidate != nil {
		return candidate.API().DeleteAllCarfiles(context.Background())
	}

	return nil
}

// Notify node to delete carfile
func (m *Manager) notifyNodeRemoveCarfile(deviceID, cid string) error {
	edge := m.nodeManager.GetEdgeNode(deviceID)
	if edge != nil {
		return edge.API().DeleteCarfile(context.Background(), cid)
	}

	candidate := m.nodeManager.GetCandidateNode(deviceID)
	if candidate != nil {
		return candidate.API().DeleteCarfile(context.Background(), cid)
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

	// result, err := cache.GetCarfileRecordCacheResult(hash)
	// if err == nil {
	// 	dInfo.ResultInfo = result
	// }

	return dInfo, nil
}

// GetDownloadingCarfileInfos get all downloading carfiles
func (m *Manager) GetDownloadingCarfileInfos() []*api.CarfileRecordInfo {
	infos := make([]*api.CarfileRecordInfo, 0)

	m.DownloadingCarfileRecords.Range(func(key, value interface{}) bool {
		if value != nil {
			data := value.(*CarfileRecord)
			if data != nil {
				cInfo := carfileRecord2Info(data)
				infos = append(infos, cInfo)
			}
		}

		return true
	})

	return infos
}

func carfileRecord2Info(cr *CarfileRecord) *api.CarfileRecordInfo {
	info := &api.CarfileRecordInfo{}
	if cr != nil {
		info.CarfileCid = cr.carfileCid
		info.CarfileHash = cr.carfileHash
		info.TotalSize = cr.totalSize
		info.Replica = cr.replica
		info.EdgeReplica = cr.edgeReplica
		info.TotalBlocks = cr.totalBlocks
		info.ExpirationTime = cr.expirationTime

		raInfos := make([]*api.ReplicaInfo, 0)

		cr.Replicas.Range(func(key, value interface{}) bool {
			ra := value.(*Replica)

			raInfo := &api.ReplicaInfo{
				Status:      ra.status,
				DoneSize:    ra.doneSize,
				DoneBlocks:  ra.doneBlocks,
				IsCandidate: ra.isCandidate,
				DeviceID:    ra.deviceID,
				CreateTime:  ra.createTime,
				EndTime:     ra.endTime,
			}

			raInfos = append(raInfos, raInfo)
			return true
		})

		info.ReplicaInfos = raInfos
	}

	return info
}
