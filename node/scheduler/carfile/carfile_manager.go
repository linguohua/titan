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
	checkCacheTimeoutInterval = 60      //  set to check node cache timeout timer (Unit:Second)
	cacheTimeoutTime          = 65      //  expiration set to redis (Unit:Second)
	startTaskInterval         = 10      //  time interval (Unit:Second)
	checkExpiredTimerInterval = 60 * 30 //  time interval (Unit:Second)
	runningCarfileMaxCount    = 10      // It needs to be changed to the number of caches
	diskUsageMax              = 90.0    // If the node disk size is greater than this value, caching will not continue
	// dispatchCacheTaskLimit    = 3       // carfile dispatch cache count limit
	rootCacheCount = 1 // The number of caches in the first stage
)

var backupCacheCount = 0 // Cache to the number of candidate nodes （does not contain 'rootCacheCount'）

// Manager carfile
type Manager struct {
	nodeManager             *node.Manager
	RunningCarfileRecordMap sync.Map // cacheing carfile map
	latelyExpiredTime       time.Time
	writeToken              []byte
	runningTaskCount        int
}

// NewCarfileManager new
func NewCarfileManager(nodeManager *node.Manager, writeToken []byte) *Manager {
	d := &Manager{
		nodeManager:       nodeManager,
		latelyExpiredTime: time.Now(),
		writeToken:        writeToken,
	}

	d.resetSystemBaseInfo()
	d.initCarfileMap()
	go d.cacheTaskTicker()
	go d.checkExpiredTicker()

	return d
}

func (m *Manager) initCarfileMap() {
	carfileHashs, err := cache.GetDB().GetCacheingCarfiles()
	if err != nil {
		log.Errorf("initCacheMap GetCacheingCarfiles err:%s", err.Error())
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

		isRunning := false
		// start timout check
		cr.CacheTaskMap.Range(func(key, value interface{}) bool {
			c := value.(*CacheTask)
			if c.status != api.CacheStatusRunning {
				return true
			}

			isRunning = true
			go c.startTimeoutTimer()

			return true
		})

		if !isRunning {
			m.carfileCacheEnd(cr, nil)
		}
	}
}

func (m *Manager) cacheTaskTicker() {
	ticker := time.NewTicker(time.Duration(startTaskInterval) * time.Second)
	defer ticker.Stop()

	for {
		<-ticker.C
		m.startCarfileCacheTasks()
	}
}

func (m *Manager) checkExpiredTicker() {
	ticker := time.NewTicker(time.Duration(checkExpiredTimerInterval) * time.Second)
	defer ticker.Stop()

	for {
		<-ticker.C
		m.checkCachesExpired()
	}
}

func (m *Manager) resetSystemBaseInfo() {
	count, err := persistent.GetSucceededCachesCount()
	if err != nil {
		log.Errorf("resetSystemBaseInfo GetSucceededCachesCount err:%s", err.Error())
		return
	}

	err = cache.GetDB().UpdateSystemBaseInfo(cache.CarFileCountField, count)
	if err != nil {
		log.Errorf("resetSystemBaseInfo UpdateSystemBaseInfo err:%s", err.Error())
	}
}

// GetCarfileRecord get a carfileRecord from map or db
func (m *Manager) GetCarfileRecord(hash string) (*CarfileRecord, error) {
	dI, exist := m.RunningCarfileRecordMap.Load(hash)
	if exist && dI != nil {
		return dI.(*CarfileRecord), nil
	}

	return loadCarfileRecord(hash, m)
}

func (m *Manager) cacheCarfileToNode(info *api.CacheCarfileInfo) error {
	carfileRecord, err := loadCarfileRecord(info.CarfileHash, m)
	if err != nil {
		return err
	}

	// only execute once
	carfileRecord.step = endStep

	m.carfileCacheStart(carfileRecord)

	err = carfileRecord.dispatchCache(info.DeviceID)
	if err != nil {
		m.carfileCacheEnd(carfileRecord, err)
	}

	return nil
}

func (m *Manager) doCarfileCacheTask(info *api.CacheCarfileInfo) error {
	exist, err := persistent.CarfileRecordExisted(info.CarfileHash)
	if err != nil {
		log.Errorf("%s CarfileRecordExist err:%s", info.CarfileCid, err.Error())
		return err
	}

	var carfileRecord *CarfileRecord
	if exist {
		carfileRecord, err = loadCarfileRecord(info.CarfileHash, m)
		if err != nil {
			return err
		}

		carfileRecord.needReliability = info.NeedReliability
		carfileRecord.expiredTime = info.ExpiredTime

		carfileRecord.initStep()
	} else {
		carfileRecord = newCarfileRecord(m, info.CarfileCid, info.CarfileHash)
		carfileRecord.needReliability = info.NeedReliability
		carfileRecord.expiredTime = info.ExpiredTime
	}

	err = persistent.CreateOrUpdateCarfileRecordInfo(&api.CarfileRecordInfo{
		CarfileCid:      carfileRecord.carfileCid,
		NeedReliability: carfileRecord.needReliability,
		ExpiredTime:     carfileRecord.expiredTime,
		CarfileHash:     carfileRecord.carfileHash,
	}, exist)
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
		log.Infof("carfile event %s , add carfile,reliability:%d,expiredTime:%s", info.CarfileCid, info.NeedReliability, info.ExpiredTime.String())
	} else {
		log.Infof("carfile event %s , add carfile,deviceID:%s", info.CarfileCid, info.DeviceID)
	}

	return cache.GetDB().PushCarfileToWaitList(info)
}

// RemoveCarfileRecord remove a carfile
func (m *Manager) RemoveCarfileRecord(carfileCid, hash string) error {
	cInfos, err := persistent.GetCarfileReplicaInfosWithHash(hash, false)
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
	return cache.GetDB().IncrBySystemBaseInfo(cache.CarFileCountField, -count)
}

// RemoveCache remove a cache
func (m *Manager) RemoveCache(carfileCid, deviceID string) error {
	hash, err := cidutil.CIDString2HashString(carfileCid)
	if err != nil {
		return err
	}

	dI, exist := m.RunningCarfileRecordMap.Load(hash)
	if exist && dI != nil {
		return xerrors.Errorf("task %s is running, please wait", carfileCid)
	}

	cacheInfo, err := persistent.GetReplicaInfo(cacheTaskID(hash, deviceID))
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
		err = cache.GetDB().IncrBySystemBaseInfo(cache.CarFileCountField, -1)
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

	var carfileRecord *CarfileRecord
	dI, exist := m.RunningCarfileRecordMap.Load(info.CarfileHash)
	if exist && dI != nil {
		carfileRecord = dI.(*CarfileRecord)
	} else {
		err = xerrors.Errorf("task not running : %s,%s ,err:%v", deviceID, info.CarfileHash, err)
		return
	}

	if !carfileRecord.candidateCacheExisted() {
		carfileRecord.totalSize = info.CarfileSize
		carfileRecord.totalBlocks = info.CarfileBlockCount
	}

	if info.Status == api.CacheStatusCreate {
		info.Status = api.CacheStatusRunning
	}

	err = carfileRecord.carfileCacheResult(deviceID, info)
	return
}

func (m *Manager) startCarfileCacheTasks() {
	doLen := runningCarfileMaxCount - m.runningTaskCount
	if doLen <= 0 {
		return
	}

	for i := 0; i < doLen; i++ {
		info, err := cache.GetDB().GetWaitCarfile()
		if err != nil {
			if cache.GetDB().IsNilErr(err) {
				return
			}
			log.Errorf("GetWaitCarfile err:%s", err.Error())
			continue
		}

		if _, exist := m.RunningCarfileRecordMap.Load(info.CarfileHash); exist {
			log.Errorf("carfileRecord %s is running, please wait", info.CarfileCid)
			continue
		}

		if info.DeviceID != "" {
			err = m.cacheCarfileToNode(info)
		} else {
			err = m.doCarfileCacheTask(info)
		}
		if err != nil {
			log.Errorf("carfile %s do caches err:%s", info.CarfileCid, err.Error())
		}
	}
}

func (m *Manager) carfileCacheStart(cr *CarfileRecord) {
	_, exist := m.RunningCarfileRecordMap.LoadOrStore(cr.carfileHash, cr)
	if !exist {
		m.runningTaskCount++
	}

	log.Infof("carfile %s cache task start ----- cur running count : %d", cr.carfileCid, m.runningTaskCount)
}

func (m *Manager) carfileCacheEnd(cr *CarfileRecord, err error) {
	_, exist := m.RunningCarfileRecordMap.LoadAndDelete(cr.carfileHash)
	if exist {
		m.runningTaskCount--
	}

	log.Infof("carfile %s cache task end ----- cur running count : %d", cr.carfileCid, m.runningTaskCount)

	m.resetLatelyExpiredTime(cr.expiredTime)

	info := &api.CarfileRecordCacheResult{
		NodeErrs:             cr.nodeCacheErrs,
		EdgeNodeCacheSummary: cr.edgeNodeCacheSummary,
	}
	if err != nil {
		info.ErrMsg = err.Error()
	}

	// save result msg
	err = cache.GetDB().SetCarfileRecordCacheResult(cr.carfileHash, info)
	if err != nil {
		log.Errorf("SetCarfileRecordCacheResult err:%s", err.Error())
	}
}

// StopCacheTask stop cache task
func (m *Manager) StopCacheTask(cid, deviceID string) error {
	// TODO
	return xerrors.New("unrealized")
}

// ResetCacheExpiredTime reset expired time
func (m *Manager) ResetCacheExpiredTime(cid string, expiredTime time.Time) error {
	hash, err := cidutil.CIDString2HashString(cid)
	if err != nil {
		return err
	}

	log.Infof("carfile event %s , reset carfile expired time:%s", cid, expiredTime.String())

	dI, exist := m.RunningCarfileRecordMap.Load(hash)
	if exist && dI != nil {
		carfileRecord := dI.(*CarfileRecord)
		carfileRecord.expiredTime = expiredTime
	}

	err = persistent.ChangeExpiredTimeWhitCarfile(hash, expiredTime)
	if err != nil {
		return err
	}

	m.resetLatelyExpiredTime(expiredTime)

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

// check expired caches
func (m *Manager) checkCachesExpired() {
	if m.latelyExpiredTime.After(time.Now()) {
		return
	}

	carfileRecords, err := persistent.GetExpiredCarfiles()
	if err != nil {
		log.Errorf("GetExpiredCarfiles err:%s", err.Error())
		return
	}

	for _, carfileRecord := range carfileRecords {
		// do remove
		err = m.RemoveCarfileRecord(carfileRecord.CarfileCid, carfileRecord.CarfileHash)
		log.Infof("cid:%s, expired,remove it ; %v", carfileRecord.CarfileCid, err)
	}

	// reset latelyExpiredTime
	latelyExpiredTime, err := persistent.GetMinExpiredTime()
	if err != nil {
		return
	}

	m.resetLatelyExpiredTime(latelyExpiredTime)
}

func (m *Manager) resetLatelyExpiredTime(t time.Time) {
	if m.latelyExpiredTime.After(t) {
		m.latelyExpiredTime = t
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

// ResetBackupCacheCount reset backupCacheCount
func (m *Manager) ResetBackupCacheCount(count int) {
	backupCacheCount = count
}

// GetBackupCacheCounts get backupCacheCount
func (m *Manager) GetBackupCacheCounts() int {
	return backupCacheCount
}

func cacheTaskID(hash, deviceID string) string {
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

	result, err := cache.GetDB().GetCarfileRecordCacheResult(hash)
	if err == nil {
		dInfo.ResultInfo = result
	}

	return dInfo, nil
}

// GetRunningCarfileInfos get all running carfiles
func (m *Manager) GetRunningCarfileInfos() []*api.CarfileRecordInfo {
	infos := make([]*api.CarfileRecordInfo, 0)

	m.RunningCarfileRecordMap.Range(func(key, value interface{}) bool {
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

func carfileRecord2Info(d *CarfileRecord) *api.CarfileRecordInfo {
	info := &api.CarfileRecordInfo{}
	if d != nil {
		info.CarfileCid = d.carfileCid
		info.CarfileHash = d.carfileHash
		info.TotalSize = d.totalSize
		info.NeedReliability = d.needReliability
		info.CurReliability = d.curReliability
		info.TotalBlocks = d.totalBlocks
		info.ExpiredTime = d.expiredTime

		caches := make([]*api.CarfileReplicaInfo, 0)

		d.CacheTaskMap.Range(func(key, value interface{}) bool {
			c := value.(*CacheTask)

			cc := &api.CarfileReplicaInfo{
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

		info.CarfileReplicaInfos = caches
	}

	return info
}
