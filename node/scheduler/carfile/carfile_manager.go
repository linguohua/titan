package carfile

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"sort"
	"sync"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/helper"
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

	// It needs to be changed to the number of caches
	runningCarfileMaxCount = 10

	// If the node disk size is greater than this value, caching will not continue
	diskUsageMax = 90.0

	// carfile dispatch cache count limit
	dispatchCacheTaskLimit = 3
)

// Manager carfile
type Manager struct {
	nodeManager       *node.Manager
	CarfileRecordMap  sync.Map // cacheing carfile map
	latelyExpiredTime time.Time
	// isLoadExpiredTime  bool
	authToken        []byte
	RunningTaskCount int

	//The number of caches in the first stage （from ipfs to titan）
	rootCacheCount int
	//Cache to the number of candidate nodes （does not contain 'rootCacheCount'）
	backupCacheCount int
}

// NewCarfileManager new
func NewCarfileManager(nodeManager *node.Manager, authToken []byte) *Manager {
	d := &Manager{
		nodeManager:       nodeManager,
		latelyExpiredTime: time.Now(),
		authToken:         authToken,
		rootCacheCount:    1,
		backupCacheCount:  0,
	}

	d.resetBaseInfo()
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

func (m *Manager) resetBaseInfo() {
	count, err := persistent.GetDB().GetSuccessCachesCount()
	if err != nil {
		log.Errorf("resetBaseInfo GetSuccessCachesCount err:%s", err.Error())
		return
	}

	err = cache.GetDB().UpdateBaseInfo(cache.CarFileCountField, count)
	if err != nil {
		log.Errorf("resetBaseInfo UpdateBaseInfo err:%s", err.Error())
	}
}

// GetCarfileRecord get a carfileRecord from map or db
func (m *Manager) GetCarfileRecord(hash string) (*CarfileRecord, error) {
	dI, ok := m.CarfileRecordMap.Load(hash)
	if ok && dI != nil {
		return dI.(*CarfileRecord), nil
	}

	return loadCarfileRecord(hash, m)
}

func (m *Manager) doCarfileCacheTask(info *api.CacheCarfileInfo) error {
	exist, err := persistent.GetDB().CarfileRecordExist(info.CarfileHash)
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

	err = persistent.GetDB().CreateOrUpdateCarfileRecordInfo(&api.CarfileRecordInfo{
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

func (m *Manager) setCacheEvent(carfileCid, msg string) {
	err := persistent.GetDB().SetCacheEventInfo(&api.CacheEventInfo{
		CID: carfileCid,
		Msg: msg,
	})
	if err != nil {
		log.Errorf("SetCacheEventInfo cid:%s,err:%s,msg:%s", carfileCid, err.Error(), msg)
	}
}

// CacheCarfile new carfile task
func (m *Manager) CacheCarfile(cid string, reliability int, expiredTime time.Time) error {
	hash, err := helper.CIDString2HashString(cid)
	if err != nil {
		return xerrors.Errorf("%s cid to hash err:", cid, err.Error())
	}

	log.Infof("carfile event %s , add carfile,reliability:%d,expiredTime:%s", cid, reliability, expiredTime.String())

	info := &api.CacheCarfileInfo{CarfileHash: hash, CarfileCid: cid, NeedReliability: reliability, ExpiredTime: expiredTime}

	return cache.GetDB().PushCarfileToWaitList(info)
}

// RemoveCarfileRecord remove a carfile
func (m *Manager) RemoveCarfileRecord(carfileCid, hash string) error {
	// dI, exist := m.CarfileRecordMap.Load(hash)
	// if exist && dI != nil {
	// 	return xerrors.Errorf("carfileRecord %s is running, please wait", carfileCid)
	// }

	cInfos, err := persistent.GetDB().GetCacheTaskInfosWithHash(hash, false)
	if err != nil {
		return xerrors.Errorf("GetCacheTaskInfosWithHash: %s,err:%s", carfileCid, err.Error())
	}

	err = persistent.GetDB().RemoveCarfileRecord(hash)
	if err != nil {
		return xerrors.Errorf("RemoveCarfileRecord err:%s ", err.Error())
	}

	log.Infof("carfile event %s , remove carfile record", carfileCid)

	count := int64(0)
	for _, cInfo := range cInfos {
		go m.notifyNodeRemoveCarfile(cInfo.DeviceID, carfileCid)

		if cInfo.Status == api.CacheStatusSuccess {
			count++
		}
	}

	// update record to redis
	return cache.GetDB().IncrByBaseInfo(cache.CarFileCountField, -count)
}

// RemoveCache remove a cache
func (m *Manager) RemoveCache(carfileCid, deviceID string) error {
	hash, err := helper.CIDString2HashString(carfileCid)
	if err != nil {
		return err
	}

	dI, exist := m.CarfileRecordMap.Load(hash)
	if exist && dI != nil {
		return xerrors.Errorf("task %s is running, please wait", carfileCid)
	}

	cacheInfo, err := persistent.GetDB().GetCacheInfo(m.cacheTaskID(hash, deviceID))
	if err != nil {
		return xerrors.Errorf("GetCacheInfo: %s,err:%s", carfileCid, err.Error())
	}

	// delete cache and update carfile info
	err = persistent.GetDB().RemoveCacheTask(cacheInfo.DeviceID, cacheInfo.CarfileHash)
	if err != nil {
		return err
	}

	log.Infof("carfile event %s , remove cache task:%s", carfileCid, deviceID)

	if cacheInfo.Status == api.CacheStatusSuccess {
		err = cache.GetDB().IncrByBaseInfo(cache.CarFileCountField, -1)
		if err != nil {
			log.Errorf("removeCache IncrByBaseInfo err:%s", err.Error())
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
	dI, exist := m.CarfileRecordMap.Load(info.CarfileHash)
	if exist && dI != nil {
		carfileRecord = dI.(*CarfileRecord)
	} else {
		err = xerrors.Errorf("task not running : %s,%s ,err:%v", deviceID, info.CarfileHash, err)
		return
	}

	if !carfileRecord.candidateCacheExist() {
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
	doLen := runningCarfileMaxCount - m.RunningTaskCount
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

		if _, exist := m.CarfileRecordMap.Load(info.CarfileHash); exist {
			// carfileRecord := cI.(*CarfileRecord)
			// carfileRecord.needReliability = info.NeedReliability
			// carfileRecord.expiredTime = info.ExpiredTime
			log.Errorf("carfileRecord %s is running, please wait", info.CarfileCid)
			continue
		}

		err = m.doCarfileCacheTask(info)
		if err != nil {
			log.Errorf("carfile %s do caches err:%s", info.CarfileCid, err.Error())
		}
	}
}

func (m *Manager) carfileCacheStart(cr *CarfileRecord) {
	log.Infof("carfile %s cache task start -----", cr.carfileCid)

	_, exist := m.CarfileRecordMap.LoadOrStore(cr.carfileHash, cr)
	if !exist {
		m.RunningTaskCount++
	}

	// if isSaveEvent {
	// 	m.setCacheEvent(cr.carfileCid, "start task")
	// }
}

func (m *Manager) carfileCacheEnd(cr *CarfileRecord, err error) {
	log.Infof("carfile %s cache task end -----", cr.carfileCid)

	_, exist := m.CarfileRecordMap.LoadAndDelete(cr.carfileHash)
	if exist {
		m.RunningTaskCount--
	}

	if err != nil {
		log.Errorf("end carfile err:%s", err.Error())
	}
	log.Warnf("nodeCacheErrs:%v", cr.nodeCacheErrs)

	m.resetLatelyExpiredTime(cr.expiredTime)

	// save cache result

	// m.setCacheEvent(cr.carfileCid, fmt.Sprintf("end task:%s", msg))
}

// StopCacheTask stop cache task
func (m *Manager) StopCacheTask(cid, deviceID string) error {
	return xerrors.New("unrealized")
}

// ReplenishCacheExpiredTime replenish time
func (m *Manager) ReplenishCacheExpiredTime(cid string, hour int) error {
	hash, err := helper.CIDString2HashString(cid)
	if err != nil {
		return err
	}

	log.Infof("carfile event %s , replenish carfile expired time:%d", cid, hour)

	dI, ok := m.CarfileRecordMap.Load(hash)
	if ok && dI != nil {
		carfileRecord := dI.(*CarfileRecord)
		carfileRecord.expiredTime = carfileRecord.expiredTime.Add((time.Duration(hour) * time.Hour))
	}

	return persistent.GetDB().ExtendExpiredTimeWhitCarfile(hash, hour)
}

// ResetCacheExpiredTime reset expired time
func (m *Manager) ResetCacheExpiredTime(cid string, expiredTime time.Time) error {
	hash, err := helper.CIDString2HashString(cid)
	if err != nil {
		return err
	}

	log.Infof("carfile event %s , reset carfile expired time:%s", cid, expiredTime.String())

	dI, ok := m.CarfileRecordMap.Load(hash)
	if ok && dI != nil {
		carfileRecord := dI.(*CarfileRecord)
		carfileRecord.expiredTime = expiredTime
	}

	err = persistent.GetDB().ChangeExpiredTimeWhitCarfile(hash, expiredTime)
	if err != nil {
		return err
	}

	m.resetLatelyExpiredTime(expiredTime)

	return nil
}

// NodesQuit clean node caches info and restore caches
func (m *Manager) NodesQuit(deviceIDs []string) {
	carfileMap, err := persistent.GetDB().UpdateCacheInfoOfQuitNode(deviceIDs)
	if err != nil {
		log.Errorf("CleanNodeAndRestoreCaches err:%s", err.Error())
		return
	}

	log.Infof("node event , nodes quit:%v", deviceIDs)

	m.resetBaseInfo()

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

	carfileRecords, err := persistent.GetDB().GetExpiredCarfiles()
	if err != nil {
		log.Errorf("GetExpiredCarfiles err:%s", err.Error())
		return
	}

	for _, carfileRecord := range carfileRecords {
		// do remove
		err = m.RemoveCarfileRecord(carfileRecord.CarfileCid, carfileRecord.CarfileHash)
		log.Warnf("cid:%s, expired,remove it ; %v", carfileRecord.CarfileCid, err)
	}

	// reset latelyExpiredTime
	latelyExpiredTime, err := persistent.GetDB().GetMinExpiredTime()
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

type findNodeResult struct {
	list                  []string
	allNodeCount          int
	filterCount           int
	insufficientDiskCount int
}

// find the edges
func (m *Manager) findAppropriateEdges(filterMap sync.Map, count int) *findNodeResult {
	resultInfo := &findNodeResult{}

	nodes := make([]*node.Node, 0)
	if count <= 0 {
		return resultInfo
	}

	m.nodeManager.EdgeNodeMap.Range(func(key, value interface{}) bool {
		deviceID := key.(string)
		resultInfo.allNodeCount++

		if cI, exist := filterMap.Load(deviceID); exist {
			cache := cI.(*CacheTask)
			if cache.status == api.CacheStatusSuccess {
				resultInfo.filterCount++
				return true
			}
		}

		node := value.(*node.EdgeNode)
		if node.DiskUsage > diskUsageMax {
			resultInfo.insufficientDiskCount++
			return true
		}

		nodes = append(nodes, node.Node)
		return true
	})

	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].GetCurCacheCount() < nodes[j].GetCurCacheCount()
	})

	if count > len(nodes) {
		count = len(nodes)
	}

	for _, node := range nodes[0:count] {
		resultInfo.list = append(resultInfo.list, node.DeviceID)
	}
	return resultInfo
}

// find the candidates
func (m *Manager) findAppropriateCandidates(filterMap sync.Map, count int) *findNodeResult {
	resultInfo := &findNodeResult{}

	nodes := make([]*node.Node, 0)
	if count <= 0 {
		return resultInfo
	}

	m.nodeManager.CandidateNodeMap.Range(func(key, value interface{}) bool {
		deviceID := key.(string)
		resultInfo.allNodeCount++

		if cI, exist := filterMap.Load(deviceID); exist {
			cache := cI.(*CacheTask)
			if cache.status == api.CacheStatusSuccess {
				resultInfo.filterCount++
				return true
			}
		}

		node := value.(*node.CandidateNode)
		if node.DiskUsage > diskUsageMax {
			resultInfo.insufficientDiskCount++
			return true
		}

		nodes = append(nodes, node.Node)
		return true
	})

	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].GetCurCacheCount() < nodes[j].GetCurCacheCount()
	})

	if count > len(nodes) {
		count = len(nodes)
	}

	for _, node := range nodes[0:count] {
		resultInfo.list = append(resultInfo.list, node.DeviceID)
	}
	return resultInfo
}

// ResetBackupCacheCount reset backupCacheCount
func (m *Manager) ResetBackupCacheCount(backupCacheCount int) {
	m.backupCacheCount = backupCacheCount
}

// GetBackupCacheCounts get backupCacheCount
func (m *Manager) GetBackupCacheCounts() int {
	return m.backupCacheCount
}

func (m *Manager) cacheTaskID(hash, deviceID string) string {
	input := fmt.Sprintf("%s%s", hash, deviceID)

	c := sha1.New()
	c.Write([]byte(input))
	bytes := c.Sum(nil)
	return hex.EncodeToString(bytes)
}
