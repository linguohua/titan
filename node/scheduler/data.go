package scheduler

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/scheduler/db"
	"github.com/linguohua/titan/region"

	"golang.org/x/xerrors"
)

type geoLevel int64

const (
	defaultLevel  geoLevel = 0
	countryLevel  geoLevel = 1
	provinceLevel geoLevel = 2
	cityLevel     geoLevel = 3
)

// 检查缓存失败的cid
func getCacheFailCids(deviceID string) ([]string, error) {
	infos, err := db.GetCacheDB().GetCacheDataInfos(deviceID)
	if err != nil {
		return nil, err
	}

	if len(infos) <= 0 {
		return nil, nil
	}

	cs := make([]string, 0)

	for cid, tag := range infos {
		// isInCacheList, err := db.GetCacheDB().IsNodeInCacheList(cid, deviceID)
		if tag == fmt.Sprintf("%d", -1) {
			cs = append(cs, cid)
		}
	}

	return cs, nil
}

// NotifyNodeCacheData Cache Data
func cacheDataOfNode(cids []string, deviceID string) ([]string, []string, error) {
	// 判断device是什么节点
	edge := getEdgeNode(deviceID)
	candidate := getCandidateNode(deviceID)
	if edge == nil && candidate == nil {
		return nil, nil, xerrors.New("node not find")
	}
	if edge != nil && candidate != nil {
		return nil, nil, xerrors.New(fmt.Sprintf("node error ,deviceID:%v", deviceID))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if edge != nil {
		reqDatas, alreadyCacheCids, notFindNodeCids := getReqCacheData(deviceID, cids, true, edge.geoInfo)

		for _, reqData := range reqDatas {
			err := edge.nodeAPI.CacheData(ctx, reqData)
			if err != nil {
				log.Errorf("edge CacheData err:%v,url:%v,cids:%v", err.Error(), reqData.CandidateURL, reqData.Cids)
			}
		}

		return alreadyCacheCids, notFindNodeCids, nil
	}

	if candidate != nil {
		reqDatas, alreadyCacheCids, notFindNodeCids := getReqCacheData(deviceID, cids, false, candidate.geoInfo)

		for _, reqData := range reqDatas {
			err := candidate.nodeAPI.CacheData(ctx, reqData)
			if err != nil {
				log.Errorf("candidate CacheData err:%v,url:%v,cids:%v", err.Error(), reqData.CandidateURL, reqData.Cids)
			}
		}

		return alreadyCacheCids, notFindNodeCids, nil
	}

	return nil, nil, nil
}

func getReqCacheData(deviceID string, cids []string, isEdge bool, geoInfo region.GeoInfo) ([]api.ReqCacheData, []string, []string) {
	alreadyCacheCids := make([]string, 0)
	notFindNodeCids := make([]string, 0)
	reqList := make([]api.ReqCacheData, 0)

	if !isEdge {
		cs := make([]string, 0)

		for _, cid := range cids {
			err := nodeCacheReady(deviceID, cid)
			if err != nil {
				log.Warnf("cacheDataOfNode nodeCacheReady err:%v,cid:%v", err, cid)
				alreadyCacheCids = append(alreadyCacheCids, cid)
				continue
			}

			cs = append(cs, cid)
		}

		reqList = append(reqList, api.ReqCacheData{Cids: cs})

		return reqList, alreadyCacheCids, notFindNodeCids
	}

	// 如果是边缘节点的话 要在候选节点里面找资源
	csMap := make(map[string][]string)

	for _, cid := range cids {
		err := nodeCacheReady(deviceID, cid)
		if err != nil {
			log.Warnf("cacheDataOfNode nodeCacheReady err:%v,cid:%v", err, cid)
			alreadyCacheCids = append(alreadyCacheCids, cid)
			continue
		}
		// 看看哪个候选节点有这个cid
		candidates, err := getCandidateNodesWithData(cid, geoInfo)
		if err != nil {
			log.Warnf("cacheDataOfNode getCandidateNodesWithData err:%v,cid:%v", err, cid)
			notFindNodeCids = append(notFindNodeCids, cid)
			continue
		}

		candidate := candidates[randomNum(0, len(candidates))]

		list := csMap[candidate.deviceInfo.DeviceId]
		if list == nil {
			list = make([]string, 0)
		}
		list = append(list, cid)

		csMap[candidate.deviceInfo.DeviceId] = list
	}

	for deviceID, list := range csMap {
		node := getCandidateNode(deviceID)
		if node != nil {
			reqList = append(reqList, api.ReqCacheData{Cids: list, CandidateURL: node.addr})
		}
	}

	return reqList, alreadyCacheCids, notFindNodeCids
}

// NodeCacheResult Device Cache Result
func nodeCacheResult(deviceID, cid string, isOk bool) (string, error) {
	log.Infof("nodeCacheResult deviceID:%v,cid:%v,isOk:%v", deviceID, cid, isOk)
	v, err := db.GetCacheDB().GetCacheDataInfo(deviceID, cid)
	if err == nil && v != -1 {
		return fmt.Sprintf("%d", v), nil
	}

	if !isOk {
		// return "", db.GetCacheDB().DelCacheDataInfo(deviceID, cid)
		return "", nil
	}

	tag, err := db.GetCacheDB().IncrNodeCacheTag(deviceID)
	if err != nil {
		// log.Errorf("NotifyNodeCacheData getTagWithNode err:%v", err)
		return "", err
	}

	err = db.GetCacheDB().SetCacheDataInfo(deviceID, cid, tag)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%d", tag), db.GetCacheDB().SetNodeToCacheList(deviceID, cid)
}

func newCacheDataTag(deviceID, cid string) (string, error) {
	v, err := db.GetCacheDB().GetCacheDataInfo(deviceID, cid)
	if err == nil && v != -1 {
		return fmt.Sprintf("%d", v), xerrors.Errorf("already cache")
	}

	tag, err := db.GetCacheDB().IncrNodeCacheTag(deviceID)
	if err != nil {
		// log.Errorf("NotifyNodeCacheData getTagWithNode err:%v", err)
		return "", err
	}

	return fmt.Sprintf("%d", tag), nil
}

// Node Cache ready
func nodeCacheReady(deviceID, cid string) error {
	v, err := db.GetCacheDB().GetCacheDataInfo(deviceID, cid)
	if err == nil && v != -1 {
		return xerrors.Errorf("already cache")
	}

	return db.GetCacheDB().SetCacheDataInfo(deviceID, cid, -1)
}

// 生成[start,end)结束的随机数
func randomNum(start, end int) int {
	// rand.Seed(time.Now().UnixNano())

	max := end - start
	if max <= 0 {
		return start
	}

	x := rand.Intn(max)
	return start + x
}

// 生成count个[start,end)结束的不重复的随机数
func randomNums(start int, end int, count int) []int {
	// 范围检查
	if end < start {
		return nil
	}

	// 存放结果的slice
	nums := make([]int, 0)

	if (end - start) < count {
		for i := start; i < end; i++ {
			nums = append(nums, i)
		}

		return nums
	}

	// 随机数生成器，加入时间戳保证每次生成的随机数不一样
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	for len(nums) < count {
		// 生成随机数
		num := r.Intn((end - start)) + start
		// 查重
		exist := false
		for _, v := range nums {
			if v == num {
				exist = true
				break
			}
		}

		if !exist {
			nums = append(nums, num)
		}
	}

	return nums
}
