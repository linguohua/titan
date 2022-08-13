package scheduler

import (
	"context"
	"fmt"
	"math/rand"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/scheduler/db"

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
func getCacheFailCids(deviceID string) ([]api.ReqCacheData, error) {
	list := make([]api.ReqCacheData, 0)

	infos, err := db.GetCacheDB().GetCacheDataInfos(deviceID)
	if err != nil {
		return list, err
	}

	if len(infos) <= 0 {
		return list, nil
	}

	for cid, tag := range infos {
		isInCacheList, err := db.GetCacheDB().IsNodeInCacheList(cid, deviceID)
		if err == nil && isInCacheList {
			continue
		}

		list = append(list, api.ReqCacheData{Cid: cid, ID: tag})
	}

	return list, nil
}

// NotifyNodeCacheData Cache Data
func cacheDataOfNode(cids []string, deviceID string) error {
	// 判断device是什么节点
	edge := getEdgeNode(deviceID)
	candidate := getCandidateNode(deviceID)
	if edge == nil && candidate == nil {
		return xerrors.New("node not find")
	}
	if edge != nil && candidate != nil {
		return xerrors.New(fmt.Sprintf("node error ,deviceID:%v", deviceID))
	}

	reqs := make([]api.ReqCacheData, 0)

	for _, cid := range cids {
		tag, err := nodeCacheReady(deviceID, cid)
		if err != nil {
			log.Warnf("cacheDataOfNode nodeCacheReady err:%v,cid:%v", err, cid)
			continue
		}

		reqData := api.ReqCacheData{Cid: cid, ID: tag}
		reqs = append(reqs, reqData)
	}

	if edge != nil {
		err := edge.nodeAPI.CacheData(context.Background(), reqs)
		if err != nil {
			log.Errorf("CacheData err:%v", err)
			return err
		}
	}

	if candidate != nil {
		err := candidate.nodeAPI.CacheData(context.Background(), reqs)
		if err != nil {
			log.Errorf("CacheData err:%v", err)
			return err
		}
	}

	return nil
}

// NodeCacheResult Device Cache Result
func nodeCacheResult(deviceID, cid string, isOk bool) error {
	log.Infof("nodeCacheResult deviceID:%v,cid:%v,isOk:%v", deviceID, cid, isOk)

	v, err := db.GetCacheDB().GetCacheDataInfo(deviceID, cid)
	if err != nil || v == "" {
		return nil
	}

	if !isOk {
		return db.GetCacheDB().DelCacheDataInfo(deviceID, cid)
	}

	return db.GetCacheDB().SetNodeToCacheList(deviceID, cid)
}

// Node Cache ready
func nodeCacheReady(deviceID, cid string) (string, error) {
	v, err := db.GetCacheDB().GetCacheDataInfo(deviceID, cid)
	if err == nil && v != "" {
		return "", xerrors.Errorf("already cache")
	}

	tag, err := db.GetCacheDB().GetNodeCacheTag(deviceID)
	if err != nil {
		// log.Errorf("NotifyNodeCacheData getTagWithNode err:%v", err)
		return "", err
	}

	return fmt.Sprintf("%d", tag), db.GetCacheDB().SetCacheDataInfo(deviceID, cid, tag)
}

func randomNum(start, end int) int {
	// rand.Seed(time.Now().UnixNano())

	max := end - start
	if max <= 0 {
		return start
	}

	x := rand.Intn(max)

	return start + x
}
