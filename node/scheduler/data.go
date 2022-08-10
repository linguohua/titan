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
func cacheDataOfNode(cid, deviceID string) error {
	edge := getEdgeNode(deviceID)
	if edge == nil {
		return xerrors.New("node not find")
	}

	tag, err := nodeCacheReady(deviceID, cid)
	if err != nil {
		log.Errorf("cacheDataOfNode nodeCacheReady err:%v", err)
		return err
	}

	reqData := api.ReqCacheData{Cid: cid, ID: fmt.Sprintf("%d", tag)}

	err = edge.nodeAPI.CacheData(context.Background(), []api.ReqCacheData{reqData})
	if err != nil {
		log.Errorf("cacheDataOfNode err:%v", err)
		return err
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
func nodeCacheReady(deviceID, cid string) (int64, error) {
	v, err := db.GetCacheDB().GetCacheDataInfo(deviceID, cid)
	if err == nil && v != "" {
		return 0, xerrors.Errorf("already cache")
	}

	tag, err := db.GetCacheDB().GetNodeCacheTag(deviceID)
	if err != nil {
		log.Errorf("NotifyNodeCacheData getTagWithNode err:%v", err)
		return 0, err
	}

	return tag, db.GetCacheDB().SetCacheDataInfo(deviceID, cid, tag)
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
