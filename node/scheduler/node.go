package scheduler

import (
	"context"
	"time"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/geoip"
	"github.com/linguohua/titan/node/scheduler/db"

	"github.com/filecoin-project/go-jsonrpc"
)

// EdgeNode Edge node
type EdgeNode struct {
	nodeAPI api.Edge
	closer  jsonrpc.ClientCloser

	deviceInfo api.DevicesInfo

	geoInfo geoip.GeoInfo

	addr string
}

// CandidateNode Candidate node
type CandidateNode struct {
	nodeAPI api.Candidate
	closer  jsonrpc.ClientCloser

	deviceInfo api.DevicesInfo

	geoInfo geoip.GeoInfo

	addr string
}

// NodeOnline Save DeciceInfo
func nodeOnline(deviceID string, onlineTime int64, geoInfo geoip.GeoInfo, typeName api.NodeTypeName) error {
	nodeInfo, err := db.GetCacheDB().GetNodeInfo(deviceID)
	if err == nil && nodeInfo.Geo != geoInfo.Geo {
		// delete old
		err = db.GetCacheDB().DelNodeWithGeoList(deviceID, nodeInfo.Geo)
		// log.Infof("SremSet err:%v", err)
	}
	// log.Infof("oldgeo:%v,newgeo:%v,err:%v", nodeInfo.Geo, geoInfo.Geo, err)

	lastTime := time.Now().Format("2006-01-02 15:04:05")
	err = db.GetCacheDB().SetNodeInfo(deviceID, db.NodeInfo{OnLineTime: onlineTime, Geo: geoInfo.Geo, LastTime: lastTime, IsOnline: true})
	if err != nil {
		return err
	}

	err = db.GetCacheDB().SetNodeToGeoList(deviceID, geoInfo.Geo)
	if err != nil {
		return err
	}

	err = db.GetCacheDB().SetNodeToNodeList(deviceID, typeName)
	if err != nil {
		return err
	}

	err = db.GetCacheDB().SetGeoToList(geoInfo.Geo)
	if err != nil {
		return err
	}

	return nil
}

// NodeOffline offline
func nodeOffline(deviceID string, geoInfo geoip.GeoInfo) error {
	err := db.GetCacheDB().DelNodeWithGeoList(deviceID, geoInfo.Geo)
	if err != nil {
		return err
	}

	lastTime := time.Now().Format("2006-01-02 15:04:05")
	err = db.GetCacheDB().SetNodeInfo(deviceID, db.NodeInfo{OnLineTime: 0, Geo: geoInfo.Geo, LastTime: lastTime, IsOnline: false})
	if err != nil {
		return err
	}

	return nil
}

func spotCheck(candidate *CandidateNode, edges []*EdgeNode) {
	req := make([]api.ReqVarify, 0)
	result := make(map[string]string)

	for _, edge := range edges {
		// 查看节点缓存了哪些数据
		infos, err := db.GetCacheDB().GetCacheDataInfos(edge.deviceInfo.DeviceId)
		if err != nil {
			log.Errorf("spotCheck GetCacheDataInfos err:%v,DeviceId:%v", err.Error(), edge.deviceInfo.DeviceId)
			continue
		}

		if len(infos) <= 0 {
			continue
		}

		// TODO 随机抽查某个数据
		var cid string
		var tag string
		for c, t := range infos {
			cid = c
			tag = t
		}

		req = append(req, api.ReqVarify{Fid: tag, URL: edge.addr})

		result[edge.deviceInfo.DeviceId] = cid
	}
	// 请求抽查
	varifyResults, err := candidate.nodeAPI.VerifyData(context.Background(), req)
	if err != nil {
		log.Errorf("VerifyData err : %v , DeviceId : %v", err.Error(), candidate.deviceInfo.DeviceId)
		return
	}
	// 抽查结果
	for _, varifyResult := range varifyResults {
		if varifyResult.IsTimeout {
			// TODO
		}
	}

	log.Infof("varifyResult candidate:%v", candidate.deviceInfo.DeviceId)
	// TODO 写入DB 时间:候选节点:被验证节点:验证的cid:序号:结果
}

// 抽查
func spotChecks() error {
	// find validators
	validators, err := db.GetCacheDB().GetValidatorsWithList()
	if err != nil {
		return err
	}

	for _, validatorID := range validators {
		geos, err := db.GetCacheDB().GetGeoWithValidatorList(validatorID)
		if err != nil {
			continue
		}

		// 待抽查列表
		edges := make([]*EdgeNode, 0)

		// find edge
		for _, geo := range geos {
			deviceIDs, err := db.GetCacheDB().GetNodesWithGeoList(geo)
			if err != nil {
				continue
			}

			for _, deviceID := range deviceIDs {
				edge := getEdgeNode(deviceID)
				if edge != nil {
					edges = append(edges, edge)
				}
			}
		}

		validator := getCandidateNode(validatorID)
		if validator != nil {
			spotCheck(validator, edges)
		}

	}

	return nil
}

func cleanValidators() error {
	validators, err := db.GetCacheDB().GetValidatorsWithList()
	if err != nil {
		return err
	}

	for _, validator := range validators {
		err = db.GetCacheDB().DelValidatorGeoList(validator)
		if err != nil {
			log.Errorf("DelValidatorGeoList err : %v, validator : %v", err.Error(), validator)
		}
	}

	err = db.GetCacheDB().DelValidatorList()
	if err != nil {
		return err
	}

	return nil
}

// 选举、分配验证者负责的区域
func electionValidators() error {
	// 每个城市 选出X个验证者
	// 每隔Y时间 重新选举
	err := cleanValidators()
	if err != nil {
		return err
	}

	geos, err := db.GetCacheDB().GetGeosWithList()
	if err != nil {
		return err
	}

	for _, geo := range geos {
		validator := ""

		gInfo := geoip.StringGeoToGeoInfo(geo)
		if gInfo == nil {
			log.Errorf("StringGeoToGeoInfo geo :%v", geo)
			continue
		}

		// 找出这个区域 或者邻近区域里的 所有候选节点
		cns, _ := findCandidateNodeWithGeo(*gInfo, []string{})
		if len(cns) > 0 {
			// 随机一个为验证者
			validator = cns[0].deviceInfo.DeviceId
		} else {
			continue
		}
		// TODO 这里要分配好,不能让一个验证者  分配到太多的边缘节点
		err = db.GetCacheDB().SetValidatorToList(validator)
		if err != nil {
			log.Errorf("SetValidatorToList err : %v , validator : %s", err.Error(), validator)
		}

		err = db.GetCacheDB().SetGeoToValidatorList(validator, geo)
		if err != nil {
			log.Errorf("SetGeoToValidatorList err : %v, validator : %s, geo : %s", err.Error(), validator, geo)
		}
	}

	return nil
}
