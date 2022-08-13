package scheduler

import (
	"context"
	"time"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/scheduler/db"
	"github.com/linguohua/titan/region"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-jsonrpc"
	gocid "github.com/ipfs/go-cid"
)

// EdgeNode Edge node
type EdgeNode struct {
	nodeAPI api.Edge
	closer  jsonrpc.ClientCloser

	deviceInfo api.DevicesInfo

	geoInfo region.GeoInfo

	addr string
}

// CandidateNode Candidate node
type CandidateNode struct {
	nodeAPI api.Candidate
	closer  jsonrpc.ClientCloser

	deviceInfo api.DevicesInfo

	geoInfo region.GeoInfo

	addr string

	isValidator bool
}

// NodeOnline Save DeciceInfo
func nodeOnline(deviceID string, onlineTime int64, geoInfo region.GeoInfo, typeName api.NodeTypeName) error {
	nodeInfo, err := db.GetCacheDB().GetNodeInfo(deviceID)
	if err == nil {
		if typeName != nodeInfo.NodeType {
			return xerrors.New("node type inconsistent")
		}

		if nodeInfo.Geo != geoInfo.Geo {
			// delete old
			err = db.GetCacheDB().DelNodeWithGeoList(deviceID, nodeInfo.Geo)
			if err != nil {
				log.Warnf("DelNodeWithGeoList err:%v", err)
			}
		}
	} else {
		if err.Error() != db.NotFind {
			log.Warnf("GetNodeInfo err:%v", err)
		}
	}
	// log.Infof("oldgeo:%v,newgeo:%v,err:%v", nodeInfo.Geo, geoInfo.Geo, err)

	lastTime := time.Now().Format("2006-01-02 15:04:05")
	err = db.GetCacheDB().SetNodeInfo(deviceID, db.NodeInfo{OnLineTime: onlineTime, Geo: geoInfo.Geo, LastTime: lastTime, IsOnline: true, NodeType: typeName})
	if err != nil {
		return err
	}

	err = db.GetCacheDB().SetNodeToGeoList(deviceID, geoInfo.Geo)
	if err != nil {
		return err
	}

	// err = db.GetCacheDB().SetNodeToNodeList(deviceID, typeName)
	// if err != nil {
	// 	return err
	// }

	err = db.GetCacheDB().SetGeoToList(geoInfo.Geo)
	if err != nil {
		return err
	}

	return nil
}

// NodeOffline offline
func nodeOffline(deviceID string, geoInfo region.GeoInfo, nodeType api.NodeTypeName) error {
	err := db.GetCacheDB().DelNodeWithGeoList(deviceID, geoInfo.Geo)
	if err != nil {
		return err
	}

	lastTime := time.Now().Format("2006-01-02 15:04:05")
	err = db.GetCacheDB().SetNodeInfo(deviceID, db.NodeInfo{OnLineTime: 0, Geo: geoInfo.Geo, LastTime: lastTime, IsOnline: false, NodeType: nodeType})
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
		datas, err := db.GetCacheDB().GetCacheDataInfos(edge.deviceInfo.DeviceId)
		if err != nil {
			log.Warnf("spotCheck GetCacheDataInfos err:%v,DeviceId:%v", err.Error(), edge.deviceInfo.DeviceId)
			continue
		}

		if len(datas) <= 0 {
			continue
		}

		// random
		var cid string
		var tag string
		randomN := randomNum(0, len(datas)-1)
		num := 0
		for c, t := range datas {
			cid = c
			tag = t

			if num == randomN {
				break
			}
			num++
		}

		req = append(req, api.ReqVarify{Fid: tag, URL: edge.addr})

		result[edge.deviceInfo.DeviceId] = cid
	}
	// 请求抽查
	varifyResults, err := candidate.nodeAPI.VerifyData(context.Background(), req)
	if err != nil {
		log.Warnf("VerifyData err:%v, DeviceId:%v", err.Error(), candidate.deviceInfo.DeviceId)
		return
	}

	// varify Result
	for _, varifyResult := range varifyResults {
		// TODO 判断带宽 超时时间等等
		// 结果判断
		c := result[varifyResult.DeviceID]

		// 判断CID
		gC, err := gocid.Decode(c)
		if err != nil {
			log.Warnf("Decode err:%v", err.Error())
			continue
		}

		gC = gocid.NewCidV1(gC.Type(), gC.Hash())

		vC, err := gocid.Decode(varifyResult.Cid)
		if err != nil {
			log.Warnf("Decode err:%v", err.Error())
			continue
		}

		vC = gocid.NewCidV1(vC.Type(), vC.Hash())

		cidOK := gC.Equals(vC)

		log.Infof("varifyResult candidate:%v , edge:%v ,eCid:%v,sCid:%v cidOK:%v", candidate.deviceInfo.DeviceId, varifyResult.DeviceID, varifyResult.Cid, c, cidOK)
		log.Infof("varifyResult vC:%v gC:%v", vC, gC)
		// TODO 写入DB 时间:候选节点:被验证节点:验证的cid:序号:结果
	}
}

// spot check edges
func spotChecks() error {
	// log.Infof("validatorCount:%v,candidateCount:%v", validatorCount, candidateCount)
	// find validators
	validators, err := db.GetCacheDB().GetValidatorsWithList()
	if err != nil {
		return err
	}

	for _, validatorID := range validators {
		geos, err := db.GetCacheDB().GetGeoWithValidatorList(validatorID)
		if err != nil {
			log.Warnf("GetGeoWithValidatorList err:%v,validatorID:%v", err.Error(), validatorID)
			continue
		}

		// edge list
		edges := make([]*EdgeNode, 0)

		// find edge
		for _, geo := range geos {
			deviceIDs, err := db.GetCacheDB().GetNodesWithGeoList(geo)
			if err != nil {
				log.Warnf("GetNodesWithGeoList err:%v,geo:%v", err.Error(), geo)
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

// 重置 验证者数据
func cleanValidators() error {
	validators, err := db.GetCacheDB().GetValidatorsWithList()
	if err != nil {
		return err
	}

	for _, validator := range validators {
		err = db.GetCacheDB().DelValidatorGeoList(validator)
		if err != nil {
			log.Warnf("DelValidatorGeoList err:%v, validator:%v", err.Error(), validator)
		}

		node := getCandidateNode(validator)
		if node != nil {
			node.isValidator = false
			// candidateCount++
			// validatorCount--
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

		gInfo := region.StringGeoToGeoInfo(geo)
		if gInfo == nil {
			log.Warnf("StringGeoToGeoInfo geo:%v", geo)
			continue
		}

		// 找出这个区域 或者邻近区域里的 所有候选节点
		cns, _ := findCandidateNodeWithGeo(*gInfo, []string{})
		if len(cns) > 0 {
			// random a validator
			validator = cns[randomNum(0, len(cns)-1)].deviceInfo.DeviceId
		} else {
			continue
		}

		// TODO 这里要分配好,不能让一个验证者  分配到太多的边缘节点
		getCandidateNode(validator).isValidator = true

		err = db.GetCacheDB().SetValidatorToList(validator)
		if err != nil {
			log.Warnf("SetValidatorToList err:%v, validator : %s", err.Error(), validator)
		}

		err = db.GetCacheDB().SetGeoToValidatorList(validator, geo)
		if err != nil {
			log.Warnf("SetGeoToValidatorList err:%v, validator : %s, geo : %s", err.Error(), validator, geo)
		}
	}

	// reset count
	resetCandidateAndValidatorCount()

	return nil
}
