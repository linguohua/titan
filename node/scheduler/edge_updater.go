package scheduler

import (
	"context"

	"github.com/linguohua/titan/node/scheduler/db"

	"github.com/linguohua/titan/api"
)

type EdgeUpdater struct {
	db          *db.SqlDB
	updateInfos map[int]*api.EdgeUpdateInfo
}

func NewEdgeUpdater(db *db.SqlDB) (*EdgeUpdater, error) {
	updater := &EdgeUpdater{
		db:          db,
		updateInfos: make(map[int]*api.EdgeUpdateInfo),
	}
	appUpdateInfo, err := db.EdgeUpdateInfos()
	if err != nil {
		log.Errorf("GetEdgeUpdateInfos error:%s", err)
		return nil, err
	}
	updater.updateInfos = appUpdateInfo
	return updater, nil
}

func (eu *EdgeUpdater) EdgeUpdateInfos(ctx context.Context) (map[int]*api.EdgeUpdateInfo, error) {
	return eu.updateInfos, nil
}

func (eu *EdgeUpdater) SetEdgeUpdateInfo(ctx context.Context, info *api.EdgeUpdateInfo) error {
	if eu.updateInfos == nil {
		eu.updateInfos = make(map[int]*api.EdgeUpdateInfo)
	}
	eu.updateInfos[int(info.NodeType)] = info
	eu.db.SetEdgeUpdateInfo(info)
	return nil
}

func (eu *EdgeUpdater) DeleteEdgeUpdateInfo(ctx context.Context, nodeType int) error {
	delete(eu.updateInfos, nodeType)
	return eu.db.DeleteEdgeUpdateInfo(nodeType)
}

func (eu *EdgeUpdater) GetNodeAppUpdateInfos(ctx context.Context) (map[int]*api.EdgeUpdateInfo, error) {
	return eu.updateInfos, nil
}
