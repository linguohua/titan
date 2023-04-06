package scheduler

import (
	"context"

	"github.com/linguohua/titan/node/scheduler/db"

	"github.com/linguohua/titan/api"
)

// EdgeUpdater edge updater
type EdgeUpdater struct {
	db          *db.SQLDB
	updateInfos map[int]*api.EdgeUpdateInfo
}

// NewEdgeUpdater update infos
func NewEdgeUpdater(db *db.SQLDB) (*EdgeUpdater, error) {
	updater := &EdgeUpdater{
		db:          db,
		updateInfos: make(map[int]*api.EdgeUpdateInfo),
	}
	appUpdateInfo, err := db.FetchEdgeUpdateInfos()
	if err != nil {
		log.Errorf("GetEdgeUpdateInfos error:%s", err)
		return nil, err
	}
	updater.updateInfos = appUpdateInfo
	return updater, nil
}

// EdgeUpdateInfos edge update infos
func (eu *EdgeUpdater) EdgeUpdateInfos(ctx context.Context) (map[int]*api.EdgeUpdateInfo, error) {
	return eu.updateInfos, nil
}

// SetEdgeUpdateInfo set edge update info
func (eu *EdgeUpdater) SetEdgeUpdateInfo(ctx context.Context, info *api.EdgeUpdateInfo) error {
	if eu.updateInfos == nil {
		eu.updateInfos = make(map[int]*api.EdgeUpdateInfo)
	}
	eu.updateInfos[info.NodeType] = info
	return eu.db.SetEdgeUpdateInfo(info)
}

// DeleteEdgeUpdateInfo delete edge update info
func (eu *EdgeUpdater) DeleteEdgeUpdateInfo(ctx context.Context, nodeType int) error {
	delete(eu.updateInfos, nodeType)
	return eu.db.DeleteEdgeUpdateInfo(nodeType)
}

// GetNodeAppUpdateInfos get node app update info
func (eu *EdgeUpdater) GetNodeAppUpdateInfos(ctx context.Context) (map[int]*api.EdgeUpdateInfo, error) {
	return eu.updateInfos, nil
}
