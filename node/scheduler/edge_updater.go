package scheduler

import (
	"context"

	"github.com/linguohua/titan/node/scheduler/db"

	"github.com/linguohua/titan/api"
)

// EdgeUpdateManager manages information about edge node updates.
type EdgeUpdateManager struct {
	db          *db.SQLDB
	updateInfos map[int]*api.EdgeUpdateInfo
}

// NewEdgeUpdateManager creates a new EdgeUpdateManager with the given SQL database connection.
func NewEdgeUpdateManager(db *db.SQLDB) (*EdgeUpdateManager, error) {
	updater := &EdgeUpdateManager{
		db:          db,
		updateInfos: make(map[int]*api.EdgeUpdateInfo),
	}
	appUpdateInfo, err := db.LoadEdgeUpdateInfos()
	if err != nil {
		log.Errorf("GetEdgeUpdateInfos error:%s", err)
		return nil, err
	}
	updater.updateInfos = appUpdateInfo
	return updater, nil
}

// GetEdgeUpdateInfos  returns the map of edge node update information.
func (eu *EdgeUpdateManager) GetEdgeUpdateInfos(ctx context.Context) (map[int]*api.EdgeUpdateInfo, error) {
	return eu.updateInfos, nil
}

// SetEdgeUpdateInfo sets the EdgeUpdateInfo for the given node type.
func (eu *EdgeUpdateManager) SetEdgeUpdateInfo(ctx context.Context, info *api.EdgeUpdateInfo) error {
	if eu.updateInfos == nil {
		eu.updateInfos = make(map[int]*api.EdgeUpdateInfo)
	}
	eu.updateInfos[info.NodeType] = info
	return eu.db.SetEdgeUpdateInfo(info)
}

// DeleteEdgeUpdateInfo deletes the EdgeUpdateInfo for the given node type.
func (eu *EdgeUpdateManager) DeleteEdgeUpdateInfo(ctx context.Context, nodeType int) error {
	delete(eu.updateInfos, nodeType)
	return eu.db.DeleteEdgeUpdateInfo(nodeType)
}

// GetNodeAppUpdateInfos returns the map of node app update information.
func (eu *EdgeUpdateManager) GetNodeAppUpdateInfos(ctx context.Context) (map[int]*api.EdgeUpdateInfo, error) {
	return eu.updateInfos, nil
}
