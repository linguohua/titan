package scheduler

import (
	"context"
	"github.com/linguohua/titan/node/scheduler/db/persistent"

	"github.com/linguohua/titan/api"
)

type AppUpdater struct {
	db            *persistent.NodeMgrDB
	appUpdateInfo map[int]*api.NodeAppUpdateInfo
}

func NewAppUpdater(db *persistent.NodeMgrDB) (*AppUpdater, error) {
	updater := &AppUpdater{
		db:            db,
		appUpdateInfo: make(map[int]*api.NodeAppUpdateInfo),
	}
	appUpdateInfo, err := db.GetNodeUpdateInfos()
	if err != nil {
		log.Errorf("GetNodeUpdateInfos error:%s", err)
		return nil, err
	}
	updater.appUpdateInfo = appUpdateInfo
	return updater, nil
}

func (a *AppUpdater) GetNodeAppUpdateInfos(ctx context.Context) (map[int]*api.NodeAppUpdateInfo, error) {
	return a.appUpdateInfo, nil
}

func (a *AppUpdater) SetNodeAppUpdateInfo(ctx context.Context, info *api.NodeAppUpdateInfo) error {
	if a.appUpdateInfo == nil {
		a.appUpdateInfo = make(map[int]*api.NodeAppUpdateInfo)
	}
	a.appUpdateInfo[int(info.NodeType)] = info
	a.db.SetNodeUpdateInfo(info)
	return nil
}

func (a *AppUpdater) DeleteNodeAppUpdateInfos(ctx context.Context, nodeType int) error {
	delete(a.appUpdateInfo, nodeType)
	return a.db.DeleteNodeUpdateInfo(nodeType)
}
