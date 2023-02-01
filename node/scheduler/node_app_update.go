package scheduler

import (
	"context"

	"github.com/linguohua/titan/api"
)

func (s *Scheduler) GetNodeAppUpdateInfos(ctx context.Context) (map[int]*api.NodeAppUpdateInfo, error) {
	// TODO: load data when scheduler init
	return s.nodeAppUpdateInfos, nil
}

func (s *Scheduler) SetNodeAppUpdateInfo(ctx context.Context, nodeType api.NodeType, info *api.NodeAppUpdateInfo) error {
	// TODO: save data to db
	s.nodeAppUpdateInfos[int(nodeType)] = info
	return nil
}
