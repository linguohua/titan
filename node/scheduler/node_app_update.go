package scheduler

import (
	"context"

	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/scheduler/db/persistent"
)

func (s *Scheduler) GetNodeAppUpdateInfos(ctx context.Context) (map[int]*api.NodeAppUpdateInfo, error) {
	return s.nodeAppUpdateInfos, nil
}

func (s *Scheduler) SetNodeAppUpdateInfo(ctx context.Context, info *api.NodeAppUpdateInfo) error {
	if s.nodeAppUpdateInfos == nil {
		s.nodeAppUpdateInfos = make(map[int]*api.NodeAppUpdateInfo)
	}
	s.nodeAppUpdateInfos[int(info.NodeType)] = info
	persistent.GetDB().SetNodeUpdateInfo(info)
	return nil
}

func (s *Scheduler) DeleteNodeAppUpdateInfos(ctx context.Context, nodeType int) error {
	delete(s.nodeAppUpdateInfos, nodeType)
	return persistent.GetDB().DeleteNodeUpdateInfo(nodeType)
}
