package locator

import "github.com/linguohua/titan/api/types"

type storage interface {
	GetSchedulerConfigs(areaID string) ([]*types.SchedulerCfg, error)
}
