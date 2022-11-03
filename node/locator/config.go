package locator

import (
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/config"
	"github.com/linguohua/titan/node/repo"
)

type localCfg struct {
	lr  repo.LockedRepo
	cfg *config.Location
}

func newLocalCfg(lr repo.LockedRepo) *localCfg {
	cfg, err := lr.Config()
	if err != nil {
		log.Panic(err)
	}

	locationCfg := cfg.(*config.Location)
	return &localCfg{lr: lr, cfg: locationCfg}
}

// func (cfg *localCfg) getAccessPoints(areaID, deviceID string) ([]string, error) {
// 	if cfg.cfg.AccessPoints == nil {
// 		log.Errorf("cfg.cfg.AccessPoints == nil ")
// 		return []string{}, nil
// 	}

// 	accessPoint, ok := cfg.cfg.AccessPoints[areaID]
// 	if !ok {
// 		log.Infof("Access point %s not exist", areaID)
// 		return []string{}, nil
// 	}

// 	serverURLs := make([]string, 0, len(accessPoint.SchedulerCfgs))
// 	for _, serverCfg := range accessPoint.SchedulerCfgs {
// 		serverURLs = append(serverURLs, serverCfg.URL)
// 	}
// 	return serverURLs, nil
// }

func (cfg *localCfg) addAccessPoints(areaID string, schedulerURL string, weight int) error {
	err := cfg.lr.SetConfig(func(raw interface{}) {
		rcfg, ok := raw.(*config.Location)
		if !ok {
			log.Errorf("expected Location config, %v", raw)
			return
		}

		if rcfg.AccessPoints == nil {
			rcfg.AccessPoints = make(map[string]config.AccessPoint)
		}

		accessPoint, ok := rcfg.AccessPoints[areaID]
		if !ok {
			accessPoint = config.AccessPoint{AreaID: areaID, SchedulerCfgs: make([]config.SchedulerCfg, 0)}
		}

		var isExist = false
		for _, serverCfg := range accessPoint.SchedulerCfgs {
			if serverCfg.URL == schedulerURL {
				isExist = true
				break
			}
		}

		if !isExist {
			serverCfg := config.SchedulerCfg{URL: schedulerURL, Weight: weight}
			accessPoint.SchedulerCfgs = append(accessPoint.SchedulerCfgs, serverCfg)
		}
		rcfg.AccessPoints[areaID] = accessPoint

		cfg.cfg = rcfg

	})

	return err
}

func (cfg *localCfg) removeAccessPoints(areaID string) error {
	err := cfg.lr.SetConfig(func(raw interface{}) {
		rcfg, ok := raw.(*config.Location)
		if !ok {
			log.Errorf("expected Location config, %v", raw)
			return
		}

		if rcfg.AccessPoints == nil {
			return
		}

		_, ok = rcfg.AccessPoints[areaID]
		if !ok {
			return
		}
		delete(rcfg.AccessPoints, areaID)

		cfg.cfg = rcfg
	})
	return err
}

func (cfg *localCfg) listAccessPoints() (areaIDs []string, err error) {
	if cfg.cfg.AccessPoints == nil {
		return []string{}, nil
	}

	for k := range cfg.cfg.AccessPoints {
		areaIDs = append(areaIDs, k)
	}
	return areaIDs, nil
}

func (cfg *localCfg) getAccessPoint(areaID string) (api.AccessPoint, error) {
	if cfg.cfg.AccessPoints == nil {
		return api.AccessPoint{}, nil
	}

	accessPoint, ok := cfg.cfg.AccessPoints[areaID]
	if !ok {
		return api.AccessPoint{}, nil
	}

	result := api.AccessPoint{AreaID: accessPoint.AreaID, SchedulerInfos: make([]api.SchedulerInfo, 0, len(accessPoint.SchedulerCfgs))}

	for _, serverCfg := range accessPoint.SchedulerCfgs {
		scfg := api.SchedulerInfo{URL: serverCfg.URL, Weight: serverCfg.Weight}
		result.SchedulerInfos = append(result.SchedulerInfos, scfg)
	}

	return result, nil
}

func (cfg *localCfg) isAccessPointExist(areaID, schedulerURL string) (bool, error) {
	if cfg.cfg.AccessPoints == nil {
		return false, nil
	}

	accessPoint, ok := cfg.cfg.AccessPoints[areaID]
	if !ok {
		return false, nil
	}

	for _, serverCfg := range accessPoint.SchedulerCfgs {
		if serverCfg.URL == schedulerURL {
			return true, nil
		}
	}

	return false, nil
}
