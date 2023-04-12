package locator

import (
	"github.com/linguohua/titan/api/types"
	"github.com/linguohua/titan/lib/etcdcli"
	"go.etcd.io/etcd/mvcc/mvccpb"
)

type EtcdClient struct {
	cli              *etcdcli.Client
	schedulerConfigs map[string][]*types.SchedulerCfg
}

func NewEtcdClient(addresses []string) (*EtcdClient, error) {
	etcd, err := etcdcli.New(addresses)
	if err != nil {
		return nil, err
	}

	ec := &EtcdClient{cli: etcd}
	ec.loadSchedulerConfigs()
	ec.watch()

	return ec, nil
}

func (ec *EtcdClient) loadSchedulerConfigs() error {
	resp, err := ec.cli.GetServers(types.NodeScheduler)
	if err != nil {
		return err
	}

	schedulerConfigs := make(map[string][]*types.SchedulerCfg)

	for _, kv := range resp.Kvs {
		config, err := etcdcli.SCUnmarshal(kv.Value)
		if err != nil {
			return err
		}

		configs, ok := schedulerConfigs[config.AreaID]
		if !ok {
			configs = make([]*types.SchedulerCfg, 0)
		}
		configs = append(configs, config)

		schedulerConfigs[config.AreaID] = configs
	}

	ec.schedulerConfigs = schedulerConfigs

	return nil
}

func (ec *EtcdClient) watch() {
	if ec.schedulerConfigs == nil {
		ec.schedulerConfigs = make(map[string][]*types.SchedulerCfg)
	}

	watchChan := ec.cli.WatchServers(types.NodeScheduler)
	for {
		resp := <-watchChan

		for _, event := range resp.Events {
			log.Debugf("watch event: %s", event.Type)
			if event.Type != mvccpb.PUT {
				continue

			}
			config, err := etcdcli.SCUnmarshal(event.Kv.Value)
			if err != nil {
				log.Errorf("etcd watch, unmarshal error:%s", err.Error())
				continue
			}

			configs, ok := ec.schedulerConfigs[config.AreaID]
			if !ok {
				configs = make([]*types.SchedulerCfg, 0)
			}
			configs = append(configs, config)

			ec.schedulerConfigs[config.AreaID] = configs
		}
	}

}

func (ec *EtcdClient) GetSchedulerConfigs(areaID string) ([]*types.SchedulerCfg, error) {
	return ec.schedulerConfigs[areaID], nil
}
