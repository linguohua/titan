package etcdcli

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	externalip "github.com/glendc/go-external-ip"
	"github.com/linguohua/titan/api/types"
	"github.com/linguohua/titan/node/modules/dtypes"

	logging "github.com/ipfs/go-log/v2"
	"go.etcd.io/etcd/clientv3"
	"golang.org/x/xerrors"
)

var log = logging.Logger("etcd")

const (
	connectServerTimeoutTime = 5  // Second
	serverKeepAliveDuration  = 10 // Second
)

// Client ...
type Client struct {
	cli *clientv3.Client
	dtypes.ServerID
	dtypes.GetSchedulerConfigFunc
	dtypes.PermissionAdminToken
}

// New new a etcd client
func New(configFunc dtypes.GetSchedulerConfigFunc, serverID dtypes.ServerID, token dtypes.PermissionAdminToken) (*Client, error) {
	cfg, err := configFunc()
	if err != nil {
		return nil, err
	}

	config := clientv3.Config{
		Endpoints:   cfg.EtcdAddresses,
		DialTimeout: connectServerTimeoutTime * time.Second,
	}
	// connect
	cli, err := clientv3.New(config)
	if err != nil {
		return nil, err
	}

	client := &Client{
		cli:                    cli,
		ServerID:               serverID,
		GetSchedulerConfigFunc: configFunc,
		PermissionAdminToken:   token,
	}

	return client, nil
}

// ServerRegister register to etcd , If already register in, return an error
func (c *Client) ServerRegister(ctx context.Context) error {
	cfg, err := c.GetSchedulerConfigFunc()
	if err != nil {
		return err
	}

	index := strings.Index(cfg.ListenAddress, ":")
	port := cfg.ListenAddress[index:]

	log.Debugln("get externalIP...")
	ip, err := externalIP()
	if err != nil {
		return err
	}
	log.Debugf("ip:%s", ip)

	sCfg := &types.SchedulerCfg{
		AreaID:       cfg.AreaID,
		SchedulerURL: ip + port,
		AccessToken:  string(c.PermissionAdminToken),
	}

	serverKey := fmt.Sprintf("/%s/%s", types.RunningNodeType.String(), c.ServerID)

	// get a lease
	lease := clientv3.NewLease(c.cli)
	leaseRsp, err := lease.Grant(ctx, serverKeepAliveDuration)
	if err != nil {
		return xerrors.Errorf("Grant lease err:%s", err.Error())
	}

	leaseID := leaseRsp.ID

	kv := clientv3.NewKV(c.cli)
	// Create transaction
	txn := kv.Txn(ctx)

	value, err := SCMarshal(sCfg)
	if err != nil {
		return xerrors.Errorf("cfg SCMarshal err:%s", err.Error())
	}

	// If the revision of key is equal to 0
	txn.If(clientv3.Compare(clientv3.CreateRevision(serverKey), "=", 0)).
		Then(clientv3.OpPut(serverKey, string(value), clientv3.WithLease(leaseID))).
		Else(clientv3.OpGet(serverKey))

	// Commit transaction
	txnResp, err := txn.Commit()
	if err != nil {
		return err
	}

	// already exists
	if !txnResp.Succeeded {
		return xerrors.Errorf("Server key already exists")
	}

	// KeepAlive
	keepRespChan, err := lease.KeepAlive(context.TODO(), leaseID)
	if err != nil {
		return err
	}
	// lease keepalive response queue capacity only 16 , so need to read it
	go func() {
		for {
			_ = <-keepRespChan
		}
	}()

	return nil
}

// WatchServers watch server login and logout
func (c *Client) WatchServers(nodeType types.NodeType) clientv3.WatchChan {
	prefix := fmt.Sprintf("/%s/", nodeType.String())

	watcher := clientv3.NewWatcher(c.cli)
	watchRespChan := watcher.Watch(context.TODO(), prefix, clientv3.WithPrefix())

	return watchRespChan
}

// ListServers list server
func (c *Client) ListServers(nodeType types.NodeType) (*clientv3.GetResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), connectServerTimeoutTime*time.Second)
	defer cancel()

	serverKeyPrefix := fmt.Sprintf("/%s/", nodeType.String())
	kv := clientv3.NewKV(c.cli)

	return kv.Get(ctx, serverKeyPrefix, clientv3.WithPrefix())
}

// ServerUnRegister UnRegister to etcd
func (c *Client) ServerUnRegister(ctx context.Context) error {
	// ctx, cancel := context.WithTimeout(context.Background(), connectServerTimeoutTime*time.Second)
	// defer cancel()

	kv := clientv3.NewKV(c.cli)

	serverKey := fmt.Sprintf("/%s/%s", types.RunningNodeType.String(), c.ServerID)

	_, err := kv.Delete(ctx, serverKey)
	return err
}

// SCUnmarshal  Unmarshal SchedulerCfg
func SCUnmarshal(v []byte) (*types.SchedulerCfg, error) {
	s := &types.SchedulerCfg{}
	err := json.Unmarshal(v, s)
	if err != nil {
		return nil, err
	}

	return s, nil
}

// SCMarshal  Marshal SchedulerCfg
func SCMarshal(s *types.SchedulerCfg) ([]byte, error) {
	v, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}

	return v, nil
}

func externalIP() (string, error) {
	consensus := externalip.DefaultConsensus(nil, nil)
	// Get your IP,
	// which is never <nil> when err is <nil>.
	ip, err := consensus.ExternalIP()
	if err != nil {
		return "", err
	}

	return ip.String(), nil
}
