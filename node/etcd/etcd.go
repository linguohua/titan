package etcd

import (
	"context"
	"fmt"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"go.etcd.io/etcd/clientv3"
	"golang.org/x/xerrors"
)

var log = logging.Logger("etcd")

// Client ...
type Client struct {
	cli *clientv3.Client
}

// New ...
func New() (*Client, error) {
	config := clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"}, // TODO addr
		DialTimeout: 5 * time.Second,
	}
	// connect
	cli, err := clientv3.New(config)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	return &Client{cli: cli}, nil
}

// NodeLogin login to etcd , If already logged in, return an error
func (c Client) NodeLogin(serverID, area string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rsp, err := c.cli.Get(ctx, serverID)
	if err != nil {
		return err
	}

	if rsp.Count > 0 {
		log.Warnf("Key is s %s \n Value is %s \n", rsp.Kvs[0].Key, rsp.Kvs[0].Value)
		return xerrors.New("server already exist")
	}

	lease, err := c.cli.Grant(ctx, 10)
	if err != nil {
		return xerrors.Errorf("Grant lease err:%s", err.Error())
	}
	_, err = c.cli.Put(ctx, serverID, area, clientv3.WithLease(lease.ID))
	if err != nil {
		return xerrors.Errorf("Put err:%s", err.Error())
	}
	_, err = c.cli.KeepAlive(ctx, lease.ID)
	if err != nil {
		return xerrors.Errorf("KeepAlive err:%s", err.Error())
	}

	return nil
}

// WatchNodes watch node login and logout
func (c Client) WatchNodes(prefix string) clientv3.WatchChan {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	watcher := clientv3.NewWatcher(c.cli)
	watchRespChan := watcher.Watch(ctx, prefix, clientv3.WithPrefix())

	return watchRespChan
	// for watchResp := range watchRespChan {
	// 	for _, event := range watchResp.Events {
	// 		string(event.Kv.Key), string(event.Kv.Value)
	// 	}
	// }
}
