package etcd

import (
	"context"
	"fmt"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api"
	"go.etcd.io/etcd/clientv3"
	"golang.org/x/xerrors"
)

var log = logging.Logger("etcd")

const (
	connectServerTimeoutTime = 5  // Second
	nodeExpirationDuration   = 10 // Second
)

// Client ...
type Client struct {
	cli *clientv3.Client
}

// New new a etcd client
func New(addrs []string) (*Client, error) {
	config := clientv3.Config{
		Endpoints:   addrs,
		DialTimeout: connectServerTimeoutTime * time.Second,
	}
	// connect
	cli, err := clientv3.New(config)
	if err != nil {
		return nil, err
	}

	return &Client{cli: cli}, nil
}

// NodeLogin login to etcd , If already logged in, return an error
func (c *Client) NodeLogin(serverID, serverAddr string, nodeType api.NodeType) error {
	ctx, cancel := context.WithTimeout(context.Background(), connectServerTimeoutTime*time.Second)
	defer cancel()

	nodeKey := fmt.Sprintf("/%s/%s", nodeType.String(), serverID)

	rsp, err := c.cli.Get(ctx, nodeKey) // TODO have lock?
	if err != nil {
		return err
	}

	if rsp.Count > 0 {
		return xerrors.New("Server already exist")
	}

	lease, err := c.cli.Grant(ctx, nodeExpirationDuration)
	if err != nil {
		return xerrors.Errorf("Grant lease err:%s", err.Error())
	}
	_, err = c.cli.Put(ctx, nodeKey, serverAddr, clientv3.WithLease(lease.ID))
	if err != nil {
		return xerrors.Errorf("Put err:%s", err.Error())
	}

	_, err = c.cli.KeepAlive(ctx, lease.ID) // TODO keepalive time
	if err != nil {
		return xerrors.Errorf("KeepAlive err:%s", err.Error())
	}

	return nil
}

// WatchNodes watch node login and logout
func (c *Client) WatchNodes(nodeType api.NodeType) clientv3.WatchChan {
	ctx, cancel := context.WithTimeout(context.Background(), connectServerTimeoutTime*time.Second)
	defer cancel()

	prefix := fmt.Sprintf("/%s/", nodeType.String())

	watcher := clientv3.NewWatcher(c.cli)
	watchRespChan := watcher.Watch(ctx, prefix, clientv3.WithPrefix())

	return watchRespChan
}
