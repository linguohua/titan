package etcd

import (
	"context"
	"fmt"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/linguohua/titan/api"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
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

// ServerLogin login to etcd , If already logged in, return an error
func (c *Client) ServerLogin(serverID, serverAddr string, nodeType api.NodeType) error {
	ctx, cancel := context.WithTimeout(context.Background(), connectServerTimeoutTime*time.Second)
	defer cancel()

	serverKey := fmt.Sprintf("/%s/%s", nodeType.String(), serverID)

	// get a leaseRsp
	leaseRsp, err := c.cli.Grant(ctx, nodeExpirationDuration)
	if err != nil {
		return xerrors.Errorf("Grant lease err:%s", err.Error())
	}

	leaseID := leaseRsp.ID

	kv := clientv3.NewKV(c.cli)
	// Create transaction
	txn := kv.Txn(ctx)

	// If the revision of key is equal to 0
	txn.If(clientv3.Compare(clientv3.CreateRevision(serverKey), "=", 0)).
		Then(clientv3.OpPut(serverKey, serverAddr, clientv3.WithLease(leaseID))).
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
	keepRespChan, err := c.cli.KeepAlive(ctx, leaseID)
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
func (c *Client) WatchServers(nodeType api.NodeType) {
	ctx, cancel := context.WithTimeout(context.Background(), connectServerTimeoutTime*time.Second)
	defer cancel()

	prefix := fmt.Sprintf("/%s/", nodeType.String())

	watcher := clientv3.NewWatcher(c.cli)
	watchRespChan := watcher.Watch(ctx, prefix, clientv3.WithPrefix())

	for watchResp := range watchRespChan {
		for _, event := range watchResp.Events {
			switch event.Type {
			case mvccpb.PUT:
				fmt.Println("Update:", string(event.Kv.Key), " ,Value:", string(event.Kv.Value), " ,Revision:",
					event.Kv.CreateRevision, event.Kv.ModRevision)
			case mvccpb.DELETE:
				fmt.Println("Delete:", string(event.Kv.Key), " ,Revision:", event.Kv.ModRevision)
			}
		}
	}

	return
}
