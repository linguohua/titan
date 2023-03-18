package gateway

import (
	"context"
	"crypto/rsa"
	"fmt"
	"io"
	gopath "path"
	"sync/atomic"

	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	bsfetcher "github.com/ipfs/go-fetcher/impl/blockservice"
	ipldformat "github.com/ipfs/go-ipld-format"
	legacy "github.com/ipfs/go-ipld-legacy"
	"github.com/ipfs/go-libipfs/blocks"
	"github.com/ipfs/go-libipfs/files"
	dag "github.com/ipfs/go-merkledag"
	ipfspath "github.com/ipfs/go-path"
	"github.com/ipfs/go-path/resolver"
	unixfile "github.com/ipfs/go-unixfs/file"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipfs/interface-go-ipfs-core/path"
	dagpb "github.com/ipld/go-codec-dagpb"
	ipldprime "github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/schema"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/carfile/store"
	titanrsa "github.com/linguohua/titan/node/rsa"
)

type Gateway struct {
	cs                 *store.CarfileStore
	scheduler          api.Scheduler
	privateKey         *rsa.PrivateKey
	schedulerPublicKey *rsa.PublicKey
}

func NewGateway(cs *store.CarfileStore, scheduler api.Scheduler) *Gateway {
	privateKey, err := titanrsa.GeneratePrivateKey(1024)
	if err != nil {
		log.Panicf("generate private key error:%s", err.Error())
	}

	publicKey := titanrsa.PublicKey2Pem(&privateKey.PublicKey)
	if publicKey == nil {
		log.Panicf("can not convert public key to pem")
	}

	publicPem, err := scheduler.ExchangePublicKey(context.Background(), publicKey)
	if err != nil {
		log.Errorf("exchange public key from scheduler error %s", err.Error())
	}

	schedulerPublicKey, err := titanrsa.Pem2PublicKey(publicPem)
	if err != nil {
		log.Errorf("can not convert pem to public key %s", err.Error())
	}

	gw := &Gateway{cs: cs, scheduler: scheduler, privateKey: privateKey, schedulerPublicKey: schedulerPublicKey}

	return gw
}

func (gw *Gateway) resolvePath(ctx context.Context, p path.Path) (path.Resolved, error) {
	if _, ok := p.(path.Resolved); ok {
		return p.(path.Resolved), nil
	}
	if err := p.IsValid(); err != nil {
		return nil, err
	}

	ipath := ipfspath.Path(p.String())
	if ipath.Segments()[0] != "ipfs" {
		return nil, fmt.Errorf("unsupported path namespace: %s", p.Namespace())
	}

	fetcherFactory := bsfetcher.NewFetcherConfig(blockservice.New(&readOnlyBlockStore{gw}, nil))
	fetcherFactory.PrototypeChooser = dagpb.AddSupportToChooser(func(lnk ipldprime.Link, lnkCtx ipldprime.LinkContext) (ipldprime.NodePrototype, error) {
		if tlnkNd, ok := lnkCtx.LinkNode.(schema.TypedLinkNode); ok {
			return tlnkNd.LinkTargetNodePrototype(), nil
		}
		return basicnode.Prototype.Any, nil
	})
	fetcherFactory.NodeReifier = unixfsnode.Reify

	resolver := resolver.NewBasicResolver(fetcherFactory)
	node, rest, err := resolver.ResolveToLastNode(ctx, ipath)
	if err != nil {
		return nil, fmt.Errorf("ResolveToLastNode error %w", err)
	}

	root, err := cid.Parse(ipath.Segments()[1])
	if err != nil {
		return nil, err
	}

	log.Debugf("node:%s root:%s, rest:%v", node.String(), root.String(), rest)
	return path.NewResolvedPath(ipath, node, root, gopath.Join(rest...)), nil
}

// GetUnixFsNode returns a read-only handle to a file tree referenced by a path.
func (gw *Gateway) getUnixFsNode(ctx context.Context, p path.Resolved) (files.Node, error) {
	ng := &nodeGetter{gw}
	node, err := ng.Get(ctx, p.Cid())
	if err != nil {
		return nil, err
	}

	dagService := dag.NewReadOnlyDagService(&nodeGetter{gw})
	return unixfile.NewUnixfsFile(ctx, dagService, node)
}

func (gw *Gateway) block(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	return gw.cs.Block(c)
}

func (gw *Gateway) carReader(ctx context.Context, c cid.Cid) (io.ReadSeeker, error) {
	return nil, nil
}

type nodeGetter struct {
	gw *Gateway
}

// Get retrieves nodes by CID. Depending on the NodeGetter
// implementation, this may involve fetching the Node from a remote
// machine; consider setting a deadline in the context.
func (ng *nodeGetter) Get(ctx context.Context, c cid.Cid) (ipldformat.Node, error) {
	blk, err := ng.gw.block(ctx, c)
	if err != nil {
		return nil, err
	}

	return legacy.DecodeNode(context.Background(), blk)
}

// GetMany returns a channel of NodeOptions given a set of CIDs.
func (ng *nodeGetter) GetMany(ctx context.Context, cids []cid.Cid) <-chan *ipldformat.NodeOption {
	var count uint64
	ch := make(chan *ipldformat.NodeOption, len(cids))
	for _, cid := range cids {
		c := cid
		go func() {
			node, err := ng.Get(ctx, c)
			ch <- &ipldformat.NodeOption{Node: node, Err: err}

			atomic.AddUint64(&count, 1)

			// TODO: will be ch = nil ?
			if int(count) == len(cids) {
				close(ch)
			}
		}()
	}
	return ch
}

type readOnlyBlockStore struct {
	gw *Gateway
}

func (robs *readOnlyBlockStore) DeleteBlock(context.Context, cid.Cid) error {
	log.Errorf("read only block store, can not delete block")
	return nil
}

func (robs *readOnlyBlockStore) Has(ctx context.Context, c cid.Cid) (bool, error) {
	return robs.gw.cs.HasBlock(c)
}

func (robs *readOnlyBlockStore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	return robs.gw.block(ctx, c)
}

// GetSize returns the CIDs mapped BlockSize
func (robs *readOnlyBlockStore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	log.Errorf("read only block store, can not get size %s", c.String())
	return 0, nil
}

// Put puts a given block to the underlying datastore
func (robs *readOnlyBlockStore) Put(context.Context, blocks.Block) error {
	log.Errorf("read only block store, can not put")
	return nil
}

func (robs *readOnlyBlockStore) PutMany(context.Context, []blocks.Block) error {
	log.Errorf("read only block store, can not put many")
	return nil
}

func (robs *readOnlyBlockStore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	log.Errorf("read only block store, not support allkeys ")
	return nil, nil
}

func (robs *readOnlyBlockStore) HashOnRead(enabled bool) {

}
