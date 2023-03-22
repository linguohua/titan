package gateway

import (
	"context"
	"crypto/rsa"
	"fmt"
	"io"
	gopath "path"

	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	bsfetcher "github.com/ipfs/go-fetcher/impl/blockservice"
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
	carStore           *store.CarfileStore
	scheduler          api.Scheduler
	privateKey         *rsa.PrivateKey
	schedulerPublicKey *rsa.PublicKey
}

func NewGateway(cs *store.CarfileStore, scheduler api.Scheduler, privateKey *rsa.PrivateKey) *Gateway {
	pem, err := scheduler.PublicKey(context.Background())
	if err != nil {
		log.Panicf("get public key from scheduler error %s", err.Error())
	}

	schedulerPublicKey, err := titanrsa.Pem2PublicKey([]byte(pem))
	if err != nil {
		log.Panicf("can not convert pem to public key %s", err.Error())
	}

	gw := &Gateway{carStore: cs, scheduler: scheduler, privateKey: privateKey, schedulerPublicKey: schedulerPublicKey}

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

	log.Debugf("resolve path node:%s root:%s, rest:%v", node.String(), root.String(), rest)
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
	return gw.carStore.Block(c)
}

func (gw *Gateway) carReader(ctx context.Context, c cid.Cid) (io.ReadSeekCloser, error) {
	return gw.carStore.CarReader(c)
}
