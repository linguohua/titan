package httpserver

import (
	"context"
	"crypto/rsa"
	"fmt"
	gopath "path"

	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	bsfetcher "github.com/ipfs/go-fetcher/impl/blockservice"
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
)

type HttpServer struct {
	asset              Asset
	scheduler          api.Scheduler
	privateKey         *rsa.PrivateKey
	schedulerPublicKey *rsa.PublicKey
}

func NewHttpServer(asset Asset, scheduler api.Scheduler, privateKey *rsa.PrivateKey) *HttpServer {
	hs := &HttpServer{asset: asset, scheduler: scheduler, privateKey: privateKey}

	return hs
}

func (hs *HttpServer) SetSchedulerPublicKey(publicKey *rsa.PublicKey) {
	hs.schedulerPublicKey = publicKey
}

func (hs *HttpServer) resolvePath(ctx context.Context, p path.Path, asset cid.Cid) (path.Resolved, error) {
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

	fetcherFactory := bsfetcher.NewFetcherConfig(blockservice.New(&readOnlyBlockStore{hs, asset}, nil))
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

	return path.NewResolvedPath(ipath, node, root, gopath.Join(rest...)), nil
}

// GetUnixFsNode returns a read-only handle to a file tree referenced by a path.
func (hs *HttpServer) getUnixFsNode(ctx context.Context, p path.Resolved, root cid.Cid) (files.Node, error) {
	ng := &nodeGetter{hs, root}
	node, err := ng.Get(ctx, p.Cid())
	if err != nil {
		return nil, err
	}

	dagService := dag.NewReadOnlyDagService(&nodeGetter{hs, root})
	return unixfile.NewUnixfsFile(ctx, dagService, node)
}
