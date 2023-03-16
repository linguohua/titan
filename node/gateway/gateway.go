package gateway

import (
	"context"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-libipfs/blocks"
	"github.com/ipfs/go-libipfs/files"
	iface "github.com/ipfs/interface-go-ipfs-core"
	"github.com/ipfs/interface-go-ipfs-core/path"
)

func (gw *Gateway) resolvePath(ctx context.Context, p path.Path) (path.Resolved, error) {
	return nil, nil
}

// GetUnixFsNode returns a read-only handle to a file tree referenced by a path.
func (gw *Gateway) getUnixFsNode(ctx context.Context, p path.Resolved) (files.Node, error) {
	return nil, nil
}

// LsUnixFsDir returns the list of links in a directory.
func (gw *Gateway) lsUnixFsDir(ctx context.Context, p path.Resolved) (<-chan iface.DirEntry, error) {
	return nil, nil
}

func (gw *Gateway) getBlock(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	return nil, nil
}
