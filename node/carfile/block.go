package carfile

import (
	"context"

	blocks "github.com/ipfs/go-block-format"
	format "github.com/ipfs/go-ipld-format"
	legacy "github.com/ipfs/go-ipld-legacy"
)

func resolveLinks(blk blocks.Block) ([]*format.Link, error) {
	ctx := context.Background()

	node, err := legacy.DecodeNode(ctx, blk)
	if err != nil {
		log.Error("resolveLinks err:%v", err)
		return make([]*format.Link, 0), err
	}

	return node.Links(), nil
}
