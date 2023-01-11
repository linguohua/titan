package carfile

import (
	"context"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
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

func newBlock(cidStr string, data []byte) (blocks.Block, error) {
	target, err := cid.Decode(cidStr)
	if err != nil {
		return nil, err
	}

	basicBlock, err := blocks.NewBlockWithCid(data, target)
	if err != nil {
		return nil, err
	}

	return basicBlock, nil
}
