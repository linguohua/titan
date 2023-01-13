package carfilestore

import (
	"testing"

	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

func TestCarfileOperation(t *testing.T) {
	path := "C:\\Users\\aaa\\.titancandidate-1\\carfilestore\\carfiles"
	carfileHash := "1220bb412ebb87c52929c955893f96cf8e6820e92d75e70d8b965a90b2bc9d5d8f4b"
	// path = filepath.Join(path, carfileHash)
	carfileTable := carfileTable{path: path}
	blocksHash, err := carfileTable.readAllBlocksHashOfCarfile(carfileHash)
	if err != nil {
		t.Errorf("readAllBlocksHashOfCarfile err:%s", err.Error())
		return
	}

	for index, blockHash := range blocksHash {
		multihash, err := mh.FromHexString(blockHash)
		if err != nil {
			t.Errorf("FromHexString err:%s", err.Error())
			return
		}

		cid := cid.NewCidV0(multihash)

		t.Logf("%d %s", index, cid.String())
	}

}
