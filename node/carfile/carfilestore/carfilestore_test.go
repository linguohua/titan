package carfilestore

import (
	"math/rand"
	"testing"

	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

func TestCarfileOperation(t *testing.T) {
	test1(t)

}

func test1(t *testing.T) {
	path := "C:\\Users\\aaa\\.titancandidate\\carfilestore\\carfiles"
	carfileHash := "12204e4406e4f7809bd6c9999a3e46a62910195b148679003dbaca634919f52c194d"
	// path = filepath.Join(path, carfileHash)
	carfileTable := carfileTable{path: path}

	blockCount, err := carfileTable.blockCountOfCarfile(carfileHash)
	if err != nil {
		t.Errorf("GetBlocksOfCarfile, BlockCountOfCarfile error:%s, carfileHash:%s", err.Error(), carfileHash)
		return
	}

	randomSeed := int64(1673869705992765000)
	indexs := make([]int, 0)
	indexMap := make(map[int]struct{})
	r := rand.New(rand.NewSource(randomSeed))

	for i := 0; i < 4460; i++ {
		index := r.Intn(blockCount)

		if _, ok := indexMap[index]; !ok {
			indexs = append(indexs, index)
			indexMap[index] = struct{}{}
		}
	}

	t.Logf("index:%v", indexs)

	blocksHash, err := carfileTable.readBlocksHashesWith(carfileHash, indexs)
	if err != nil {
		t.Errorf("readBlocksHashOfCarfile error:%s, carfileHash:%s", err.Error(), carfileHash)
	}

	ret := make(map[int]string)
	for index, blockHash := range blocksHash {
		multihash, err := mh.FromHexString(blockHash)
		if err != nil {
			t.Errorf("err:%s", err.Error())
			continue
		}
		cid := cid.NewCidV1(cid.Raw, multihash)

		pos := indexs[index]
		ret[pos] = cid.String()
	}

	t.Logf("cids:%v", ret)
}
