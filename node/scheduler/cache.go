package scheduler

import (
	"fmt"

	"github.com/linguohua/titan/node/scheduler/db/cache"
)

// Cache Cache
type Cache struct {
	id     string
	blocks []*BlockInfo
	status int

	reliability int
}

// BlockInfo BlockInfo
type BlockInfo struct {
	cid        string
	deviceName string
	deviceArea string
	deviceIP   string
	isSuccess  bool

	reliability int
}

func newCacheID(cid string) (string, error) {
	fid, err := cache.GetDB().IncrCacheID(cid)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s_%v", cid, fid), nil
}

func newCache(cid string) (*Cache, error) {
	id, err := newCacheID(cid)
	if err != nil {
		return nil, err
	}

	return &Cache{
		reliability: 0,
		status:      0,
		blocks:      make([]*BlockInfo, 0),
		id:          id,
	}, nil
}

func (c *Cache) doCache() {
}
