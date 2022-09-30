package scheduler

// Cache Cache
type Cache struct {
	id string

	blocks []*BlockInfo
}

// BlockInfo BlockInfo
type BlockInfo struct {
	cid string

	deviceName string
	deviceArea string
	deviceIP   string
}
