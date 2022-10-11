package scheduler

// Cache Cache
type Cache struct {
	id     string
	blocks []*BlockInfo
	status int
	// 可靠性 PS blocks可靠性决定
	reliability int
}

// BlockInfo BlockInfo
type BlockInfo struct {
	id         string
	cid        string
	deviceName string
	deviceArea string
	deviceIP   string
	isSuccess  bool
	// 可靠性 PS 由设备可靠性决定
	reliability int
}
