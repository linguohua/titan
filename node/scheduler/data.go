package scheduler

// Data Data
type Data struct {
	cid             string
	caches          map[string]*Cache
	reliability     int
	needReliability int

	Total int
}

func newData(cid string, reliability int, area string) *Data {
	return &Data{
		cid:             cid,
		reliability:     0,
		needReliability: reliability,
		caches:          make(map[string]*Cache),
	}
}

func (d *Data) createCache() {
	c, err := newCache(d.cid)
	if err != nil {
		log.Errorf("new cache err:%v", err.Error())
		return
	}

	d.caches[c.id] = c
	d.saveData()
}

func (d *Data) saveData() {
	// TODO save to db
}
