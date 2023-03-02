package persistent

import (
	"fmt"
	"github.com/jmoiron/sqlx"
)

func NewDB(dns string) (*sqlx.DB, error) {
	dns = fmt.Sprintf("%s?parseTime=true&loc=Local", dns)

	client, err := sqlx.Open("mysql", dns)
	if err != nil {
		return nil, err
	}

	if err = client.Ping(); err != nil {
		return nil, err
	}

	return client, nil
}

// NodeCacheInfo node cache info
type NodeCacheInfo struct {
	DiskUsage     float64
	TotalDownload float64
	TotalUpload   float64
	BlockCount    int
	DeviceID      string
}
