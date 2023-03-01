package persistent

import (
	"golang.org/x/xerrors"
)

// InitDB New  DB
func InitDB(url, dbType string) (err error) {
	switch dbType {
	case TypeMySQL():
		err = InitSQL(url)
	default:
		err = xerrors.New("unknown DB type")
	}

	return err
}

// NodeCacheInfo node cache info
type NodeCacheInfo struct {
	DiskUsage     float64
	TotalDownload float64
	TotalUpload   float64
	BlockCount    int
	DeviceID      string
}
