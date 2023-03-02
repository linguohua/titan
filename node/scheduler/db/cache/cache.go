package cache

import (
	"context"
	"github.com/go-redis/redis/v8"
	"time"
)

const (
	// CarFileCountField SystemBaseInfo Field
	CarFileCountField = "CarfileCount"
	// DownloadCountField SystemBaseInfo Field
	DownloadCountField = "DownloadCount"
	// NextElectionTimeField SystemBaseInfo Field
	NextElectionTimeField = "NextElectionTime"

	// blockCountField DeviceInfo Field
	blockCountField = "BlockCount"
	// totalDownloadField DeviceInfo Field
	totalDownloadField = "TotalDownload"
	// totalUploadField DeviceInfo Field
	totalUploadField = "TotalUpload"
	// diskUsageField DeviceInfo Field
	diskUsageField = "DiskUsage"
)

// NewCacheDB New nodeMgrCache DB
//func NewCacheDB(url, dbType string) error {
//	var err error
//
//	switch dbType {
//	case TypeRedis():
//		err = InitRedis(url)
//	default:
//		err = xerrors.New("unknown CacheDB type")
//	}
//
//	return err
//}

const connectionTimeout = 5 * time.Second

func NewRedis(url string) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:      url,
		Dialer:    nil,
		OnConnect: nil,
	})

	ctx, cancel := context.WithTimeout(context.Background(), connectionTimeout)
	defer cancel()
	_, err := client.Ping(ctx).Result()

	return client, err
}

// DataTask data cache task
type DataTask struct {
	CarfileHash string
	DeviceID    string
}

// NodeCacheInfo node cache info
type NodeCacheInfo struct {
	DiskUsage     float64
	TotalDownload float64
	TotalUpload   float64
	BlockCount    int
	DeviceID      string
}

type DownloadBlockRecord struct {
	SN            int64  `redis:"SN"`
	ID            string `redis:"ID"`
	Cid           string `redis:"Cid"`
	UserPublicKey string `redis:"UserPublicKey"`
	NodeStatus    int    `redis:"NodeStatus"`
	UserStatus    int    `redis:"UserStatus"`
	SignTime      int64  `redis:"SignTime"`
	Timeout       int    `redis:"Timeout"`
}
