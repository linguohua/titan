package api

import (
	"context"
	"time"
)

// Scheduler Scheduler node
type Scheduler interface {
	Common

	// call by command
	// CacheBlocks(ctx context.Context, cids []string, deviceID string) ([]string, error)                 //perm:admin
	// DeleteBlocks(ctx context.Context, deviceID string, cids []string) (map[string]string, error)       //perm:admin
	GetOnlineDeviceIDs(ctx context.Context, nodeType NodeTypeName) ([]string, error)                   //perm:read
	ElectionValidators(ctx context.Context) error                                                      //perm:admin
	Validate(ctx context.Context) error                                                                //perm:admin
	QueryCacheStatWithNode(ctx context.Context, deviceID string) ([]CacheStat, error)                  //perm:read
	QueryCachingBlocksWithNode(ctx context.Context, deviceID string) (CachingBlockList, error)         //perm:read
	CacheCarfile(ctx context.Context, cid string, reliability int) error                               //perm:admin
	RemoveCarfile(ctx context.Context, carfileID string) error                                         //perm:admin
	RemoveCache(ctx context.Context, carfileID, cacheID string) error                                  //perm:admin
	ShowDataTask(ctx context.Context, cid string) (CacheDataInfo, error)                               //perm:read
	ListDatas(ctx context.Context, page int) (DataListInfo, error)                                     //perm:read
	ShowDataTasks(ctx context.Context) ([]CacheDataInfo, error)                                        //perm:read
	RegisterNode(ctx context.Context, t NodeType) (NodeRegisterInfo, error)                            //perm:read
	DeleteBlockRecords(ctx context.Context, deviceID string, cids []string) (map[string]string, error) //perm:admin
	CacheContinue(ctx context.Context, cid, cacheID string) error                                      //perm:admin
	ValidateSwitch(ctx context.Context, open bool) error                                               //perm:admin

	// call by locator
	LocatorConnect(ctx context.Context, edgePort int, areaID, locatorID, locatorToken string) error //perm:write

	// call by node
	DownloadBlockResult(ctx context.Context, stat DownloadBlockStat) error                         //perm:write
	EdgeNodeConnect(ctx context.Context, edgePort int) (externalIP string, err error)              //perm:write
	ValidateBlockResult(ctx context.Context, validateResults ValidateResults) error                //perm:write
	CandidateNodeConnect(ctx context.Context, edgePort int) (externalIP string, err error)         //perm:write
	CacheResult(ctx context.Context, deviceID string, resultInfo CacheResultInfo) (string, error)  //perm:write
	UpdateDownloadServerAccessAuth(ctx context.Context, accessAuth DownloadServerAccessAuth) error //perm:write

	// call by user
	GetDownloadInfosWithBlocks(ctx context.Context, cids []string) (map[string][]DownloadInfo, error) //perm:read
	GetDownloadInfoWithBlocks(ctx context.Context, cids []string) (map[string]DownloadInfo, error)    //perm:read
	GetDownloadInfoWithBlock(ctx context.Context, cid string) (DownloadInfo, error)                   //perm:read
	GetDevicesInfo(ctx context.Context, deviceID string) (DevicesInfo, error)                         //perm:read
	StateNetwork(ctx context.Context) (StateNetwork, error)                                           //perm:read
	GetDownloadInfo(ctx context.Context, deviceID string) ([]*BlockDownloadInfo, error)               //perm:read
}

// DataListInfo Data List Info
type DataListInfo struct {
	Page      int
	TotalPage int
	Cids      int
	CidList   []string
}

// NodeRegisterInfo Node Register Info
type NodeRegisterInfo struct {
	ID         int
	DeviceID   string `db:"device_id"`
	Secret     string `db:"secret"`
	CreateTime string `db:"create_time"`
	NodeType   int    `db:"node_type"`
}

// CacheResultInfo cache data result info
type CacheResultInfo struct {
	DeviceID      string
	Cid           string
	IsOK          bool
	Msg           string
	From          string
	DownloadSpeed float32
	// links cid
	Links     []string
	BlockSize int
	LinksSize uint64

	CarFileCid string
	CacheID    string
	// Fid        int
}

// CacheDataInfo Cache Data Info
type CacheDataInfo struct {
	CarfileCid      string
	NeedReliability int // 预期可靠性
	CurReliability  int // 当前可靠性
	TotalSize       int // 总大小
	Blocks          int // 总block个数
	Nodes           int
	ExpiredTime     time.Time
	CacheInfos      []CacheInfo
}

// CacheInfo Cache Info
type CacheInfo struct {
	CacheID     string
	Status      int // cache 状态 1:创建 2:失败 3:成功
	DoneSize    int // 已完成大小
	DoneBlocks  int // 已完成block
	Nodes       int
	ExpiredTime time.Time

	// BloackInfo []BloackInfo
}

// BloackInfo Bloack Info
// type BloackInfo struct {
// 	Cid      string
// 	Status   int    // cache 状态 1:创建 2:失败 3:成功
// 	DeviceID string // 在哪个设备上
// 	Size     int
// }

type DownloadBlockStat struct {
	Cid           string
	DeviceID      string
	BlockSize     int
	DownloadSpeed int64
	ClientIP      string
}

type DownloadServerAccessAuth struct {
	DeviceID    string
	URL         string
	SecurityKey string
}
