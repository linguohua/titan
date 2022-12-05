package api

import (
	"context"
	"time"
)

// Scheduler Scheduler node
type Scheduler interface {
	Common
	Web

	// call by command
	GetOnlineDeviceIDs(ctx context.Context, nodeType NodeTypeName) ([]string, error)                    //perm:read
	ElectionValidators(ctx context.Context) error                                                       //perm:admin
	Validate(ctx context.Context) error                                                                 //perm:admin
	QueryCacheStatWithNode(ctx context.Context, deviceID string) ([]CacheStat, error)                   //perm:read
	QueryCachingBlocksWithNode(ctx context.Context, deviceID string) (CachingBlockList, error)          //perm:read
	CacheCarfile(ctx context.Context, cid string, reliability, hour int) error                          //perm:admin
	RemoveCarfile(ctx context.Context, carfileID string) error                                          //perm:admin
	RemoveCache(ctx context.Context, carfileID, cacheID string) error                                   //perm:admin
	ShowDataTask(ctx context.Context, cid string) (CacheDataInfo, error)                                //perm:read
	ListDatas(ctx context.Context, page int) (DataListInfo, error)                                      //perm:read
	ShowDataTasks(ctx context.Context) ([]CacheDataInfo, error)                                         //perm:read
	RegisterNode(ctx context.Context, t NodeType) (NodeRegisterInfo, error)                             //perm:read
	DeleteBlockRecords(ctx context.Context, deviceID string, cids []string) (map[string]string, error)  //perm:admin
	CacheContinue(ctx context.Context, cid, cacheID string) error                                       //perm:admin
	ValidateSwitch(ctx context.Context, open bool) error                                                //perm:admin
	GetValidationInfo(ctx context.Context) (ValidationInfo, error)                                      //perm:admin
	ListEvents(ctx context.Context, page int) (EventListInfo, error)                                    //perm:read
	ResetCacheExpiredTime(ctx context.Context, carfileCid, cacheID string, expiredTime time.Time) error //perm:admin
	ReplenishCacheExpiredTime(ctx context.Context, carfileCid, cacheID string, hour int) error          //perm:admin
	NodeExit(ctx context.Context, device string) error                                                  //perm:admin

	// call by locator
	LocatorConnect(ctx context.Context, edgePort int, areaID, locatorID, locatorToken string) error //perm:write

	// call by node
	// node send result when user download block complete
	NodeDownloadBlockResult(ctx context.Context, result NodeBlockDownloadResult) error                            //perm:write
	EdgeNodeConnect(ctx context.Context, rpcURL, downloadSrvURL string) error                                     //perm:write
	ValidateBlockResult(ctx context.Context, validateResults ValidateResults) error                               //perm:write
	CandidateNodeConnect(ctx context.Context, rpcURL, downloadSrvURL string) error                                //perm:write
	CacheResult(ctx context.Context, deviceID string, resultInfo CacheResultInfo) (string, error)                 //perm:write
	GetCandidateDownloadInfoWithBlocks(ctx context.Context, cids []string) (map[string]DownloadInfoResult, error) //perm:write
	GetExternalIP(ctx context.Context) (string, error)                                                            //perm:write
	GetPublicKey(ctx context.Context) (string, error)                                                             //perm:write

	// call by user
	GetDownloadInfosWithBlocks(ctx context.Context, cids []string, publicKey string) (map[string][]DownloadInfoResult, error) //perm:read
	GetDownloadInfoWithBlocks(ctx context.Context, cids []string, publicKey string) (map[string]DownloadInfoResult, error)    //perm:read
	GetDownloadInfoWithBlock(ctx context.Context, cid, publicKey string) (DownloadInfoResult, error)                          //perm:read
	GetDevicesInfo(ctx context.Context, deviceID string) (DevicesInfo, error)                                                 //perm:read
	GetDownloadInfo(ctx context.Context, deviceID string) ([]*BlockDownloadInfo, error)                                       //perm:read

	// user send result when user download block complete
	UserDownloadBlockResults(ctx context.Context, results []UserBlockDownloadResult) error //perm:read
}

// DataListInfo Data List Info
type DataListInfo struct {
	Page       int
	TotalPage  int
	Cids       int
	CacheInfos []*CacheDataInfo
}

// EventInfo Event Info
type EventInfo struct {
	ID         int
	CID        string    `db:"cid"`
	CacheID    string    `db:"cache_id"`
	DeviceID   string    `db:"device_id"`
	User       string    `db:"user"`
	Event      string    `db:"event"`
	Msg        string    `db:"msg"`
	CreateTime time.Time `db:"created_time"`
}

// EventListInfo Event List Info
type EventListInfo struct {
	Page      int
	TotalPage int
	Count     int
	EventList []EventInfo
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

	CarFileHash string
	CacheID     string
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

	CarfileHash string
	DataTimeout time.Duration
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

type NodeBlockDownloadResult struct {
	// serial number
	SN int64
	// scheduler signature
	Sign          []byte
	DownloadSpeed int64
	BlockSize     int
	ClientIP      string
	Result        bool
	FailedReason  string
}

type DownloadServerAccessAuth struct {
	DeviceID   string
	URL        string
	PrivateKey string
}

type UserBlockDownloadResult struct {
	// serial number
	SN int64
	// user signature
	Sign   []byte
	Result bool
}

type DownloadInfoResult struct {
	URL      string
	Sign     string
	SN       int64
	SignTime int64
	TimeOut  int
	DeviceID string `json:"-"`
}
