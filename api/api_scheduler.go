package api

import (
	"context"
)

type Scheduler interface {
	Common

	// call by command
	CacheData(context.Context, []string, string) ([]string, error)                //perm:read
	DeleteData(context.Context, string, []string) (map[string]string, error)      //perm:read
	GetOnlineDeviceIDs(context.Context, NodeTypeName) ([]string, error)           //perm:read
	ElectionValidators(context.Context) error                                     //perm:read
	Validate(context.Context) error                                               //perm:read
	InitNodeDeviceIDs(context.Context) error                                      //perm:read
	QueryCacheStatWithNode(context.Context, string) ([]CacheStat, error)          //perm:read
	QueryCachingBlocksWithNode(context.Context, string) (CachingBlockList, error) //perm:read

	// call by node
	EdgeNodeConnect(context.Context, string) error                                 //perm:read
	DeleteDataRecord(context.Context, string, []string) (map[string]string, error) //perm:read
	ValidateDataResult(context.Context, VerifyResults) error                       //perm:read
	CandidateNodeConnect(context.Context, string) error                            //perm:read
	CacheResult(context.Context, string, CacheResultInfo) (string, error)          //perm:read
	GetCacheTag(context.Context, string, string) (string, error)                   //perm:read

	// call by user
	FindNodeWithData(context.Context, string, string) (string, error)                   //perm:read
	GetDownloadURLWithData(context.Context, string, string) (string, error)             //perm:read
	GetIndexInfo(context.Context, IndexRequest) (IndexPageRes, error)                   //perm:read
	Retrieval(context.Context, IndexPageSearch) (RetrievalPageRes, error)               //perm:read
	GetDevicesInfo(context.Context, DevicesSearch) (DevicesInfoPage, error)             //perm:read
	GetDevicesCount(context.Context, DevicesSearch) (DeviceType, error)                 //perm:read
	GetDeviceDiagnosisDaily(context.Context, IncomeDailySearch) (IncomeDailyRes, error) //perm:read
	GetDeviceDiagnosisHour(context.Context, IncomeDailySearch) (HourDailyRes, error)    //perm:read
	SaveDailyInfo(context.Context, IncomeDaily) error                                   //perm:read
}

// CacheResultInfo cache data result info
type CacheResultInfo struct {
	Cid           string
	IsOK          bool
	Msg           string
	From          string
	DownloadSpeed float32
}
