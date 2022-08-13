package api

import (
	"context"
)

type Scheduler interface {
	Common

	EdgeNodeConnect(context.Context, string) error //perm:read

	CacheData(context.Context, []string, string) error //perm:read

	GetDeviceIDs(context.Context) ([]string, error) //perm:read

	FindNodeWithData(context.Context, string, string) (string, error) //perm:read

	GetDownloadURLWithData(context.Context, string, string) (string, error) //perm:read

	CandidateNodeConnect(context.Context, string) error //perm:read

	CacheResult(context.Context, string, string, bool) error //perm:read

	GetCacheTag(context.Context, string, string) (string, error) //perm:read

	GetIndexInfo(context.Context, IndexRequest) (IndexPageRes, error) //perm:read

	Retrieval(context.Context, IndexPageSearch) (RetrievalPageRes, error) //perm:read

	GetDevicesInfo(context.Context, DevicesSearch) (DevicesInfoPage, error) //perm:read

	GetDevicesCount(context.Context, DevicesSearch) (DeviceType, error) //perm:read

	GetDeviceDiagnosisDaily(context.Context, IncomeDailySearch) (IncomeDailyRes, error) //perm:read

	GetDeviceDiagnosisHour(context.Context, IncomeDailySearch) (HourDailyRes, error) //perm:read

	SaveDailyInfo(context.Context, IncomeDaily) error //perm:read

	ElectionValidators(ctx context.Context) error //perm:read

	SpotCheck(ctx context.Context) error //perm:read
}
