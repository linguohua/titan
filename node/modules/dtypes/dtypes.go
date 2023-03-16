package dtypes

import (
	"github.com/ipfs/go-datastore"
	"github.com/linguohua/titan/node/config"
)

// MetadataDS stores metadata.
type MetadataDS datastore.Batching

// GeoDBPath the location of a geo database
type GeoDBPath string

// DatabaseAddress the DSN to connect to the database
type DatabaseAddress string

// PermissionWriteToken token with write permission
type PermissionWriteToken []byte

// PermissionAdminToken token with admin permission
type PermissionAdminToken []byte

// SessionCallbackFunc callback function when the node connects
type SessionCallbackFunc func(string, string)

// LocatorUUID the locator unique identifier
type LocatorUUID string

// NodeID candidate or edge unique identifier
type NodeID string

// InternalIP local network address
type InternalIP string

// ScheduleSecretKey used for connecting to the scheduler
type ScheduleSecretKey string

type CarfileStorePath string

// ServerID server id
type ServerID string

// SetSchedulerConfigFunc is a function which is used to
// sets the scheduler config.
type SetSchedulerConfigFunc func(cfg config.SchedulerCfg) error

// GetSchedulerConfigFunc is a function which is used to
// get the sealing config.
type GetSchedulerConfigFunc func() (config.SchedulerCfg, error)
