package dtypes

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

// ExitCallbackFunc callback function when the node exits
type ExitCallbackFunc func([]string)

// LocatorUUID the locator unique identifier
type LocatorUUID string

// NodeID candidate or edge unique identifier
type NodeID string

// InternalIP local network address
type InternalIP string

// ScheduleSecretKey used for connecting to the scheduler
type ScheduleSecretKey string

type CarfileStoreType string

type CarfileStorePath string

// ServerID server id
type ServerID string
