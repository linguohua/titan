package api

import (
	"time"
)

type OpenRPCDocument map[string]interface{}

type Base struct {
	ID        uint      `gorm:"primarykey"`
	CreatedAt time.Time `json:"created_at" gorm:"comment:'创建时间';type:timestamp;"`
	UpdatedAt time.Time `json:"updated_at" gorm:"comment:'更新时间';type:timestamp;"`
}

// DeviceInfo Info
type DeviceInfo struct {
	Base
	NodeID        string   `json:"node_id" form:"nodeId" gorm:"column:node_id;comment:;" db:"node_id"`
	UserID        string   `json:"user_id" form:"userId" gorm:"column:user_id;comment:;"`
	SnCode        string   `json:"sn_code" form:"snCode" gorm:"column:sn_code;comment:;"`
	NodeType      NodeType `json:"node_type"`
	DeviceName    string   `json:"device_name" form:"deviceName" gorm:"column:device_name;comment:;"`
	Operator      string   `json:"operator" form:"operator" gorm:"column:operator;comment:;"`
	NetworkType   string   `json:"network_type" form:"networkType" gorm:"column:network_type;comment:;"`
	SystemVersion string   `json:"system_version" form:"systemVersion" gorm:"column:system_version;comment:;"`
	ProductType   string   `json:"product_type" form:"productType" gorm:"column:product_type;comment:;"`
	NetworkInfo   string   `json:"network_info" form:"networkInfo" gorm:"column:network_info;comment:;"`
	ExternalIP    string   `json:"external_ip" form:"externalIp" gorm:"column:external_ip;comment:;"`
	InternalIP    string   `json:"internal_ip" form:"internalIp" gorm:"column:internal_ip;comment:;"`
	IPLocation    string   `json:"ip_location" form:"ipLocation" gorm:"column:ip_location;comment:;"`
	MacLocation   string   `json:"mac_location" form:"macLocation" gorm:"column:mac_location;comment:;"`
	NatType       string   `json:"nat_type" form:"natType" gorm:"column:nat_type;comment:;"`
	Upnp          string   `json:"upnp" form:"upnp" gorm:"column:upnp;comment:;"`
	PkgLossRatio  float64  `json:"pkg_loss_ratio" form:"pkgLossRatio" gorm:"column:pkg_loss_ratio;comment:;"`
	Latency       float64  `json:"latency" form:"latency" gorm:"column:latency;comment:;"`
	CPUUsage      float64  `json:"cpu_usage" form:"cpuUsage" gorm:"column:cpu_usage;comment:;"`
	CPUCores      int      `json:"cpu_cores" form:"cpuCores" gorm:"column:cpu_cores;comment:;"`
	MemoryUsage   float64  `json:"memory_usage" form:"memoryUsage" gorm:"column:memory_usage;comment:;"`
	Memory        float64  `json:"memory" form:"memory" gorm:"column:memory;comment:;"`
	DiskUsage     float64  `json:"disk_usage" form:"diskUsage" gorm:"column:disk_usage;comment:;"`
	DiskSpace     float64  `json:"disk_space" form:"diskSpace" gorm:"column:disk_space;comment:;"`
	DiskType      string   `json:"disk_type" form:"diskType" gorm:"column:disk_type;comment:;"`
	WorkStatus    string   `json:"work_status" form:"workStatus" gorm:"column:work_status;comment:;"`
	IoSystem      string   `json:"io_system" form:"ioSystem" gorm:"column:io_system;comment:;"`
	NatRatio      float64  `json:"nat_ratio" form:"nat_ratio" gorm:"column:nat_ratio;comment:;"`
	BandwidthUp   float64  `json:"bandwidth_up"`
	BandwidthDown float64  `json:"bandwidth_down"`
	Blocks        int      `json:"blocks" form:"blockCount" gorm:"column:blocks;comment:;"`
	Latitude      float64  `json:"latitude"`
	Longitude     float64  `json:"longitude"`

	DeviceStatus     string    `json:"device_status" form:"deviceStatus" gorm:"column:device_status;comment:;" db:"device_status"`
	OnlineTime       int       `json:"online_time" form:"OnlineTime" db:"online_time"`
	CumulativeProfit float64   `json:"profit" db:"profit"`
	DownloadTraffic  float64   `json:"download_traffic" db:"download_traffic"`
	UploadTraffic    float64   `json:"upload_traffic" db:"upload_traffic"`
	DownloadBlocks   int       `json:"download_blocks" form:"downloadCount" gorm:"column:download_blocks;comment:;" db:"download_blocks"`
	PortMapping      string    `db:"port_mapping"`
	LastTime         time.Time `db:"last_time"`
	PrivateKeyStr    string    `db:"private_key"`
	Quitted          bool      `db:"quitted"`
}

type BlockDownloadInfo struct {
	ID           string    `json:"-"`
	NodeID       string    `json:"node_id" db:"node_id"`
	BlockCID     string    `json:"block_cid" db:"block_cid"`
	CarfileCID   string    `json:"carfile_cid" db:"carfile_cid"`
	BlockSize    int       `json:"block_size" db:"block_size"`
	Speed        int64     `json:"speed" db:"speed"`
	Reward       int64     `json:"reward" db:"reward"`
	Status       int       `json:"status" db:"status"`
	FailedReason string    `json:"failed_reason" db:"failed_reason"`
	ClientIP     string    `json:"client_ip" db:"client_ip"`
	CreatedTime  time.Time `json:"created_time" db:"created_time"`
	CompleteTime time.Time `json:"complete_time" db:"complete_time"`
}
