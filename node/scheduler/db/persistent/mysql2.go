package persistent

import (
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/linguohua/titan/api"
)

const (
	nodeTable = "node"
)

// UpdateNodeInfo update node info
func UpdateNodeInfo(info *api.DeviceInfo) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (device_id, device_name, operator, network_type, system_version, product_ype, network_info, external_ip, internal_ip, ip_location, mac_location, nat_type,
			    upnp, pkg_loss_ratio, latency, cpu_usage, cpu_cores, memory_usage, memory, disk_usage, disk_space, disk_type, work_status, io_system, nat_ratio, cumulative_profit,
				bandwidth_up, bandwidth_down, latitude, longitude, private_key, last_time, quitted) 
				VALUES (:device_id, :device_name, :operator, :network_type, :system_version, :product_ype, :network_info, :external_ip, :internal_ip, :ip_location, :mac_location, :nat_type,
				:upnp, :pkg_loss_ratio, :latency, :cpu_usage, :cpu_cores, :memory_usage, :memory, :disk_usage, :disk_space, :disk_type, :work_status, :io_system, :nat_ratio, :cumulative_profit,
				:bandwidth_up, :bandwidth_down, :latitude, :longitude, :private_key, :last_time, :quitted) 
				ON DUPLICATE KEY UPDATE device_id=:device_id, last_time=:last_time, quitted=:quitted, disk_usage=:disk_usage, memory_usage=:memory_usage, cpu_usage=:cpu_usage, 
				cpu_usage=:cpu_usage, nat_type=:nat_type, external_ip=:external_ip, system_version:=system_version`, nodeTable)

	_, err := mysqlCli.NamedExec(query, info)
	return err
}

// UpdateNodeOnlineTime update node online time and last time
func UpdateNodeOnlineTime(deviceID string, onlineTime int) error {
	info := &api.DeviceInfo{
		DeviceID:   deviceID,
		OnlineTime: onlineTime,
	}
	// update
	_, err := mysqlCli.NamedExec(`UPDATE node SET last_time=NOW(),online_time=:online_time WHERE device_id=:device_id`, info)
	return err
}

func GetDeviceIDs(cursor int, count int) ([]string, int64, error) {
	var total int64
	countSQL := "SELECT count(*) FROM node"
	err := mysqlCli.Get(&total, countSQL)
	if err != nil {
		return nil, 0, err
	}

	queryString := "SELECT device_id FROM node order by device_id asc limit ?,?"

	if count > loadNodeInfoMaxCount {
		count = loadNodeInfoMaxCount
	}

	var out []string
	err = mysqlCli.Select(&out, queryString, cursor, count)
	if err != nil {
		return nil, 0, err
	}

	return out, total, nil
}

func GetDeviceInfo(deviceID string) (*api.DeviceInfo, error) {
	queryString := "SELECT * FROM node WHERE device_id=?"

	var out api.DeviceInfo
	err := mysqlCli.Select(&out, queryString, deviceID)
	if err != nil {
		return nil, err
	}

	return &out, nil
}

func UpdateNodeCacheInfo(deviceID string, diskUsage float64, blockCount int) error {
	info := &api.DeviceInfo{
		DeviceID:   deviceID,
		DiskUsage:  diskUsage,
		BlockCount: blockCount,
	}
	// update
	_, err := mysqlCli.NamedExec(`UPDATE node SET disk_usage=:disk_usage, block_count=:block_count WHERE device_id=:device_id`, info)
	return err
}
