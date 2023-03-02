package persistent

import (
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/linguohua/titan/api"
)

const (
	nodeTable           = "node"
	waitingCarfileTable = "carfile_waiting"
	validatorsTable     = "validators"
)

// UpdateNodeInfo update node info
func (n *NodeMgrDB) UpdateNodeInfo(info *api.DeviceInfo) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (device_id, device_name, operator, network_type, system_version, product_type, network_info, external_ip, internal_ip, ip_location, mac_location, nat_type,
			    upnp, pkg_loss_ratio, latency, cpu_usage, cpu_cores, memory_usage, memory, disk_usage, disk_space, disk_type, work_status, io_system, nat_ratio, cumulative_profit,
				bandwidth_up, bandwidth_down, latitude, longitude, private_key, last_time, quitted) 
				VALUES (:device_id, :device_name, :operator, :network_type, :system_version, :product_type, :network_info, :external_ip, :internal_ip, :ip_location, :mac_location, :nat_type,
				:upnp, :pkg_loss_ratio, :latency, :cpu_usage, :cpu_cores, :memory_usage, :memory, :disk_usage, :disk_space, :disk_type, :work_status, :io_system, :nat_ratio, :cumulative_profit,
				:bandwidth_up, :bandwidth_down, :latitude, :longitude, :private_key, :last_time, :quitted) 
				ON DUPLICATE KEY UPDATE device_id=:device_id, last_time=:last_time, quitted=:quitted, disk_usage=:disk_usage, memory_usage=:memory_usage, cpu_usage=:cpu_usage, 
				cpu_usage=:cpu_usage, nat_type=:nat_type, external_ip=:external_ip, system_version:=system_version`, nodeTable)

	_, err := n.db.NamedExec(query, info)
	return err
}

// UpdateNodeOnlineTime update node online time and last time
func (n *NodeMgrDB) UpdateNodeOnlineTime(deviceID string, onlineTime int) error {
	info := &api.DeviceInfo{
		DeviceID:   deviceID,
		OnlineTime: onlineTime,
	}

	query := fmt.Sprintf(`UPDATE %s SET last_time=NOW(),online_time=:online_time WHERE device_id=:device_id`, nodeTable)
	// update
	_, err := n.db.NamedExec(query, info)
	return err
}

// ListDeviceIDs list devices
func (n *NodeMgrDB) ListDeviceIDs(cursor int, count int) ([]string, int64, error) {
	var total int64

	cQuery := fmt.Sprintf(`SELECT count(*) FROM %s`, nodeTable)
	err := n.db.Get(&total, cQuery)
	if err != nil {
		return nil, 0, err
	}

	sQuery := fmt.Sprintf(`SELECT device_id FROM %s order by device_id asc limit ?,?`, nodeTable)

	if count > loadNodeInfoMaxCount {
		count = loadNodeInfoMaxCount
	}

	var out []string
	err = n.db.Select(&out, sQuery, cursor, count)
	if err != nil {
		return nil, 0, err
	}

	return out, total, nil
}

// LoadDeviceInfo load device info
func (n *NodeMgrDB) LoadDeviceInfo(deviceID string) (*api.DeviceInfo, error) {
	query := fmt.Sprintf(`SELECT * FROM %s WHERE device_id=?`, nodeTable)

	var out api.DeviceInfo
	err := n.db.Select(&out, query, deviceID)
	if err != nil {
		return nil, err
	}

	return &out, nil
}

// UpdateNodeCacheInfo update node info if cache
func (n *NodeMgrDB) UpdateNodeCacheInfo(deviceID string, diskUsage float64, blockCount int) error {
	info := &api.DeviceInfo{
		DeviceID:   deviceID,
		DiskUsage:  diskUsage,
		BlockCount: blockCount,
	}
	query := fmt.Sprintf(`UPDATE %s SET disk_usage=:disk_usage, block_count=:block_count WHERE device_id=:device_id`, nodeTable)
	// update
	_, err := n.db.NamedExec(query, info)
	return err
}

// PushCarfileToWaitList waiting data list
func (n *NodeMgrDB) PushCarfileToWaitList(info *api.CacheCarfileInfo) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (carfile_hash, carfile_cid, replicas, device_id, expiration_time, server_id) 
				VALUES (:carfile_hash, :carfile_cid, :replicas, :device_id, :expiration_time, :server_id) 
				ON DUPLICATE KEY UPDATE carfile_hash=:carfile_hash, carfile_cid=:carfile_cid, replicas=:replicas, device_id=:device_id, 
				expiration_time=:expiration_time, server_id=:server_id`, waitingCarfileTable)

	_, err := n.db.NamedExec(query, info)
	return err
}

// LoadWaitCarfiles load
func (n *NodeMgrDB) LoadWaitCarfiles(serverID string) ([]*api.CacheCarfileInfo, error) {
	sQuery := fmt.Sprintf(`SELECT * FROM %s WHERE server_id=? order by id asc limit ?,?`, waitingCarfileTable)

	var out []*api.CacheCarfileInfo
	err := n.db.Select(&out, sQuery, serverID, 0, 1)
	if err != nil {
		return nil, err
	}

	return out, nil
}

// RemoveWaitCarfile remove
func (n *NodeMgrDB) RemoveWaitCarfile(id string) error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE id=?`, waitingCarfileTable)
	_, err := n.db.Exec(query, id)
	return err
}

// ResetValidators validator list
func (n *NodeMgrDB) ResetValidators(deviceIDs []string, serverID string) error {
	tx := n.db.MustBegin()
	// clean old validators
	dQuery := fmt.Sprintf(`DELETE FROM %s WHERE server_id=? `, validatorsTable)
	tx.MustExec(dQuery, serverID)

	for _, deviceID := range deviceIDs {
		iQuery := fmt.Sprintf(`INSERT INTO %s (device_id, server_id) VALUES (?, ?)`, validatorsTable)
		tx.MustExec(iQuery, deviceID, serverID)
	}

	err := tx.Commit()
	if err != nil {
		err = tx.Rollback()
	}

	return err
}

// GetValidatorsWithList load validators
func (n *NodeMgrDB) GetValidatorsWithList(serverID string) ([]string, error) {
	sQuery := fmt.Sprintf(`SELECT device_id FROM %s WHERE server_id=?`, validatorsTable)

	var out []string
	err := n.db.Select(&out, sQuery, serverID)
	if err != nil {
		return nil, err
	}

	return out, nil
}
