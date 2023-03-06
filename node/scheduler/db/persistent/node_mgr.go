package persistent

import (
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/node/modules/dtypes"
)

type NodeMgrDB struct {
	db *sqlx.DB
}

func NewNodeMgrDB(db *sqlx.DB) *NodeMgrDB {
	return &NodeMgrDB{db}
}

// NodeOffline Set the last online time of the node
func (n *NodeMgrDB) NodeOffline(deviceID string, lastTime time.Time) error {
	info := &api.DeviceInfo{
		DeviceID: deviceID,
		LastTime: lastTime,
	}

	query := fmt.Sprintf("UPDATE %s WHERE SET last_time=:last_time WHERE device_id=:device_id", nodeInfoTable)
	_, err := n.db.NamedExec(query, info)

	return err
}

// NodePrivateKey Get node privateKey
func (n *NodeMgrDB) NodePrivateKey(deviceID string) (string, error) {
	var privateKey string
	query := fmt.Sprintf("SELECT private_key FROM %s WHERE device_id=?", nodeInfoTable)
	if err := n.db.Get(&privateKey, query, deviceID); err != nil {
		return "", err
	}

	return privateKey, nil
}

// LongTimeOfflineNodes get nodes that are offline for a long time
func (n *NodeMgrDB) LongTimeOfflineNodes(hour int) ([]*api.DeviceInfo, error) {
	list := make([]*api.DeviceInfo, 0)

	time := time.Now().Add(-time.Duration(hour) * time.Hour)

	cmd := fmt.Sprintf("SELECT device_id FROM %s WHERE quitted=? AND last_time <= ?", nodeInfoTable)
	if err := n.db.Select(&list, cmd, false, time); err != nil {
		return nil, err
	}

	return list, nil
}

// SetNodesQuit Node quit the titan
func (n *NodeMgrDB) SetNodesQuit(deviceIDs []string) error {
	tx := n.db.MustBegin()

	for _, deviceID := range deviceIDs {
		dCmd := fmt.Sprintf(`UPDATE %s SET quitted=? WHERE device_id=?`, nodeInfoTable)
		tx.MustExec(dCmd, true, deviceID)
	}

	err := tx.Commit()
	if err != nil {
		err = tx.Rollback()
		return err
	}

	return nil
}

// NodePortMapping load node mapping port
func (n *NodeMgrDB) NodePortMapping(deviceID string) (string, error) {
	var privateKey string
	query := fmt.Sprintf("SELECT port_mapping FROM %s WHERE device_id=?", nodeInfoTable)
	if err := n.db.Get(&privateKey, query, deviceID); err != nil {
		return "", err
	}

	return privateKey, nil
}

// SetNodePortMapping Set node mapping port
func (n *NodeMgrDB) SetNodePortMapping(deviceID, port string) error {
	info := api.DeviceInfo{
		DeviceID:    deviceID,
		PortMapping: port,
	}
	// update
	dCmd := fmt.Sprintf(`UPDATE %s SET port_mapping=:port_mapping WHERE device_id=:device_id`, nodeInfoTable)
	_, err := n.db.NamedExec(dCmd, info)
	return err
}

// InitValidateResultInfos init validator result infos
func (n *NodeMgrDB) InitValidateResultInfos(infos []*api.ValidateResult) error {
	tx := n.db.MustBegin()
	for _, info := range infos {
		query := "INSERT INTO validate_result (round_id, device_id, validator_id, status, start_time) VALUES (?, ?, ?, ?, ?)"
		tx.MustExec(query, info.RoundID, info.DeviceID, info.ValidatorID, info.Status, info.StartTime)
	}

	err := tx.Commit()
	if err != nil {
		err = tx.Rollback()
		return err
	}

	return nil
}

// SetValidateTimeoutOfNodes Set validator timeout of nodes
func (n *NodeMgrDB) SetValidateTimeoutOfNodes(roundID int64, deviceIDs []string) error {
	tx := n.db.MustBegin()

	updateCachesCmd := `UPDATE validate_result SET status=?,end_time=NOW() WHERE round_id=? AND device_id in (?)`
	query, args, err := sqlx.In(updateCachesCmd, api.ValidateStatusTimeOut, roundID, deviceIDs)
	if err != nil {
		return err
	}

	// cache info
	query = n.db.Rebind(query)
	tx.MustExec(query, args...)

	err = tx.Commit()
	if err != nil {
		err = tx.Rollback()
		return err
	}

	return nil
}

// UpdateValidateResultInfo Update validator info
func (n *NodeMgrDB) UpdateValidateResultInfo(info *api.ValidateResult) error {
	if info.Status == api.ValidateStatusSuccess {
		query := "UPDATE validate_result SET block_number=:block_number,status=:status, duration=:duration, bandwidth=:bandwidth, end_time=NOW() WHERE round_id=:round_id AND device_id=:device_id"
		_, err := n.db.NamedExec(query, info)
		return err
	}

	query := "UPDATE validate_result SET status=:status, end_time=NOW() WHERE round_id=:round_id AND device_id=:device_id"
	_, err := n.db.NamedExec(query, info)
	return err
}

// ValidateResultInfos Get validator result infos
func (n *NodeMgrDB) ValidateResultInfos(startTime, endTime time.Time, pageNumber, pageSize int) (*api.SummeryValidateResult, error) {
	res := new(api.SummeryValidateResult)
	var infos []api.ValidateResult
	query := fmt.Sprintf("SELECT *, (duration/1e3 * bandwidth) AS `upload_traffic` FROM validate_result WHERE start_time between ? and ? order by id asc  LIMIT ?,? ")

	if pageSize > loadValidateInfoMaxCount {
		pageSize = loadValidateInfoMaxCount
	}

	err := n.db.Select(&infos, query, startTime, endTime, (pageNumber-1)*pageSize, pageSize)
	if err != nil {
		return nil, err
	}

	res.ValidateResultInfos = infos

	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM validate_result WHERE start_time between ? and ? ")
	var count int
	err = n.db.Get(&count, countQuery, startTime, endTime)
	if err != nil {
		return nil, err
	}

	res.Total = count

	return res, nil
}

func (n *NodeMgrDB) SetNodeUpdateInfo(info *api.NodeAppUpdateInfo) error {
	sqlString := fmt.Sprintf(`INSERT INTO %s (node_type, app_name, version, hash, download_url) VALUES (:node_type, :app_name, :version, :hash, :download_url) ON DUPLICATE KEY UPDATE app_name=:app_name, version=:version, hash=:hash, download_url=:download_url`, nodeUpdateInfo)
	_, err := n.db.NamedExec(sqlString, info)
	return err
}

func (n *NodeMgrDB) GetNodeUpdateInfos() (map[int]*api.NodeAppUpdateInfo, error) {
	query := fmt.Sprintf(`SELECT * FROM %s`, nodeUpdateInfo)

	var out []*api.NodeAppUpdateInfo
	if err := n.db.Select(&out, query); err != nil {
		return nil, err
	}

	ret := make(map[int]*api.NodeAppUpdateInfo)
	for _, info := range out {
		ret[info.NodeType] = info
	}
	return ret, nil
}

func (n *NodeMgrDB) DeleteNodeUpdateInfo(nodeType int) error {
	deleteString := fmt.Sprintf(`DELETE FROM %s WHERE node_type=?`, nodeUpdateInfo)
	_, err := n.db.Exec(deleteString, nodeType)
	return err
}

// IsNilErr Is NilErr
func IsNilErr(err error) bool {
	return err.Error() == errNotFind
}

func (n *NodeMgrDB) GetNodes(cursor int, count int) ([]*api.DeviceInfo, int64, error) {
	var total int64
	countSQL := fmt.Sprintf("SELECT count(*) FROM %s", nodeInfoTable)
	err := n.db.Get(&total, countSQL)
	if err != nil {
		return nil, 0, err
	}

	queryString := fmt.Sprintf(`SELECT device_id, is_online FROM %s order by device_id asc limit ?,?`, nodeInfoTable)

	if count > loadNodeInfoMaxCount {
		count = loadNodeInfoMaxCount
	}

	var out []*api.DeviceInfo
	err = n.db.Select(&out, queryString, cursor, count)
	if err != nil {
		return nil, 0, err
	}

	return out, total, nil
}

// ResetValidators validator list
func (n *NodeMgrDB) ResetValidators(deviceIDs []string, serverID dtypes.ServerID) error {
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
func (n *NodeMgrDB) GetValidatorsWithList(serverID dtypes.ServerID) ([]string, error) {
	sQuery := fmt.Sprintf(`SELECT device_id FROM %s WHERE server_id=?`, validatorsTable)

	var out []string
	err := n.db.Select(&out, sQuery, serverID)
	if err != nil {
		return nil, err
	}

	return out, nil
}

// UpdateNodeInfo update node info
func (n *NodeMgrDB) UpdateNodeInfo(info *api.DeviceInfo) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (device_id, profit,
				private_key, last_time, quitted) 
				VALUES (:device_id, :profit,
				:private_key, :last_time, :quitted) 
				ON DUPLICATE KEY UPDATE device_id=:device_id, last_time=:last_time, quitted=:quitted`, nodeInfoTable)

	_, err := n.db.NamedExec(query, info)
	return err
}

// UpdateNodeOnlineTime update node online time and last time
func (n *NodeMgrDB) UpdateNodeOnlineTime(deviceID string, onlineTime int) error {
	info := &api.DeviceInfo{
		DeviceID:   deviceID,
		OnlineTime: onlineTime,
	}

	query := fmt.Sprintf(`UPDATE %s SET last_time=NOW(),online_time=:online_time WHERE device_id=:device_id`, nodeInfoTable)
	// update
	_, err := n.db.NamedExec(query, info)
	return err
}

// ListDeviceIDs list devices
func (n *NodeMgrDB) ListDeviceIDs(cursor int, count int) ([]string, int64, error) {
	var total int64

	cQuery := fmt.Sprintf(`SELECT count(*) FROM %s`, nodeInfoTable)
	err := n.db.Get(&total, cQuery)
	if err != nil {
		return nil, 0, err
	}

	sQuery := fmt.Sprintf(`SELECT device_id FROM %s order by device_id asc limit ?,?`, nodeInfoTable)

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

// LoadNodeInfo load node info
func (n *NodeMgrDB) LoadNodeInfo(deviceID string) (*api.DeviceInfo, error) {
	query := fmt.Sprintf(`SELECT * FROM %s WHERE device_id=?`, nodeInfoTable)

	var out api.DeviceInfo
	err := n.db.Select(&out, query, deviceID)
	if err != nil {
		return nil, err
	}

	return &out, nil
}

// SetNodesToVerifyingList validator list
func (n *NodeMgrDB) SetNodesToVerifyingList(deviceIDs []string, serverID dtypes.ServerID) error {
	tx := n.db.MustBegin()
	// clean old validators
	dQuery := fmt.Sprintf(`DELETE FROM %s WHERE server_id=? `, nodeVerifyingTable)
	tx.MustExec(dQuery, serverID)

	for _, deviceID := range deviceIDs {
		iQuery := fmt.Sprintf(`INSERT INTO %s (device_id, server_id) VALUES (?, ?)`, nodeVerifyingTable)
		tx.MustExec(iQuery, deviceID, serverID)
	}

	err := tx.Commit()
	if err != nil {
		err = tx.Rollback()
	}

	return err
}

// GetNodesWithVerifyingList load validators
func (n *NodeMgrDB) GetNodesWithVerifyingList(serverID dtypes.ServerID) ([]string, error) {
	sQuery := fmt.Sprintf(`SELECT device_id FROM %s WHERE server_id=?`, nodeVerifyingTable)

	var out []string
	err := n.db.Select(&out, sQuery, serverID)
	if err != nil {
		return nil, err
	}

	return out, nil
}

// CountVerifyingNode ...
func (n *NodeMgrDB) CountVerifyingNode(serverID dtypes.ServerID) (int64, error) {
	var count int64
	cmd := fmt.Sprintf("SELECT count(device_id) FROM %s WHERE server_id=?", nodeVerifyingTable)
	err := n.db.Get(&count, cmd, serverID)
	return count, err
}

// RemoveValidatedWithList ...
func (n *NodeMgrDB) RemoveValidatedWithList(deviceID string, serverID dtypes.ServerID) error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE server_id=? AND device_id=?`, nodeVerifyingTable)
	_, err := n.db.Exec(query, serverID, deviceID)
	return err
}

// RemoveVerifyingList ...
func (n *NodeMgrDB) RemoveVerifyingList(serverID dtypes.ServerID) error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE server_id=?`, nodeVerifyingTable)
	_, err := n.db.Exec(query, serverID)
	return err
}
