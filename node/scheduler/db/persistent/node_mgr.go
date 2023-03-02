package persistent

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/linguohua/titan/api"
	"time"
)

type NodeMgrDB struct {
	db *sqlx.DB
}

func NewNodeMgrDB(db *sqlx.DB) *NodeMgrDB {
	return &NodeMgrDB{db}
}

// SetNodeInfo Set node info
func (n *NodeMgrDB) SetNodeInfo(deviceID string, info *api.DeviceInfo) error {
	info.DeviceID = deviceID

	var count int64
	cmd := "SELECT count(device_id) FROM node WHERE device_id=?"
	err := n.db.Get(&count, cmd, deviceID)
	if err != nil {
		return err
	}

	if count == 0 {
		_, err = n.db.NamedExec(`INSERT INTO node (device_id, last_time, geo, node_type,  address, private_key)
                VALUES (:device_id, :last_time, :geo, :node_type,  :address, :private_key)`, info)
		return err
	}

	// update
	_, err = n.db.NamedExec(`UPDATE node SET last_time=:last_time,geo=:geo,address=:address,quitted=:quitted WHERE device_id=:device_id`, info)
	return err
}

// NodeOffline Set the last online time of the node
func (n *NodeMgrDB) NodeOffline(deviceID string, lastTime time.Time) error {
	info := &api.DeviceInfo{
		DeviceID: deviceID,
		LastTime: lastTime,
	}

	_, err := n.db.NamedExec(`UPDATE node SET last_time=:last_time WHERE device_id=:device_id`, info)

	return err
}

// NodePrivateKey Get node privateKey
func (n *NodeMgrDB) NodePrivateKey(deviceID string) (string, error) {
	var privateKey string
	query := "SELECT private_key FROM node WHERE device_id=?"
	if err := n.db.Get(&privateKey, query, deviceID); err != nil {
		return "", err
	}

	return privateKey, nil
}

// LongTimeOfflineNodes get nodes that are offline for a long time
func (n *NodeMgrDB) LongTimeOfflineNodes(hour int) ([]*api.DeviceInfo, error) {
	list := make([]*api.DeviceInfo, 0)

	time := time.Now().Add(-time.Duration(hour) * time.Hour)

	cmd := "SELECT device_id FROM node WHERE quitted=? AND last_time <= ?"
	if err := n.db.Select(&list, cmd, false, time); err != nil {
		return nil, err
	}

	return list, nil
}

// SetNodesQuit Node quit the titan
func (n *NodeMgrDB) SetNodesQuit(deviceIDs []string) error {
	tx := n.db.MustBegin()

	for _, deviceID := range deviceIDs {
		dCmd := `UPDATE node SET quitted=? WHERE device_id=?`
		tx.MustExec(dCmd, true, deviceID)
	}

	err := tx.Commit()
	if err != nil {
		err = tx.Rollback()
		return err
	}

	return nil
}

// SetNodePort Set node port
func (n *NodeMgrDB) SetNodePort(deviceID, port string) error {
	info := api.DeviceInfo{
		DeviceID: deviceID,
		Port:     port,
	}
	// update
	_, err := n.db.NamedExec(`UPDATE node SET port=:port WHERE device_id=:device_id`, info)
	return err
}

// InitValidateResultInfos init validator result infos
func (n *NodeMgrDB) InitValidateResultInfos(infos []*api.ValidateResult) error {
	tx := n.db.MustBegin()
	for _, info := range infos {
		query := "INSERT INTO validate_result (round_id, device_id, validator_id, status, start_time) VALUES (?, ?, ?, ?, ?, ?)"
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
	countSQL := "SELECT count(*) FROM node"
	err := n.db.Get(&total, countSQL)
	if err != nil {
		return nil, 0, err
	}

	queryString := "SELECT device_id, is_online FROM node order by device_id asc limit ?,?"

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
