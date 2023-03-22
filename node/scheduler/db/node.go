package db

import (
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/types"
	"github.com/linguohua/titan/node/modules/dtypes"
	"golang.org/x/xerrors"
)

// ListTimeoutNodes get nodes that are offline for a long time
func (n *SqlDB) ListTimeoutNodes(timeoutHour int) ([]*types.NodeInfo, error) {
	list := make([]*types.NodeInfo, 0)

	time := time.Now().Add(-time.Duration(timeoutHour) * time.Hour)

	cmd := fmt.Sprintf("SELECT node_id FROM %s WHERE quitted=? AND last_time <= ?", nodeInfoTable)
	if err := n.db.Select(&list, cmd, false, time); err != nil {
		return nil, err
	}

	return list, nil
}

// SetNodesQuit Node quit the titan
func (n *SqlDB) SetNodesQuit(nodeIDs []string) error {
	updateCachesCmd := fmt.Sprintf(`UPDATE %s SET quitted=? WHERE node_id in (?)`, nodeInfoTable)
	query, args, err := sqlx.In(updateCachesCmd, true, nodeIDs)
	if err != nil {
		return err
	}

	// cache info
	query = n.db.Rebind(query)
	_, err = n.db.Exec(query, args...)

	return err
}

// GetPortMappingOfNode get mapping port of node
func (n *SqlDB) GetPortMappingOfNode(nodeID string) (string, error) {
	var port string
	query := fmt.Sprintf("SELECT port_mapping FROM %s WHERE node_id=?", nodeInfoTable)
	if err := n.db.Get(&port, query, nodeID); err != nil {
		return "", err
	}

	return port, nil
}

// SetPortMappingOfNode Set node mapping port
func (n *SqlDB) SetPortMappingOfNode(nodeID, port string) error {
	info := types.NodeInfo{
		NodeID:      nodeID,
		PortMapping: port,
	}
	// update
	dCmd := fmt.Sprintf(`UPDATE %s SET port_mapping=:port_mapping WHERE node_id=:node_id`, nodeInfoTable)
	_, err := n.db.NamedExec(dCmd, info)
	return err
}

// InsertValidatedResultInfos Insert validator result infos
func (n *SqlDB) InsertValidatedResultInfos(infos []*types.ValidatedResultInfo) error {
	tx, err := n.db.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, info := range infos {
		query := fmt.Sprintf(`INSERT INTO %s (round_id, node_id, validator_id, status, start_time) VALUES (?, ?, ?, ?, ?)`, validateResultTable)
		_, err = tx.Exec(query, info.RoundID, info.NodeID, info.ValidatorID, info.Status, info.StartTime)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

// UpdateValidatedResultInfo Update validator info
func (n *SqlDB) UpdateValidatedResultInfo(info *types.ValidatedResultInfo) error {
	if info.Status == types.ValidateStatusSuccess {
		query := fmt.Sprintf(`UPDATE %s SET block_number=:block_number,status=:status, duration=:duration, bandwidth=:bandwidth, end_time=NOW() WHERE round_id=:round_id AND node_id=:node_id`, validateResultTable)
		_, err := n.db.NamedExec(query, info)
		return err
	}

	query := fmt.Sprintf(`UPDATE %s SET status=:status, end_time=NOW() WHERE round_id=:round_id AND node_id=:node_id`, validateResultTable)
	_, err := n.db.NamedExec(query, info)
	return err
}

// SetValidatedResultTimeout set timeout status to validated result
func (n *SqlDB) SetValidatedResultTimeout(roundID string) error {
	query := fmt.Sprintf(`UPDATE %s SET status=?, end_time=NOW() WHERE round_id=? AND status=?`, validateResultTable)
	_, err := n.db.Exec(query, types.ValidateStatusTimeOut, roundID, types.ValidateStatusCreate)
	return err
}

// ListValidatedResultInfos Get validator result infos
func (n *SqlDB) ListValidatedResultInfos(startTime, endTime time.Time, pageNumber, pageSize int) (*types.ListValidatedResultRsp, error) {
	res := new(types.ListValidatedResultRsp)
	var infos []types.ValidatedResultInfo
	query := fmt.Sprintf("SELECT *, (duration/1e3 * bandwidth) AS `upload_traffic` FROM %s WHERE start_time between ? and ? order by id asc  LIMIT ?,? ", validateResultTable)

	if pageSize > loadValidateInfosLimit {
		pageSize = loadValidateInfosLimit
	}

	err := n.db.Select(&infos, query, startTime, endTime, (pageNumber-1)*pageSize, pageSize)
	if err != nil {
		return nil, err
	}

	res.ValidatedResultInfos = infos

	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE start_time between ? and ?", validateResultTable)
	var count int
	err = n.db.Get(&count, countQuery, startTime, endTime)
	if err != nil {
		return nil, err
	}

	res.Total = count

	return res, nil
}

func (n *SqlDB) SetEdgeUpdateInfo(info *api.EdgeUpdateInfo) error {
	sqlString := fmt.Sprintf(`INSERT INTO %s (node_type, app_name, version, hash, download_url) VALUES (:node_type, :app_name, :version, :hash, :download_url) ON DUPLICATE KEY UPDATE app_name=:app_name, version=:version, hash=:hash, download_url=:download_url`, edgeUpdateTable)
	_, err := n.db.NamedExec(sqlString, info)
	return err
}

func (n *SqlDB) GetEdgeUpdateInfos() (map[int]*api.EdgeUpdateInfo, error) {
	query := fmt.Sprintf(`SELECT * FROM %s`, edgeUpdateTable)

	var out []*api.EdgeUpdateInfo
	if err := n.db.Select(&out, query); err != nil {
		return nil, err
	}

	ret := make(map[int]*api.EdgeUpdateInfo)
	for _, info := range out {
		ret[info.NodeType] = info
	}
	return ret, nil
}

func (n *SqlDB) DeleteEdgeUpdateInfo(nodeType int) error {
	deleteString := fmt.Sprintf(`DELETE FROM %s WHERE node_type=?`, edgeUpdateTable)
	_, err := n.db.Exec(deleteString, nodeType)
	return err
}

func (n *SqlDB) ListNodes(cursor int, count int) ([]*types.NodeInfo, int64, error) {
	var total int64
	countSQL := fmt.Sprintf("SELECT count(*) FROM %s", nodeInfoTable)
	err := n.db.Get(&total, countSQL)
	if err != nil {
		return nil, 0, err
	}

	queryString := fmt.Sprintf(`SELECT node_id FROM %s order by node_id asc limit ?,?`, nodeInfoTable)

	if count > loadNodeInfosLimit {
		count = loadNodeInfosLimit
	}

	var out []*types.NodeInfo
	err = n.db.Select(&out, queryString, cursor, count)
	if err != nil {
		return nil, 0, err
	}

	return out, total, nil
}

// ResetValidators validator list
func (n *SqlDB) ResetValidators(nodeIDs []string, serverID dtypes.ServerID) error {
	tx, err := n.db.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// clean old validators
	dQuery := fmt.Sprintf(`DELETE FROM %s WHERE server_id=? `, validatorsTable)
	_, err = tx.Exec(dQuery, serverID)
	if err != nil {
		return err
	}

	for _, nodeID := range nodeIDs {
		iQuery := fmt.Sprintf(`INSERT INTO %s (node_id, server_id) VALUES (?, ?)`, validatorsTable)
		_, err = tx.Exec(iQuery, nodeID, serverID)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

// ListValidators load validators
func (n *SqlDB) ListValidators(serverID dtypes.ServerID) ([]string, error) {
	sQuery := fmt.Sprintf(`SELECT node_id FROM %s WHERE server_id=?`, validatorsTable)

	var out []string
	err := n.db.Select(&out, sQuery, serverID)
	if err != nil {
		return nil, err
	}

	return out, nil
}

// UpdateValidatorInfo reset scheduler server id for validator
func (n *SqlDB) UpdateValidatorInfo(serverID dtypes.ServerID, nodeID string) error {
	var count int64
	sQuery := fmt.Sprintf("SELECT count(node_id) FROM %s WHERE node_id=?", validatorsTable)
	err := n.db.Get(&count, sQuery, nodeID)
	if err != nil {
		return err
	}

	if count < 1 {
		return nil
	}

	uQuery := fmt.Sprintf(`UPDATE %s SET server_id=? WHERE node_id=?`, validatorsTable)
	_, err = n.db.Exec(uQuery, serverID, nodeID)

	return err
}

// UpdateNodeInfo update node info
func (n *SqlDB) UpdateNodeInfo(info *types.NodeInfo) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (node_id, mac_location, product_type, cpu_cores, memory, node_name, latitude, disk_usage,
			    longitude, disk_type, io_system, system_version, nat_type, disk_space, bandwidth_up, bandwidth_down, blocks) 
				VALUES (:node_id, :mac_location, :product_type, :cpu_cores, :memory, :node_name, :latitude, :disk_usage,
				:longitude, :disk_type, :io_system, :system_version, :nat_type, :disk_space, :bandwidth_up, :bandwidth_down, :blocks) 
				ON DUPLICATE KEY UPDATE node_id=:node_id, last_time=:last_time, quitted=:quitted, disk_usage=:disk_usage, blocks=:blocks`, nodeInfoTable)

	_, err := n.db.NamedExec(query, info)
	return err
}

// UpdateNodeOnlineTime update node online time and last time
func (n *SqlDB) UpdateNodeOnlineTime(nodeID string, onlineTime int) error {
	info := &types.NodeInfo{
		NodeID:     nodeID,
		OnlineTime: onlineTime,
	}

	query := fmt.Sprintf(`UPDATE %s SET last_time=NOW(),online_time=:online_time WHERE node_id=:node_id`, nodeInfoTable)
	// update
	_, err := n.db.NamedExec(query, info)
	return err
}

// InsertNode Insert Node
func (n *SqlDB) InsertNode(pKey, nodeID string, nodeType types.NodeType) error {
	info := types.NodeAllocateInfo{
		PublicKey:  pKey,
		NodeID:     nodeID,
		NodeType:   int(nodeType),
		CreateTime: time.Now().Format("2006-01-02 15:04:05"),
	}

	query := fmt.Sprintf(`INSERT INTO %s (node_id, public_key, create_time, node_type)
	VALUES (:node_id, :public_key, :create_time, :node_type)`, nodeAllocateTable)

	_, err := n.db.NamedExec(query, info)

	return err
}

// GetNodePublicKey get node public key
func (n *SqlDB) GetNodePublicKey(nodeID string) (string, error) {
	var pKey string

	query := fmt.Sprintf(`SELECT public_key FROM %s WHERE node_id=?`, nodeAllocateTable)
	if err := n.db.Get(&pKey, query, nodeID); err != nil {
		return pKey, err
	}

	return pKey, nil
}

// NodeExists is node exists
func (n *SqlDB) NodeExists(nodeID string, nodeType types.NodeType) error {
	var count int
	cQuery := fmt.Sprintf(`SELECT count(*) FROM %s WHERE node_id=? AND node_type=?`, nodeAllocateTable)
	err := n.db.Get(&count, cQuery, count, nodeType)
	if err != nil {
		return err
	}

	if count < 1 {
		return xerrors.New("node not exists")
	}

	return nil
}

// ListNodeIDs list nodes
func (n *SqlDB) ListNodeIDs(cursor int, count int) ([]string, int64, error) {
	var total int64

	cQuery := fmt.Sprintf(`SELECT count(*) FROM %s`, nodeInfoTable)
	err := n.db.Get(&total, cQuery)
	if err != nil {
		return nil, 0, err
	}

	sQuery := fmt.Sprintf(`SELECT node_id FROM %s order by node_id asc limit ?,?`, nodeInfoTable)

	if count > loadNodeInfosLimit {
		count = loadNodeInfosLimit
	}

	var out []string
	err = n.db.Select(&out, sQuery, cursor, count)
	if err != nil {
		return nil, 0, err
	}

	return out, total, nil
}

// GetNodeInfo get node info
func (n *SqlDB) GetNodeInfo(nodeID string) (*types.NodeInfo, error) {
	query := fmt.Sprintf(`SELECT * FROM %s WHERE node_id=?`, nodeInfoTable)

	var out types.NodeInfo
	err := n.db.Select(&out, query, nodeID)
	if err != nil {
		return nil, err
	}

	return &out, nil
}
