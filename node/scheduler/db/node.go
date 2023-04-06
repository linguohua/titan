package db

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/gob"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/linguohua/titan/api"
	"github.com/linguohua/titan/api/types"
	"github.com/linguohua/titan/node/modules/dtypes"
	"golang.org/x/xerrors"
)

// LoadTimeoutNodes retrieves nodes that are offline for a long time.
func (n *SQLDB) LoadTimeoutNodes(timeoutHour int, serverID dtypes.ServerID) ([]string, error) {
	list := make([]string, 0)

	time := time.Now().Add(-time.Duration(timeoutHour) * time.Hour)
	query := fmt.Sprintf("SELECT node_id FROM %s WHERE scheduler_sid=? AND quitted=? AND last_time <= ?", nodeInfoTable)
	if err := n.db.Select(&list, query, serverID, false, time); err != nil {
		return nil, err
	}

	return list, nil
}

// SetNodesQuitted sets the nodes' status as quitted.
func (n *SQLDB) SetNodesQuitted(nodeIDs []string) error {
	uQuery := fmt.Sprintf(`UPDATE %s SET quitted=? WHERE node_id in (?) `, nodeInfoTable)
	query, args, err := sqlx.In(uQuery, true, nodeIDs)
	if err != nil {
		return err
	}

	query = n.db.Rebind(query)
	_, err = n.db.Exec(query, args...)

	return err
}

// LoadPortMapping retrieves the mapping port of a node.
func (n *SQLDB) LoadPortMapping(nodeID string) (string, error) {
	var port string
	query := fmt.Sprintf("SELECT port_mapping FROM %s WHERE node_id=?", nodeInfoTable)
	if err := n.db.Get(&port, query, nodeID); err != nil {
		return "", err
	}

	return port, nil
}

// SetPortMapping sets the node's mapping port.
func (n *SQLDB) SetPortMapping(nodeID, port string) error {
	info := types.NodeInfo{
		NodeID:      nodeID,
		PortMapping: port,
	}
	// update
	query := fmt.Sprintf(`UPDATE %s SET port_mapping=:port_mapping WHERE node_id=:node_id`, nodeInfoTable)
	_, err := n.db.NamedExec(query, info)
	return err
}

// SetValidateResultInfos inserts validate result information.
func (n *SQLDB) SetValidateResultInfos(infos []*types.ValidateResultInfo) error {
	query := fmt.Sprintf(`INSERT INTO %s (round_id, node_id, validator_id, status, cid) VALUES (:round_id, :node_id, :validator_id, :status, :cid)`, validateResultTable)
	_, err := n.db.NamedExec(query, infos)

	return err
}

// LoadNodeValidateCID retrieves the cid of a validate result.
func (n *SQLDB) LoadNodeValidateCID(roundID, nodeID string) (string, error) {
	query := fmt.Sprintf("SELECT cid FROM %s WHERE round_id=? AND node_id=?", validateResultTable)
	var cid string
	err := n.db.Get(&cid, query, roundID, nodeID)
	return cid, err
}

// UpdateValidateResultInfo updates the validate result information.
func (n *SQLDB) UpdateValidateResultInfo(info *types.ValidateResultInfo) error {
	if info.Status == types.ValidateStatusSuccess {
		query := fmt.Sprintf(`UPDATE %s SET block_number=:block_number,status=:status, duration=:duration, bandwidth=:bandwidth, end_time=NOW() WHERE round_id=:round_id AND node_id=:node_id`, validateResultTable)
		_, err := n.db.NamedExec(query, info)
		return err
	}

	query := fmt.Sprintf(`UPDATE %s SET status=:status, end_time=NOW() WHERE round_id=:round_id AND node_id=:node_id`, validateResultTable)
	_, err := n.db.NamedExec(query, info)
	return err
}

// SetValidateResultsTimeout sets the validate results' status as timeout.
func (n *SQLDB) SetValidateResultsTimeout(roundID string) error {
	query := fmt.Sprintf(`UPDATE %s SET status=?, end_time=NOW() WHERE round_id=? AND status=?`, validateResultTable)
	_, err := n.db.Exec(query, types.ValidateStatusValidatorTimeOut, roundID, types.ValidateStatusCreate)
	return err
}

// LoadValidateResultInfos retrieves validate results.
func (n *SQLDB) LoadValidateResultInfos(startTime, endTime time.Time, pageNumber, pageSize int) (*types.ListValidateResultRsp, error) {
	// TODO problematic from web
	res := new(types.ListValidateResultRsp)
	var infos []types.ValidateResultInfo
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

// SetEdgeUpdateInfo inserts edge update information.
func (n *SQLDB) SetEdgeUpdateInfo(info *api.EdgeUpdateInfo) error {
	sqlString := fmt.Sprintf(`INSERT INTO %s (node_type, app_name, version, hash, download_url) VALUES (:node_type, :app_name, :version, :hash, :download_url) ON DUPLICATE KEY UPDATE app_name=:app_name, version=:version, hash=:hash, download_url=:download_url`, edgeUpdateTable)
	_, err := n.db.NamedExec(sqlString, info)
	return err
}

// LoadEdgeUpdateInfos retrieves edge update information.
func (n *SQLDB) LoadEdgeUpdateInfos() (map[int]*api.EdgeUpdateInfo, error) {
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

// DeleteEdgeUpdateInfo delete edge update info
func (n *SQLDB) DeleteEdgeUpdateInfo(nodeType int) error {
	deleteString := fmt.Sprintf(`DELETE FROM %s WHERE node_type=?`, edgeUpdateTable)
	_, err := n.db.Exec(deleteString, nodeType)
	return err
}

// UpdateValidators update validators
func (n *SQLDB) UpdateValidators(nodeIDs []string, serverID dtypes.ServerID) error {
	tx, err := n.db.Beginx()
	if err != nil {
		return err
	}

	defer func() {
		err = tx.Rollback()
		if err != nil && err != sql.ErrTxDone {
			log.Errorf("UpdateValidators Rollback err:%s", err.Error())
		}
	}()

	// clean old validators
	dQuery := fmt.Sprintf(`DELETE FROM %s WHERE scheduler_sid=? `, validatorsTable)
	_, err = tx.Exec(dQuery, serverID)
	if err != nil {
		return err
	}

	for _, nodeID := range nodeIDs {
		iQuery := fmt.Sprintf(`INSERT INTO %s (node_id, scheduler_sid) VALUES (?, ?)`, validatorsTable)
		_, err = tx.Exec(iQuery, nodeID, serverID)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

// LoadValidators retrieves validators information.
func (n *SQLDB) LoadValidators(serverID dtypes.ServerID) ([]string, error) {
	sQuery := fmt.Sprintf(`SELECT node_id FROM %s WHERE scheduler_sid=?`, validatorsTable)

	var out []string
	err := n.db.Select(&out, sQuery, serverID)
	if err != nil {
		return nil, err
	}

	return out, nil
}

// IsValidator Determine whether the node is a validator
func (n *SQLDB) IsValidator(nodeID string) (bool, error) {
	var count int64
	sQuery := fmt.Sprintf("SELECT count(node_id) FROM %s WHERE node_id=?", validatorsTable)
	err := n.db.Get(&count, sQuery, nodeID)
	if err != nil {
		return false, err
	}

	return count > 0, nil
}

// UpdateValidatorInfo reset scheduler server id for validator
func (n *SQLDB) UpdateValidatorInfo(serverID dtypes.ServerID, nodeID string) error {
	var count int64
	sQuery := fmt.Sprintf("SELECT count(node_id) FROM %s WHERE node_id=?", validatorsTable)
	err := n.db.Get(&count, sQuery, nodeID)
	if err != nil {
		return err
	}

	if count < 1 {
		return nil
	}

	uQuery := fmt.Sprintf(`UPDATE %s SET scheduler_sid=? WHERE node_id=?`, validatorsTable)
	_, err = n.db.Exec(uQuery, serverID, nodeID)

	return err
}

// UpsertNodeInfo Insert or update node info
func (n *SQLDB) UpsertNodeInfo(info *types.NodeInfo) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (node_id, mac_location, product_type, cpu_cores, memory, node_name, latitude, disk_usage,
			    longitude, disk_type, io_system, system_version, nat_type, disk_space, bandwidth_up, bandwidth_down, blocks, scheduler_sid) 
				VALUES (:node_id, :mac_location, :product_type, :cpu_cores, :memory, :node_name, :latitude, :disk_usage,
				:longitude, :disk_type, :io_system, :system_version, :nat_type, :disk_space, :bandwidth_up, :bandwidth_down, :blocks, :scheduler_sid) 
				ON DUPLICATE KEY UPDATE node_id=:node_id, last_time=:last_time, quitted=:quitted, disk_usage=:disk_usage, blocks=:blocks, scheduler_sid=:scheduler_sid`, nodeInfoTable)

	_, err := n.db.NamedExec(query, info)
	return err
}

// UpdateNodeOnlineTime update node online time and last time
func (n *SQLDB) UpdateNodeOnlineTime(nodeID string, onlineTime int) error {
	query := fmt.Sprintf(`UPDATE %s SET last_time=NOW(),online_time=? WHERE node_id=?`, nodeInfoTable)
	// update
	_, err := n.db.Exec(query, onlineTime, nodeID)
	return err
}

// InsertNodeRegisterInfo Insert Node register info
func (n *SQLDB) InsertNodeRegisterInfo(pKey, nodeID string, nodeType types.NodeType) error {
	query := fmt.Sprintf(`INSERT INTO %s (node_id, public_key, create_time, node_type)
	VALUES (?, ?, NOW(), ?)`, nodeRegisterTable)

	_, err := n.db.Exec(query, nodeID, pKey, nodeType)

	return err
}

// LoadNodePublicKey retrieves public key of node.
func (n *SQLDB) LoadNodePublicKey(nodeID string) (string, error) {
	var pKey string

	query := fmt.Sprintf(`SELECT public_key FROM %s WHERE node_id=?`, nodeRegisterTable)
	if err := n.db.Get(&pKey, query, nodeID); err != nil {
		return pKey, err
	}

	return pKey, nil
}

// NodeExists is node exists
func (n *SQLDB) NodeExists(nodeID string, nodeType types.NodeType) error {
	var count int
	cQuery := fmt.Sprintf(`SELECT count(*) FROM %s WHERE node_id=? AND node_type=?`, nodeRegisterTable)
	err := n.db.Get(&count, cQuery, count, nodeType)
	if err != nil {
		return err
	}

	if count < 1 {
		return xerrors.New("node not exists")
	}

	return nil
}

// LoadNodeInfos retrieves nodes information.
func (n *SQLDB) LoadNodeInfos(limit, offset int) (*sqlx.Rows, int64, error) {
	var total int64
	cQuery := fmt.Sprintf(`SELECT count(node_id) FROM %s`, nodeInfoTable)
	err := n.db.Get(&total, cQuery)
	if err != nil {
		return nil, 0, err
	}

	if limit > loadNodeInfosLimit || limit == 0 {
		limit = loadNodeInfosLimit
	}

	sQuery := fmt.Sprintf(`SELECT * FROM %s order by node_id asc LIMIT ? OFFSET ?`, nodeInfoTable)
	rows, err := n.db.QueryxContext(context.Background(), sQuery, limit, offset)
	return rows, total, err
}

// LoadNodeInfo retrieves node information.
func (n *SQLDB) LoadNodeInfo(nodeID string) (*types.NodeInfo, error) {
	query := fmt.Sprintf(`SELECT * FROM %s WHERE node_id=?`, nodeInfoTable)

	var out types.NodeInfo
	err := n.db.Select(&out, query, nodeID)
	if err != nil {
		return nil, err
	}

	return &out, nil
}

// LoadTopHash load assets view top hash
func (n *SQLDB) LoadTopHash(nodeID string) (string, error) {
	query := fmt.Sprintf(`SELECT top_hash FROM %s WHERE node_id=?`, AssetsView)

	var out string
	err := n.db.Get(&out, query, nodeID)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", nil
		}
		return "", err
	}

	return out, nil
}

// LoadBucketHashes load assets view buckets hashes
func (n *SQLDB) LoadBucketHashes(nodeID string) (map[uint32]string, error) {
	query := fmt.Sprintf(`SELECT bucket_hashes FROM %s WHERE node_id=?`, AssetsView)

	var data []byte
	err := n.db.Get(&data, query, nodeID)
	if err != nil {
		if err == sql.ErrNoRows {
			return make(map[uint32]string), nil
		}
		return nil, err
	}

	buffer := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buffer)

	out := make(map[uint32]string)
	err = dec.Decode(&out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// UpsertAssetsView update or insert top hash and buckets hashes to assets view
// bucketHashes key is number of bucket, value is bucket hash
func (n *SQLDB) UpsertAssetsView(nodeID string, topHash string, bucketHashes map[uint32]string) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (node_id, top_hash, bucket_hashes) VALUES (?, ?, ?) 
				ON DUPLICATE KEY UPDATE top_hash=?, bucket_hashes=?`, AssetsView)

	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(bucketHashes)
	if err != nil {
		return err
	}

	buf := buffer.Bytes()
	_, err = n.db.Exec(query, nodeID, topHash, buf, topHash, buf)
	return err
}

func (n *SQLDB) DeleteAssetsView(nodeID string) error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE node_id=?`, AssetsView)
	_, err := n.db.Exec(query, nodeID)
	return err
}

// LoadBucket load assets ids from bucket
// return hashes of asset
func (n *SQLDB) LoadBucket(bucketID string) ([]string, error) {
	query := fmt.Sprintf(`SELECT asset_hashes FROM %s WHERE bucket_id=?`, bucket)

	var data []byte
	err := n.db.Get(&data, query, bucketID)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	buffer := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buffer)

	out := make([]string, 0)
	err = dec.Decode(&out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// UpsertBucket update or insert assets ids to bucket
func (n *SQLDB) UpsertBucket(bucketID string, assetHashes []string) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (bucket_id, asset_hashes) VALUES (?, ?) 
				ON DUPLICATE KEY UPDATE asset_hashes=?`, bucket)

	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(assetHashes)
	if err != nil {
		return err
	}

	buf := buffer.Bytes()
	_, err = n.db.Exec(query, bucketID, buf, buf)
	return err
}

func (n *SQLDB) DeleteBucket(bucketID string) error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE bucket_id=?`, bucket)
	_, err := n.db.Exec(query, bucketID)
	return err
}
