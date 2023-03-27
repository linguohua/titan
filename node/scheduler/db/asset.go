package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/linguohua/titan/api/types"

	"github.com/jmoiron/sqlx"
	"github.com/linguohua/titan/node/modules/dtypes"
	"golang.org/x/xerrors"
)

// UpdateUnfinishedReplicaInfo update unfinished replica info , return an error if the replica is finished
func (n *SQLDB) UpdateUnfinishedReplicaInfo(cInfo *types.ReplicaInfo) error {
	query := fmt.Sprintf(`UPDATE %s SET end_time=NOW(), status=?, done_size=? WHERE hash=? AND node_id=? AND (status=? or status=?)`, replicaInfoTable)
	result, err := n.db.Exec(query, cInfo.Status, cInfo.DoneSize, cInfo.Hash, cInfo.NodeID, types.ReplicaStatusPulling, types.ReplicaStatusWaiting)
	if err != nil {
		return err
	}

	r, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if r < 1 {
		return xerrors.New("nothing to update")
	}

	return nil
}

// UpdateStatusOfUnfinishedReplicas update status of unfinished asset replicas
func (n *SQLDB) UpdateStatusOfUnfinishedReplicas(hash string, status types.ReplicaStatus) error {
	query := fmt.Sprintf(`UPDATE %s SET end_time=NOW(), status=? WHERE hash=? AND (status=? or status=?)`, replicaInfoTable)
	_, err := n.db.Exec(query, status, hash, types.ReplicaStatusPulling, types.ReplicaStatusWaiting)

	return err
}

// BatchUpsertReplicas Insert or update replicas info
func (n *SQLDB) BatchUpsertReplicas(infos []*types.ReplicaInfo) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (hash, node_id, status, is_candidate) 
				VALUES (:hash, :node_id, :status, :is_candidate) 
				ON DUPLICATE KEY UPDATE status=VALUES(status)`, replicaInfoTable)

	_, err := n.db.NamedExec(query, infos)

	return err
}

// UpsertAssetRecord Insert or update asset record info
func (n *SQLDB) UpsertAssetRecord(info *types.AssetRecord) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (hash, cid, state, edge_replicas, candidate_replicas, expiration, total_size, total_blocks, scheduler_sid, end_time) 
				VALUES (:hash, :cid, :state, :edge_replicas, :candidate_replicas, :expiration, :total_size, :total_blocks, :scheduler_sid, NOW()) 
				ON DUPLICATE KEY UPDATE total_size=VALUES(total_size), total_blocks=VALUES(total_blocks), state=VALUES(state), end_time=NOW()`, assetRecordsTable)

	_, err := n.db.NamedExec(query, info)
	return err
}

// LoadAssetRecord load asset record of hash
func (n *SQLDB) LoadAssetRecord(hash string) (*types.AssetRecord, error) {
	var info types.AssetRecord
	query := fmt.Sprintf("SELECT * FROM %s WHERE hash=?", assetRecordsTable)
	err := n.db.Get(&info, query, hash)
	if err != nil {
		return nil, err
	}

	return &info, err
}

// LoadAssetRecords load asset record infos
func (n *SQLDB) LoadAssetRecords(statuses []string, limit, offset int, serverID dtypes.ServerID) (*sqlx.Rows, error) {
	if limit > loadAssetRecordsLimit || limit == 0 {
		limit = loadAssetRecordsLimit
	}

	sQuery := fmt.Sprintf(`SELECT * FROM %s WHERE state in (?) AND scheduler_sid=? order by hash asc LIMIT ? OFFSET ?`, assetRecordsTable)
	query, args, err := sqlx.In(sQuery, statuses, serverID, limit, offset)
	if err != nil {
		return nil, err
	}

	query = n.db.Rebind(query)
	return n.db.QueryxContext(context.Background(), query, args...)
}

// LoadReplicasOfHash load replicas of asset hash
func (n *SQLDB) LoadReplicasOfHash(hash string, statuses []string) (*sqlx.Rows, error) {
	sQuery := fmt.Sprintf(`SELECT * FROM %s WHERE hash=? AND status in (?)`, replicaInfoTable)
	query, args, err := sqlx.In(sQuery, hash, statuses)
	if err != nil {
		return nil, err
	}

	query = n.db.Rebind(query)
	return n.db.QueryxContext(context.Background(), query, args...)
}

// LoadAssetReplicaInfos load all replicas of the asset
func (n *SQLDB) LoadAssetReplicaInfos(hash string) ([]*types.ReplicaInfo, error) {
	var out []*types.ReplicaInfo
	query := fmt.Sprintf(`SELECT * FROM %s WHERE hash=? `, replicaInfoTable)
	if err := n.db.Select(&out, query, hash); err != nil {
		return nil, err
	}

	return out, nil
}

// LoadReplicaCountOfNode load succeeded replica count of node
func (n *SQLDB) LoadReplicaCountOfNode(nodeID string) (int, error) {
	query := fmt.Sprintf(`SELECT count(hash) FROM %s WHERE node_id=? AND status=?`, replicaInfoTable)

	var count int
	err := n.db.Get(&count, query, nodeID, types.ReplicaStatusSucceeded)

	return count, err
}

// LoadAssetHashesOfNode load a asset of the node
func (n *SQLDB) LoadAssetHashesOfNode(nodeID string, limit, offset int) ([]string, error) {
	var hashes []string
	query := fmt.Sprintf("SELECT hash FROM %s WHERE node_id=? AND status=? LIMIT %d OFFSET %d", replicaInfoTable, limit, offset)
	if err := n.db.Select(&hashes, query, nodeID, types.ReplicaStatusSucceeded); err != nil {
		return nil, err
	}

	return hashes, nil
}

// UpdateAssetRecordExpiration reset asset record expiration time
func (n *SQLDB) UpdateAssetRecordExpiration(hash string, eTime time.Time) error {
	query := fmt.Sprintf(`UPDATE %s SET expiration=? WHERE hash=?`, assetRecordsTable)
	_, err := n.db.Exec(query, eTime, hash)

	return err
}

// LoadMinExpirationOfAssetRecords Get the minimum expiration time of asset records
func (n *SQLDB) LoadMinExpirationOfAssetRecords() (time.Time, error) {
	query := fmt.Sprintf(`SELECT MIN(expiration) FROM %s`, assetRecordsTable)

	var out time.Time
	if err := n.db.Get(&out, query); err != nil {
		return out, err
	}

	return out, nil
}

// LoadExpiredAssetRecords load all expired asset records
func (n *SQLDB) LoadExpiredAssetRecords() ([]*types.AssetRecord, error) {
	query := fmt.Sprintf(`SELECT * FROM %s WHERE expiration <= NOW()`, assetRecordsTable)

	var out []*types.AssetRecord
	if err := n.db.Select(&out, query); err != nil {
		return nil, err
	}

	return out, nil
}

// LoadPullingNodes load unfinished nodes for asset hash
func (n *SQLDB) LoadPullingNodes(hash string) ([]string, error) {
	var nodes []string
	query := fmt.Sprintf(`SELECT node_id FROM %s WHERE hash=? AND (status=? or status=?)`, replicaInfoTable)
	err := n.db.Select(&nodes, query, hash, types.ReplicaStatusPulling, types.ReplicaStatusWaiting)
	return nodes, err
}

// RemoveAssetRecord remove asset record
func (n *SQLDB) RemoveAssetRecord(hash string) error {
	tx, err := n.db.Beginx()
	if err != nil {
		return err
	}

	defer func() {
		err = tx.Rollback()
		if err != nil && err != sql.ErrTxDone {
			log.Errorf("RemoveAssetRecord Rollback err:%s", err.Error())
		}
	}()

	// replica info
	cQuery := fmt.Sprintf(`DELETE FROM %s WHERE hash=? `, replicaInfoTable)
	_, err = tx.Exec(cQuery, hash)
	if err != nil {
		return err
	}

	// asset info
	dQuery := fmt.Sprintf(`DELETE FROM %s WHERE hash=?`, assetRecordsTable)
	_, err = tx.Exec(dQuery, hash)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// LoadAssetHashesOfNodes load asset hashes of nodes
func (n *SQLDB) LoadAssetHashesOfNodes(nodeIDs []string) (hashes []string, err error) {
	sQuery := fmt.Sprintf(`select hash from %s WHERE node_id in (?) GROUP BY hash`, replicaInfoTable)
	query, args, err := sqlx.In(sQuery, nodeIDs)
	if err != nil {
		return
	}

	query = n.db.Rebind(query)
	err = n.db.Select(&hashes, query, args...)

	return
}

// RemoveReplicaInfoOfNodes remove replica info of nodes
func (n *SQLDB) RemoveReplicaInfoOfNodes(nodeIDs []string) error {
	// remove replica
	dQuery := fmt.Sprintf(`DELETE FROM %s WHERE node_id in (?)`, replicaInfoTable)
	query, args, err := sqlx.In(dQuery, nodeIDs)
	if err != nil {
		return err
	}

	query = n.db.Rebind(query)
	_, err = n.db.Exec(query, args...)
	return err
}

// LoadReplicaInfosOfNode load node replica infos
func (n *SQLDB) LoadReplicaInfosOfNode(nodeID string, index, count int) (info *types.NodeReplicaRsp, err error) {
	info = &types.NodeReplicaRsp{}

	query := fmt.Sprintf("SELECT count(hash) FROM %s WHERE node_id=?", replicaInfoTable)
	err = n.db.Get(&info.TotalCount, query, nodeID)
	if err != nil {
		return
	}

	query = fmt.Sprintf("SELECT hash,status FROM %s WHERE node_id=? order by hash asc LIMIT %d,%d", replicaInfoTable, index, count)
	if err = n.db.Select(&info.Replica, query, nodeID); err != nil {
		return
	}

	return
}

// LoadReplicaInfos load replicas info
func (n *SQLDB) LoadReplicaInfos(startTime time.Time, endTime time.Time, cursor, count int) (*types.ListReplicaInfosRsp, error) {
	var total int64
	countSQL := fmt.Sprintf(`SELECT count(hash) FROM %s WHERE end_time between ? and ?`, replicaInfoTable)
	if err := n.db.Get(&total, countSQL, startTime, endTime); err != nil {
		return nil, err
	}

	if count > loadReplicaInfosLimit {
		count = loadReplicaInfosLimit
	}

	query := fmt.Sprintf(`SELECT * FROM %s WHERE end_time between ? and ? limit ?,?`, replicaInfoTable)

	var out []*types.ReplicaInfo
	if err := n.db.Select(&out, query, startTime, endTime, cursor, count); err != nil {
		return nil, err
	}

	return &types.ListReplicaInfosRsp{Replicas: out, Total: total}, nil
}
