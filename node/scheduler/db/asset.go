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

// UpdateUnfinishedReplica update unfinished replica info , return an error if the replica is finished
func (n *SQLDB) UpdateUnfinishedReplica(cInfo *types.ReplicaInfo) error {
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

// UpdateUnfinishedReplicasStatus updates the status of unfinished asset replicas
func (n *SQLDB) UpdateUnfinishedReplicasStatus(hash string, status types.ReplicaStatus) error {
	query := fmt.Sprintf(`UPDATE %s SET end_time=NOW(), status=? WHERE hash=? AND (status=? or status=?)`, replicaInfoTable)
	_, err := n.db.Exec(query, status, hash, types.ReplicaStatusPulling, types.ReplicaStatusWaiting)

	return err
}

// BulkUpsertReplicas inserts or updates replica information in bulk
func (n *SQLDB) BulkUpsertReplicas(infos []*types.ReplicaInfo) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (hash, node_id, status, is_candidate) 
				VALUES (:hash, :node_id, :status, :is_candidate) 
				ON DUPLICATE KEY UPDATE status=VALUES(status)`, replicaInfoTable)

	_, err := n.db.NamedExec(query, infos)

	return err
}

// UpsertAssetRecord inserts or updates asset record information
func (n *SQLDB) UpsertAssetRecord(info *types.AssetRecord) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (hash, cid, state, edge_replicas, candidate_replicas, expiration, total_size, total_blocks, scheduler_sid, end_time) 
				VALUES (:hash, :cid, :state, :edge_replicas, :candidate_replicas, :expiration, :total_size, :total_blocks, :scheduler_sid, NOW()) 
				ON DUPLICATE KEY UPDATE total_size=VALUES(total_size), total_blocks=VALUES(total_blocks), state=VALUES(state), end_time=NOW()`, assetRecordTable)

	_, err := n.db.NamedExec(query, info)
	return err
}

// FetchAssetRecord fetches asset record information
func (n *SQLDB) FetchAssetRecord(hash string) (*types.AssetRecord, error) {
	var info types.AssetRecord
	query := fmt.Sprintf("SELECT * FROM %s WHERE hash=?", assetRecordTable)
	err := n.db.Get(&info, query, hash)
	if err != nil {
		return nil, err
	}

	return &info, err
}

// FetchAssetRecords fetches asset records information
func (n *SQLDB) FetchAssetRecords(statuses []string, limit, offset int, serverID dtypes.ServerID) (*sqlx.Rows, error) {
	if limit > loadAssetRecordsLimit || limit == 0 {
		limit = loadAssetRecordsLimit
	}
	sQuery := fmt.Sprintf(`SELECT * FROM %s WHERE state in (?) AND scheduler_sid=? order by hash asc LIMIT ? OFFSET ?`, assetRecordTable)
	query, args, err := sqlx.In(sQuery, statuses, serverID, limit, offset)
	if err != nil {
		return nil, err
	}

	query = n.db.Rebind(query)
	return n.db.QueryxContext(context.Background(), query, args...)
}

// FetchReplicasByHash fetches asset replica information based on hash and statuses.
func (n *SQLDB) FetchReplicasByHash(hash string, statuses []types.ReplicaStatus) (*sqlx.Rows, error) {
	sQuery := fmt.Sprintf(`SELECT * FROM %s WHERE hash=? AND status in (?)`, replicaInfoTable)
	query, args, err := sqlx.In(sQuery, hash, statuses)
	if err != nil {
		return nil, err
	}

	query = n.db.Rebind(query)
	return n.db.QueryxContext(context.Background(), query, args...)
}

// FetchAssetReplicas fetches asset replica information based on hash.
func (n *SQLDB) FetchAssetReplicas(hash string) ([]*types.ReplicaInfo, error) {
	var out []*types.ReplicaInfo
	query := fmt.Sprintf(`SELECT * FROM %s WHERE hash=? `, replicaInfoTable)
	if err := n.db.Select(&out, query, hash); err != nil {
		return nil, err
	}

	return out, nil
}

// FetchNodeReplicaCount retrieves the succeeded replica count of a node based on nodeID.
func (n *SQLDB) FetchNodeReplicaCount(nodeID string) (int, error) {
	query := fmt.Sprintf(`SELECT count(hash) FROM %s WHERE node_id=? AND status=?`, replicaInfoTable)

	var count int
	err := n.db.Get(&count, query, nodeID, types.ReplicaStatusSucceeded)

	return count, err
}

// FetchAssetCIDsByNodeID retrieves asset CIDs of a node based on nodeID.
func (n *SQLDB) FetchAssetCIDsByNodeID(nodeID string, limit, offset int) ([]string, error) {
	var hashes []string
	query := fmt.Sprintf("select cid from (select hash from %s WHERE node_id=? AND status=? LIMIT %d OFFSET %d) as a left join %s as b on a.hash = b.hash", replicaInfoTable, limit, offset, assetRecordTable)
	if err := n.db.Select(&hashes, query, nodeID, types.ReplicaStatusSucceeded); err != nil {
		return nil, err
	}

	return hashes, nil
}

// UpdateAssetRecordExpiry resets asset record expiration time based on hash and eTime
func (n *SQLDB) UpdateAssetRecordExpiry(hash string, eTime time.Time) error {
	query := fmt.Sprintf(`UPDATE %s SET expiration=? WHERE hash=?`, assetRecordTable)
	_, err := n.db.Exec(query, eTime, hash)

	return err
}

// FetchMinExpiryOfAssetRecords  fetches the minimum expiration time of asset records based on serverID.
func (n *SQLDB) FetchMinExpiryOfAssetRecords(serverID dtypes.ServerID) (time.Time, error) {
	query := fmt.Sprintf(`SELECT MIN(expiration) FROM %s WHERE scheduler_sid=?`, assetRecordTable)

	var out time.Time
	if err := n.db.Get(&out, query, serverID); err != nil {
		return out, err
	}

	return out, nil
}

// FetchExpiredAssetRecords fetches all expired asset records based on serverID.
func (n *SQLDB) FetchExpiredAssetRecords(serverID dtypes.ServerID) ([]*types.AssetRecord, error) {
	query := fmt.Sprintf(`SELECT * FROM %s WHERE scheduler_sid=? AND expiration <= NOW() LIMIT ?`, assetRecordTable)

	var out []*types.AssetRecord
	if err := n.db.Select(&out, query, serverID, loadExpiredAssetRecordsLimit); err != nil {
		return nil, err
	}

	return out, nil
}

// FetchUnfinishedPullAssetNodes retrieves the node IDs for all nodes that have not yet finished pulling an asset for a given asset hash.
func (n *SQLDB) FetchUnfinishedPullAssetNodes(hash string) ([]string, error) {
	var nodes []string
	query := fmt.Sprintf(`SELECT node_id FROM %s WHERE hash=? AND (status=? or status=?)`, replicaInfoTable)
	err := n.db.Select(&nodes, query, hash, types.ReplicaStatusPulling, types.ReplicaStatusWaiting)
	return nodes, err
}

// DeleteAssetRecord removes all records associated with a given asset hash from the database.
func (n *SQLDB) DeleteAssetRecord(hash string) error {
	tx, err := n.db.Beginx()
	if err != nil {
		return err
	}

	defer func() {
		err = tx.Rollback()
		if err != nil && err != sql.ErrTxDone {
			log.Errorf("DeleteAssetRecord Rollback err:%s", err.Error())
		}
	}()

	// replica info
	cQuery := fmt.Sprintf(`DELETE FROM %s WHERE hash=? `, replicaInfoTable)
	_, err = tx.Exec(cQuery, hash)
	if err != nil {
		return err
	}

	// asset info
	dQuery := fmt.Sprintf(`DELETE FROM %s WHERE hash=?`, assetRecordTable)
	_, err = tx.Exec(dQuery, hash)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// FetchAssetHashesOfNodes fetches the asset hashes associated with a set of node IDs.
func (n *SQLDB) FetchAssetHashesOfNodes(nodeIDs []string) (hashes []string, err error) {
	sQuery := fmt.Sprintf(`select hash from %s WHERE node_id in (?) GROUP BY hash`, replicaInfoTable)
	query, args, err := sqlx.In(sQuery, nodeIDs)
	if err != nil {
		return
	}

	query = n.db.Rebind(query)
	err = n.db.Select(&hashes, query, args...)

	return
}

// DeleteReplicasForNodes removes all replica entries associated with a set of node IDs from the database.
func (n *SQLDB) DeleteReplicasForNodes(nodeIDs []string) error {
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

// FetchReplicasForNode fetches information about all replicas associated with a given node ID.
func (n *SQLDB) FetchReplicasForNode(nodeID string, index, count int) (info *types.NodeReplicaRsp, err error) {
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

// FetchReplicas fetches information about all replicas whose end_time is between startTime and endTime, limited to "count" results and starting from "cursor".
func (n *SQLDB) FetchReplicas(startTime time.Time, endTime time.Time, cursor, count int) (*types.ListReplicaInfosRsp, error) {
	var total int64
	countSQL := fmt.Sprintf(`SELECT count(hash) FROM %s WHERE end_time between ? and ?`, replicaInfoTable)
	if err := n.db.Get(&total, countSQL, startTime, endTime); err != nil {
		return nil, err
	}

	if count > loadReplicaInfosLimit {
		count = loadReplicaInfosLimit
	}
	// TODO problematic from web
	query := fmt.Sprintf(`SELECT * FROM %s WHERE end_time between ? and ? limit ?,?`, replicaInfoTable)

	var out []*types.ReplicaInfo
	if err := n.db.Select(&out, query, startTime, endTime, cursor, count); err != nil {
		return nil, err
	}

	return &types.ListReplicaInfosRsp{Replicas: out, Total: total}, nil
}
