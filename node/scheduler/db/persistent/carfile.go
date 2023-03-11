package persistent

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/linguohua/titan/api/types"

	"github.com/jmoiron/sqlx"
	"github.com/linguohua/titan/node/modules/dtypes"
	"golang.org/x/xerrors"
)

type CarfileDB struct {
	DB *sqlx.DB
}

func NewCarfileDB(db *sqlx.DB) *CarfileDB {
	return &CarfileDB{db}
}

func (c *CarfileDB) DeleteCarfileSql() string {
	return fmt.Sprintf(`DELETE FROM %s WHERE carfile_hash=?`, carfileInfoTable)
}

func (c *CarfileDB) UpdateOrCreateCarfileSql() string {
	return fmt.Sprintf("UPDATE %s SET total_size=:total_size,total_blocks=:total_blocks,end_time=NOW() WHERE carfile_hash=:carfile_hash", carfileInfoTable)
}

// CreateCarfileReplicaInfo Create replica info
func (c *CarfileDB) CreateCarfileReplicaInfo(cInfo *types.ReplicaInfo) error {
	cmd := fmt.Sprintf("INSERT INTO %s (id, carfile_hash, node_id, status, is_candidate) VALUES (:id, :carfile_hash, :node_id, :status, :is_candidate)", replicaInfoTable)
	_, err := c.DB.NamedExec(cmd, cInfo)
	return err
}

// UpdateCarfileReplicaStatus Update
func (c *CarfileDB) UpdateCarfileReplicaStatus(hash string, nodeIDs []string, status types.CacheStatus) error {
	tx := c.DB.MustBegin()

	cmd := fmt.Sprintf("UPDATE %s SET status=? WHERE carfile_hash=? AND node_id in (?) ", replicaInfoTable)
	query, args, err := sqlx.In(cmd, status, hash, nodeIDs)
	if err != nil {
		return err
	}

	// cache info
	query = c.DB.Rebind(query)
	tx.MustExec(query, args...)

	err = tx.Commit()
	if err != nil {
		err = tx.Rollback()
		return err
	}

	return nil
}

// UpdateCarfileReplicaInfo update replica info
func (c *CarfileDB) UpdateCarfileReplicaInfo(cInfo []*types.ReplicaInfo) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (id, carfile_hash, node_id, status, is_candidate) 
				VALUES (:id, :carfile_hash, :node_id, :status, :is_candidate) 
				ON DUPLICATE KEY UPDATE id=VALUES(id), end_time=NOW(), status=VALUES(status)`, replicaInfoTable)

	_, err := c.DB.NamedExec(query, cInfo)

	return err
}

// // CreateCarfileReplicaInfos Create replica infos
// func (c *CarfileDB) CreateCarfileReplicaInfos(cInfos []*types.ReplicaInfo) error {
// 	cmd := fmt.Sprintf(`INSERT INTO %s (id, carfile_hash, node_id, status, is_candidate)
// 	        VALUES (:id, :carfile_hash, :node_id, :status, :is_candidate)`, replicaInfoTable)
// 	_, err := c.DB.NamedExec(cmd, cInfos)
// 	return err
// }

// UpdateCarfileRecordCachesInfo update storage info
func (c *CarfileDB) UpdateCarfileRecordCachesInfo(dInfo *types.CarfileRecordInfo) error {
	fmt.Println("info:", dInfo)
	// update
	cmd := fmt.Sprintf("UPDATE %s SET total_size=:total_size,total_blocks=:total_blocks WHERE carfile_hash=:carfile_hash", carfileInfoTable)
	_, err := c.DB.NamedExec(cmd, dInfo)

	return err
}

// CreateOrUpdateCarfileRecordInfo create or update storage record info
func (c *CarfileDB) CreateOrUpdateCarfileRecordInfo(info *types.CarfileRecordInfo) error {
	cmd := fmt.Sprintf(`INSERT INTO %s (carfile_hash, carfile_cid, state, edge_replica, candidate_replica, expiration, total_blocks, total_size, succeed_edges, succeed_candidates, failed_candidates, failed_edges)
	        VALUES (:carfile_hash, :carfile_cid, :state, :edge_replica, :candidate_replica, :expiration, :total_blocks, :total_size, :succeed_edges, :succeed_candidates, :failed_candidates, :failed_edges) 
	        ON DUPLICATE KEY UPDATE succeed_edges=VALUES(succeed_edges),succeed_candidates=VALUES(succeed_candidates),failed_edges=VALUES(failed_edges),failed_candidates=VALUES(failed_candidates),
			total_size=VALUES(total_size),total_blocks=VALUES(total_blocks),edge_replica=VALUES(edge_replica),candidate_replica=VALUES(candidate_replica),expiration=VALUES(expiration),state=VALUES(state)`, carfileInfoTable)
	_, err := c.DB.NamedExec(cmd, info)
	return err
}

// CarfileRecordExisted Carfile record existed
func (c *CarfileDB) CarfileRecordExisted(hash string) (bool, error) {
	var count int
	cmd := fmt.Sprintf("SELECT count(carfile_hash) FROM %s WHERE carfile_hash=?", carfileInfoTable)
	err := c.DB.Get(&count, cmd, hash)
	return count > 0, err
}

// LoadCarfileInfo get storage info with hash
func (c *CarfileDB) LoadCarfileInfo(hash string) (*types.CarfileRecordInfo, error) {
	var info types.CarfileRecordInfo
	cmd := fmt.Sprintf("SELECT * FROM %s WHERE carfile_hash=?", carfileInfoTable)
	err := c.DB.Get(&info, cmd, hash)
	return &info, err
}

// LoadCarfileInfos get storage infos with hashs
func (c *CarfileDB) LoadCarfileInfos(hashs []string) ([]*types.CarfileRecordInfo, error) {
	getCarfilesCmd := fmt.Sprintf(`SELECT * FROM %s WHERE carfile_hash in (?)`, carfileInfoTable)
	carfilesQuery, args, err := sqlx.In(getCarfilesCmd, hashs)
	if err != nil {
		return nil, err
	}
	tx := c.DB.MustBegin()

	carfileRecords := make([]*types.CarfileRecordInfo, 0)

	carfilesQuery = c.DB.Rebind(carfilesQuery)
	tx.Select(&carfileRecords, carfilesQuery, args...)

	err = tx.Commit()
	if err != nil {
		err = tx.Rollback()
		return nil, err
	}

	return carfileRecords, nil
}

// CountCarfiles count carfiles size
func (c *CarfileDB) CountCarfiles() (int, error) {
	var size int
	cmd := fmt.Sprintf("SELECT count(carfile_hash) FROM %s ;", carfileInfoTable)
	err := c.DB.Get(&size, cmd)
	if err != nil {
		return 0, err
	}
	return size, nil
}

// QueryCarfilesRows ...
func (c *CarfileDB) QueryCarfilesRows(ctx context.Context, limit, offset int) (rows *sqlx.Rows, err error) {
	maxCount := 100
	if limit == 0 {
		limit = maxCount
	}
	if limit > maxCount {
		limit = maxCount
	}

	cmd := fmt.Sprintf("SELECT * FROM %s WHERE state!='Finalize' order by carfile_hash asc LIMIT ? OFFSET ? ", carfileInfoTable)
	return c.DB.QueryxContext(ctx, cmd, limit, offset)
}

// CarfileRecordInfos get storage record infos
func (c *CarfileDB) CarfileRecordInfos(page int) (info *types.ListCarfileRecordRsp, err error) {
	num := 20

	info = &types.ListCarfileRecordRsp{}

	cmd := fmt.Sprintf("SELECT count(carfile_hash) FROM %s ;", carfileInfoTable)
	err = c.DB.Get(&info.Cids, cmd)
	if err != nil {
		return
	}

	info.TotalPage = info.Cids / num
	if info.Cids%num > 0 {
		info.TotalPage++
	}

	if info.TotalPage == 0 {
		return
	}

	if page > info.TotalPage {
		page = info.TotalPage
	}
	info.Page = page

	cmd = fmt.Sprintf("SELECT * FROM %s order by carfile_hash asc LIMIT %d,%d", carfileInfoTable, (num * (page - 1)), num)
	if err = c.DB.Select(&info.CarfileRecords, cmd); err != nil {
		return
	}

	return
}

// CandidatesByHash get candidates by hash
func (c *CarfileDB) CandidatesByHash(hash string) ([]string, error) {
	var out []string
	query := fmt.Sprintf(`SELECT node_id FROM %s WHERE carfile_hash=? AND status=? AND is_candidate=?`,
		replicaInfoTable)

	if err := c.DB.Select(&out, query, hash, types.CacheStatusSucceeded, true); err != nil {
		return nil, err
	}

	return out, nil
}

// EdgesByHash get edges by hash
func (c *CarfileDB) EdgesByHash(hash string) ([]string, error) {
	var out []string
	query := fmt.Sprintf(`SELECT node_id FROM %s WHERE carfile_hash=? AND status=? AND is_candidate=?`,
		replicaInfoTable)

	if err := c.DB.Select(&out, query, hash, types.CacheStatusSucceeded, false); err != nil {
		return nil, err
	}

	return out, nil
}

// CarfileReplicaInfosByHash get storage replica infos by hash
func (c *CarfileDB) CarfileReplicaInfosByHash(hash string, isSuccess bool) ([]*types.ReplicaInfo, error) {
	var out []*types.ReplicaInfo
	if isSuccess {
		query := fmt.Sprintf(`SELECT * FROM %s WHERE carfile_hash=? AND status=?`, replicaInfoTable)

		if err := c.DB.Select(&out, query, hash, types.CacheStatusSucceeded); err != nil {
			return nil, err
		}
	} else {
		query := fmt.Sprintf(`SELECT * FROM %s WHERE carfile_hash=? `, replicaInfoTable)

		if err := c.DB.Select(&out, query, hash); err != nil {
			return nil, err
		}
	}

	return out, nil
}

// RandomCarfileFromNode Get a random carfile from the node
func (c *CarfileDB) RandomCarfileFromNode(nodeID string) (string, error) {
	query := fmt.Sprintf(`SELECT count(carfile_hash) FROM %s WHERE node_id=? AND status=?`, replicaInfoTable)

	var count int
	if err := c.DB.Get(&count, query, nodeID, types.CacheStatusSucceeded); err != nil {
		return "", err
	}

	if count < 1 {
		return "", xerrors.Errorf("node %s no cache", nodeID)
	}

	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
	// rand count
	index := rand.Intn(count)

	var hashs []string
	cmd := fmt.Sprintf("SELECT carfile_hash FROM %s WHERE node_id=? AND status=? LIMIT %d,%d", replicaInfoTable, index, 1)
	if err := c.DB.Select(&hashs, cmd, nodeID, types.CacheStatusSucceeded); err != nil {
		return "", err
	}

	if len(hashs) > 0 {
		return hashs[0], nil
	}

	return "", nil
}

// ResetCarfileExpiration reset expiration time with storage record
func (c *CarfileDB) ResetCarfileExpiration(carfileHash string, eTime time.Time) error {
	tx := c.DB.MustBegin()

	cmd := fmt.Sprintf(`UPDATE %s SET expiration=? WHERE carfile_hash=?`, carfileInfoTable)
	tx.MustExec(cmd, eTime, carfileHash)

	err := tx.Commit()
	if err != nil {
		err = tx.Rollback()
		return err
	}

	return nil
}

// MinExpiration Get the minimum expiration time
func (c *CarfileDB) MinExpiration() (time.Time, error) {
	query := fmt.Sprintf(`SELECT MIN(expiration) FROM %s`, carfileInfoTable)

	var out time.Time
	if err := c.DB.Get(&out, query); err != nil {
		return out, err
	}

	return out, nil
}

func (c *CarfileDB) ExpiredCarfiles() ([]*types.CarfileRecordInfo, error) {
	query := fmt.Sprintf(`SELECT * FROM %s WHERE expiration <= NOW()`, carfileInfoTable)

	var out []*types.CarfileRecordInfo
	if err := c.DB.Select(&out, query); err != nil {
		return nil, err
	}

	return out, nil
}

// SucceededCachesCount get succeeded caches count
func (c *CarfileDB) SucceededCachesCount() (int, error) {
	query := fmt.Sprintf(`SELECT count(carfile_hash) FROM %s WHERE status=?`, replicaInfoTable)

	var count int
	if err := c.DB.Get(&count, query, types.CacheStatusSucceeded); err != nil {
		return 0, err
	}

	return count, nil
}

// LoadReplicaInfo load replica info with id
func (c *CarfileDB) LoadReplicaInfo(id string) (*types.ReplicaInfo, error) {
	var cache types.ReplicaInfo
	query := fmt.Sprintf("SELECT * FROM %s WHERE id=? ", replicaInfoTable)
	if err := c.DB.Get(&cache, query, id); err != nil {
		return nil, err
	}

	return &cache, nil
}

// RemoveCarfileRecord remove storage
func (c *CarfileDB) RemoveCarfileRecord(carfileHash string) error {
	tx := c.DB.MustBegin()
	// cache info
	cCmd := fmt.Sprintf(`DELETE FROM %s WHERE carfile_hash=? `, replicaInfoTable)
	tx.MustExec(cCmd, carfileHash)

	// data info
	dCmd := fmt.Sprintf(`DELETE FROM %s WHERE carfile_hash=?`, carfileInfoTable)
	tx.MustExec(dCmd, carfileHash)

	err := tx.Commit()
	if err != nil {
		err = tx.Rollback()
		return err
	}

	return nil
}

// RemoveCarfileReplica remove replica info
func (c *CarfileDB) RemoveCarfileReplica(nodeID, carfileHash string) error {
	tx := c.DB.MustBegin()

	// cache info
	cCmd := fmt.Sprintf(`DELETE FROM %s WHERE node_id=? AND carfile_hash=?`, replicaInfoTable)
	tx.MustExec(cCmd, nodeID, carfileHash)

	var count int
	cmd := fmt.Sprintf("SELECT count(*) FROM %s WHERE carfile_hash=? AND status=? AND is_candidate=?", replicaInfoTable)
	err := tx.Get(&count, cmd, carfileHash, types.CacheStatusSucceeded, false)
	if err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		err = tx.Rollback()
		return err
	}

	return nil
}

// LoadCarfileRecordsWithNodes load carfile record hashs with nodes
func (c *CarfileDB) LoadCarfileRecordsWithNodes(nodeIDs []string) (hashs []string, err error) {
	tx := c.DB.MustBegin()

	// get carfiles
	getCarfilesCmd := fmt.Sprintf(`select carfile_hash from %s WHERE node_id in (?) GROUP BY carfile_hash`, replicaInfoTable)
	carfilesQuery, args, err := sqlx.In(getCarfilesCmd, nodeIDs)
	if err != nil {
		return
	}

	carfilesQuery = c.DB.Rebind(carfilesQuery)
	tx.Select(&hashs, carfilesQuery, args...)

	err = tx.Commit()
	if err != nil {
		err = tx.Rollback()
		return
	}

	return
}

// RemoveReplicaInfoWithNodes remove replica info with nodes
func (c *CarfileDB) RemoveReplicaInfoWithNodes(nodeIDs []string) error {
	tx := c.DB.MustBegin()

	// remove cache
	cmd := fmt.Sprintf(`DELETE FROM %s WHERE node_id in (?)`, replicaInfoTable)
	query, args, err := sqlx.In(cmd, nodeIDs)
	if err != nil {
		return err
	}

	query = c.DB.Rebind(query)
	tx.MustExec(query, args...)

	err = tx.Commit()
	if err != nil {
		err = tx.Rollback()
		return err
	}

	return nil
}

// download info
func (c *CarfileDB) SetBlockDownloadInfo(info *types.DownloadRecordInfo) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (id, node_id, block_cid, carfile_cid, block_size, speed, reward, status, failed_reason, client_ip, created_time, complete_time) 
				VALUES (:id, :node_id, :block_cid, :carfile_cid, :block_size, :speed, :reward, :status, :failed_reason, :client_ip, :created_time, :complete_time) ON DUPLICATE KEY UPDATE node_id=:node_id, speed=:speed, reward=:reward, status=:status, failed_reason=:failed_reason, complete_time=:complete_time`, blockDownloadInfo)

	_, err := c.DB.NamedExec(query, info)
	if err != nil {
		return err
	}

	return nil
}

// func (c *CarfileDB) GetBlockDownloadInfoByNodeID(nodeID string) ([]*types.DownloadRecordInfo, error) {
// 	query := fmt.Sprintf(`SELECT * FROM %s WHERE node_id = ? and TO_DAYS(created_time) >= TO_DAYS(NOW()) ORDER BY created_time DESC`, blockDownloadInfo)

// 	var out []*types.DownloadRecordInfo
// 	if err := c.DB.Select(&out, query, nodeID); err != nil {
// 		return nil, err
// 	}

// 	return out, nil
// }

func (c *CarfileDB) GetBlockDownloadInfoByID(id string) (*types.DownloadRecordInfo, error) {
	query := fmt.Sprintf(`SELECT * FROM %s WHERE id = ?`, blockDownloadInfo)

	var out []*types.DownloadRecordInfo
	if err := c.DB.Select(&out, query, id); err != nil {
		return nil, err
	}

	if len(out) > 0 {
		return out[0], nil
	}
	return nil, nil
}

func (c *CarfileDB) GetNodesByUserDownloadBlockIn(minute int) ([]string, error) {
	starTime := time.Now().Add(time.Duration(minute) * time.Minute * -1)

	query := fmt.Sprintf(`SELECT node_id FROM %s WHERE complete_time > ? group by node_id`, blockDownloadInfo)

	var out []string
	if err := c.DB.Select(&out, query, starTime); err != nil {
		return nil, err
	}

	return out, nil
}

func (c *CarfileDB) GetCacheInfosWithNode(nodeID string, index, count int) (info *types.NodeCacheRsp, err error) {
	info = &types.NodeCacheRsp{}

	cmd := fmt.Sprintf("SELECT count(id) FROM %s WHERE node_id=?", replicaInfoTable)
	err = c.DB.Get(&info.TotalCount, cmd, nodeID)
	if err != nil {
		return
	}

	cmd = fmt.Sprintf("SELECT carfile_hash,status FROM %s WHERE node_id=? order by id asc LIMIT %d,%d", replicaInfoTable, index, count)
	if err = c.DB.Select(&info.Caches, cmd, nodeID); err != nil {
		return
	}

	return
}

func (c *CarfileDB) GetBlockDownloadInfos(nodeID string, startTime time.Time, endTime time.Time, cursor, count int) ([]types.DownloadRecordInfo, int64, error) {
	query := fmt.Sprintf(`SELECT * FROM %s WHERE node_id = ? and created_time between ? and ? limit ?,?`, blockDownloadInfo)

	var total int64
	countSQL := fmt.Sprintf(`SELECT count(*) FROM %s WHERE node_id = ? and created_time between ? and ?`, blockDownloadInfo)
	if err := c.DB.Get(&total, countSQL, nodeID, startTime, endTime); err != nil {
		return nil, 0, err
	}

	if count > loadBlockDownloadMaxCount {
		count = loadBlockDownloadMaxCount
	}

	var out []types.DownloadRecordInfo
	if err := c.DB.Select(&out, query, nodeID, startTime, endTime, cursor, count); err != nil {
		return nil, 0, err
	}

	return out, total, nil
}

func (c *CarfileDB) CarfileReplicaList(startTime time.Time, endTime time.Time, cursor, count int) (*types.ListCarfileReplicaRsp, error) {
	var total int64
	countSQL := fmt.Sprintf(`SELECT count(*) FROM %s WHERE end_time between ? and ?`, replicaInfoTable)
	if err := c.DB.Get(&total, countSQL, startTime, endTime); err != nil {
		return nil, err
	}

	if count > loadReplicaInfoMaxCount {
		count = loadReplicaInfoMaxCount
	}

	query := fmt.Sprintf(`SELECT * FROM %s WHERE end_time between ? and ? limit ?,?`, replicaInfoTable)

	var out []*types.ReplicaInfo
	if err := c.DB.Select(&out, query, startTime, endTime, cursor, count); err != nil {
		return nil, err
	}

	return &types.ListCarfileReplicaRsp{Datas: out, Total: total}, nil
}

// PushCarfileToWaitList waiting data list
func (c *CarfileDB) PushCarfileToWaitList(info *types.CacheCarfileInfo) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (carfile_hash, carfile_cid, replicas, node_id, expiration, server_id) 
				VALUES (:carfile_hash, :carfile_cid, :replicas, :node_id, :expiration, :server_id) 
				ON DUPLICATE KEY UPDATE carfile_hash=:carfile_hash, carfile_cid=:carfile_cid, replicas=:replicas, node_id=:node_id, 
				expiration=:expiration, server_id=:server_id`, waitingCarfileTable)

	_, err := c.DB.NamedExec(query, info)
	return err
}

// LoadWaitCarfiles load
func (c *CarfileDB) LoadWaitCarfiles(serverID dtypes.ServerID) (*types.CacheCarfileInfo, error) {
	sQuery := fmt.Sprintf(`SELECT * FROM %s WHERE server_id=? order by id asc limit ?`, waitingCarfileTable)

	info := &types.CacheCarfileInfo{}
	err := c.DB.Get(info, sQuery, serverID, 1)
	if err != nil {
		return nil, err
	}

	return info, nil
}

// RemoveWaitCarfile remove
func (c *CarfileDB) RemoveWaitCarfile(id string) error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE id=?`, waitingCarfileTable)
	_, err := c.DB.Exec(query, id)
	return err
}

// GetCachingCarfiles ...
func (c *CarfileDB) GetCachingCarfiles(serverID dtypes.ServerID) ([]string, error) {
	sQuery := fmt.Sprintf(`SELECT carfile_hash FROM %s WHERE server_id=? GROUP BY carfile_hash`, downloadingTable)

	var out []string
	if err := c.DB.Select(&out, sQuery, serverID); err != nil {
		return nil, err
	}
	return out, nil
}

// ReplicaTasksStart ...
func (c *CarfileDB) ReplicaTasksStart(serverID dtypes.ServerID, hash string, nodeIDs []string) error {
	tx := c.DB.MustBegin()

	for _, nodeID := range nodeIDs {
		sQuery := fmt.Sprintf(`INSERT INTO %s (carfile_hash, node_id, server_id) VALUES (?, ?, ?)`, downloadingTable)
		tx.MustExec(sQuery, hash, nodeID, serverID)
	}

	err := tx.Commit()
	if err != nil {
		err = tx.Rollback()
	}

	return err
}

// ReplicaTasksEnd ...
func (c *CarfileDB) ReplicaTasksEnd(serverID dtypes.ServerID, hash string, nodeIDs []string) (bool, error) {
	dQuery := fmt.Sprintf("DELETE FROM %s WHERE server_id=? AND carfile_hash=? AND node_id in (?) ", downloadingTable)
	query, args, err := sqlx.In(dQuery, serverID, hash, nodeIDs)
	if err != nil {
		return false, err
	}

	// cache info
	query = c.DB.Rebind(query)
	_, err = c.DB.Exec(query, args...)
	if err != nil {
		return false, err
	}

	var count int
	sQuery := fmt.Sprintf("SELECT count(*) FROM %s WHERE carfile_hash=? AND server_id=?", downloadingTable)
	err = c.DB.Get(&count, sQuery, hash, serverID)
	if err != nil {
		return false, err
	}

	return count == 0, nil
}
