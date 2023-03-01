package persistent

import (
	"fmt"
	"math/rand"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/linguohua/titan/api"
	"golang.org/x/xerrors"
)

// TypeMySQL MySql
func TypeMySQL() string {
	return "MySQL"
}

const (
	errNotFind = "Not Found"

	// tables
	carfileInfoTable  = "carfiles"
	replicaInfoTable  = "replicas"
	blockDownloadInfo = "block_download_info"
	nodeUpdateInfo    = "node_update_info"

	// NodeTypeKey node info key
	NodeTypeKey = "node_type"
	// SecretKey node info key
	SecretKey = "secret"

	loadNodeInfoMaxCount      = 100
	loadBlockDownloadMaxCount = 100
	loadReplicaInfoMaxCount   = 100
	loadValidateInfoMaxCount  = 100
)

var (
	url      string
	mysqlCli *sqlx.DB
)

// InitSQL init sql
func InitSQL(url string) (err error) {
	url = fmt.Sprintf("%s?parseTime=true&loc=Local", url)

	mysqlCli, err = sqlx.Open("mysql", url)
	if err != nil {
		return
	}

	if err = mysqlCli.Ping(); err != nil {
		return
	}

	return nil
}

// SetNodeInfo  Set node info
func SetNodeInfo(deviceID string, info *api.NodeInfo) error {
	info.DeviceID = deviceID

	var count int64
	cmd := "SELECT count(device_id) FROM node WHERE device_id=?"
	err := mysqlCli.Get(&count, cmd, deviceID)
	if err != nil {
		return err
	}

	if count == 0 {
		_, err = mysqlCli.NamedExec(`INSERT INTO node (device_id, last_time, geo, node_type,  address, private_key)
                VALUES (:device_id, :last_time, :geo, :node_type,  :address, :private_key)`, info)
		return err
	}

	// update
	_, err = mysqlCli.NamedExec(`UPDATE node SET last_time=:last_time,geo=:geo,address=:address,quitted=:quitted WHERE device_id=:device_id`, info)
	return err
}

// NodeOffline Set the last online time of the node
func NodeOffline(deviceID string, lastTime time.Time) error {
	info := &api.NodeInfo{
		DeviceID: deviceID,
		LastTime: lastTime,
	}

	_, err := mysqlCli.NamedExec(`UPDATE node SET last_time=:last_time WHERE device_id=:device_id`, info)

	return err
}

// NodePrivateKey Get node privateKey
func NodePrivateKey(deviceID string) (string, error) {
	var privateKey string
	query := "SELECT private_key FROM node WHERE device_id=?"
	if err := mysqlCli.Get(&privateKey, query, deviceID); err != nil {
		return "", err
	}

	return privateKey, nil
}

// LongTimeOfflineNodes get nodes that are offline for a long time
func LongTimeOfflineNodes(hour int) ([]*api.NodeInfo, error) {
	list := make([]*api.NodeInfo, 0)

	time := time.Now().Add(-time.Duration(hour) * time.Hour)

	cmd := "SELECT device_id FROM node WHERE quitted=? AND last_time <= ?"
	if err := mysqlCli.Select(&list, cmd, false, time); err != nil {
		return nil, err
	}

	return list, nil
}

// SetNodesQuit Node quit the titan
func SetNodesQuit(deviceIDs []string) error {
	tx := mysqlCli.MustBegin()

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
func SetNodePort(deviceID, port string) error {
	info := api.NodeInfo{
		DeviceID: deviceID,
		Port:     port,
	}
	// update
	_, err := mysqlCli.NamedExec(`UPDATE node SET port=:port WHERE device_id=:device_id`, info)
	return err
}

// InitValidateResultInfos init validate result infos
func InitValidateResultInfos(infos []*api.ValidateResult) error {
	tx := mysqlCli.MustBegin()
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

// SetValidateTimeoutOfNodes Set validate timeout of nodes
func SetValidateTimeoutOfNodes(roundID int64, deviceIDs []string) error {
	tx := mysqlCli.MustBegin()

	updateCachesCmd := `UPDATE validate_result SET status=?,end_time=NOW() WHERE round_id=? AND device_id in (?)`
	query, args, err := sqlx.In(updateCachesCmd, api.ValidateStatusTimeOut, roundID, deviceIDs)
	if err != nil {
		return err
	}

	// cache info
	query = mysqlCli.Rebind(query)
	tx.MustExec(query, args...)

	err = tx.Commit()
	if err != nil {
		err = tx.Rollback()
		return err
	}

	return nil
}

// UpdateValidateResultInfo Update validate info
func UpdateValidateResultInfo(info *api.ValidateResult) error {
	if info.Status == api.ValidateStatusSuccess {
		query := "UPDATE validate_result SET block_number=:block_number,status=:status, duration=:duration, bandwidth=:bandwidth, end_time=NOW() WHERE round_id=:round_id AND device_id=:device_id"
		_, err := mysqlCli.NamedExec(query, info)
		return err
	}

	query := "UPDATE validate_result SET status=:status, end_time=NOW() WHERE round_id=:round_id AND device_id=:device_id"
	_, err := mysqlCli.NamedExec(query, info)
	return err
}

// ValidateResultInfos Get validate result infos
func ValidateResultInfos(startTime, endTime time.Time, pageNumber, pageSize int) (*api.SummeryValidateResult, error) {
	res := new(api.SummeryValidateResult)
	var infos []api.ValidateResult
	query := fmt.Sprintf("SELECT *, (duration/1e3 * bandwidth) AS `upload_traffic` FROM validate_result WHERE start_time between ? and ? order by id asc  LIMIT ?,? ")

	if pageSize > loadValidateInfoMaxCount {
		pageSize = loadValidateInfoMaxCount
	}

	err := mysqlCli.Select(&infos, query, startTime, endTime, (pageNumber-1)*pageSize, pageSize)
	if err != nil {
		return nil, err
	}

	res.ValidateResultInfos = infos

	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM validate_result WHERE start_time between ? and ? ")
	var count int
	err = mysqlCli.Get(&count, countQuery, startTime, endTime)
	if err != nil {
		return nil, err
	}

	res.Total = count

	return res, nil
}

// CreateCarfileReplicaInfo Create replica info
func CreateCarfileReplicaInfo(cInfo *api.ReplicaInfo) error {
	cmd := fmt.Sprintf("INSERT INTO %s (id, carfile_hash, device_id, status, is_candidate) VALUES (:id, :carfile_hash, :device_id, :status, :is_candidate)", replicaInfoTable)
	_, err := mysqlCli.NamedExec(cmd, cInfo)
	return err
}

// UpdateCarfileReplicaStatus Update
func UpdateCarfileReplicaStatus(hash string, deviceIDs []string, status api.CacheStatus) error {
	tx := mysqlCli.MustBegin()

	cmd := fmt.Sprintf("UPDATE %s SET status=? WHERE carfile_hash=? AND device_id in (?) ", replicaInfoTable)
	query, args, err := sqlx.In(cmd, status, hash, deviceIDs)
	if err != nil {
		return err
	}

	// cache info
	query = mysqlCli.Rebind(query)
	tx.MustExec(query, args...)

	err = tx.Commit()
	if err != nil {
		err = tx.Rollback()
		return err
	}

	return nil
}

// UpdateCarfileReplicaInfo update replica info
func UpdateCarfileReplicaInfo(cInfo *api.ReplicaInfo) error {
	cmd := fmt.Sprintf("UPDATE %s SET status=:status,end_time=:end_time WHERE id=:id", replicaInfoTable)
	_, err := mysqlCli.NamedExec(cmd, cInfo)

	return err
}

// UpdateCarfileRecordCachesInfo update carfile info
func UpdateCarfileRecordCachesInfo(dInfo *api.CarfileRecordInfo) error {
	// update
	cmd := fmt.Sprintf("UPDATE %s SET total_size=:total_size,total_blocks=:total_blocks,end_time=NOW(),replica=:replica,expired_time=:expired_time WHERE carfile_hash=:carfile_hash", carfileInfoTable)
	_, err := mysqlCli.NamedExec(cmd, dInfo)

	return err
}

// CreateOrUpdateCarfileRecordInfo create or update carfile record info
func CreateOrUpdateCarfileRecordInfo(info *api.CarfileRecordInfo) error {
	cmd := fmt.Sprintf("INSERT INTO %s (carfile_hash, carfile_cid, replica,expired_time) VALUES (:carfile_hash, :carfile_cid, :replica, :expired_time) ON DUPLICATE KEY UPDATE replica=:replica,expired_time=:expired_time", carfileInfoTable)
	_, err := mysqlCli.NamedExec(cmd, info)
	return err
}

// CarfileRecordExisted Carfile record existed
func CarfileRecordExisted(hash string) (bool, error) {
	var count int
	cmd := fmt.Sprintf("SELECT count(carfile_hash) FROM %s WHERE carfile_hash=?", carfileInfoTable)
	err := mysqlCli.Get(&count, cmd, hash)
	return count > 0, err
}

// LoadCarfileInfo get carfile info with hash
func LoadCarfileInfo(hash string) (*api.CarfileRecordInfo, error) {
	var info api.CarfileRecordInfo
	cmd := fmt.Sprintf("SELECT * FROM %s WHERE carfile_hash=?", carfileInfoTable)
	err := mysqlCli.Get(&info, cmd, hash)
	return &info, err
}

// LoadCarfileInfos get carfile infos with hashs
func LoadCarfileInfos(hashs []string) ([]*api.CarfileRecordInfo, error) {
	getCarfilesCmd := fmt.Sprintf(`SELECT * FROM %s WHERE carfile_hash in (?)`, carfileInfoTable)
	carfilesQuery, args, err := sqlx.In(getCarfilesCmd, hashs)
	if err != nil {
		return nil, err
	}
	tx := mysqlCli.MustBegin()

	carfileRecords := make([]*api.CarfileRecordInfo, 0)

	carfilesQuery = mysqlCli.Rebind(carfilesQuery)
	tx.Select(&carfileRecords, carfilesQuery, args...)

	err = tx.Commit()
	if err != nil {
		err = tx.Rollback()
		return nil, err
	}

	return carfileRecords, nil
}

// CarfileRecordInfos get carfile record infos
func CarfileRecordInfos(page int) (info *api.CarfileRecordsInfo, err error) {
	num := 20

	info = &api.CarfileRecordsInfo{}

	cmd := fmt.Sprintf("SELECT count(carfile_hash) FROM %s ;", carfileInfoTable)
	err = mysqlCli.Get(&info.Cids, cmd)
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
	if err = mysqlCli.Select(&info.CarfileRecords, cmd); err != nil {
		return
	}

	return
}

// CandidatesWithHash get candidates with hash
func CandidatesWithHash(hash string) ([]string, error) {
	var out []string
	query := fmt.Sprintf(`SELECT device_id FROM %s WHERE carfile_hash=? AND status=? AND is_candidate=?`,
		replicaInfoTable)

	if err := mysqlCli.Select(&out, query, hash, api.CacheStatusSucceeded, true); err != nil {
		return nil, err
	}

	return out, nil
}

// CarfileReplicaInfosWithHash get carfile replica infos with hash
func CarfileReplicaInfosWithHash(hash string, isSuccess bool) ([]*api.ReplicaInfo, error) {
	var out []*api.ReplicaInfo
	if isSuccess {
		query := fmt.Sprintf(`SELECT * FROM %s WHERE carfile_hash=? AND status=?`, replicaInfoTable)

		if err := mysqlCli.Select(&out, query, hash, api.CacheStatusSucceeded); err != nil {
			return nil, err
		}
	} else {
		query := fmt.Sprintf(`SELECT * FROM %s WHERE carfile_hash=? `, replicaInfoTable)

		if err := mysqlCli.Select(&out, query, hash); err != nil {
			return nil, err
		}
	}

	return out, nil
}

// RandomCarfileFromNode Get a random carfile from the node
func RandomCarfileFromNode(deviceID string) (string, error) {
	query := fmt.Sprintf(`SELECT count(carfile_hash) FROM %s WHERE device_id=? AND status=?`, replicaInfoTable)

	var count int
	if err := mysqlCli.Get(&count, query, deviceID, api.CacheStatusSucceeded); err != nil {
		return "", err
	}

	if count < 1 {
		return "", xerrors.Errorf("node %s no cache", deviceID)
	}

	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
	// rand count
	index := rand.Intn(count)

	var hashs []string
	cmd := fmt.Sprintf("SELECT carfile_hash FROM %s WHERE device_id=? AND status=? LIMIT %d,%d", replicaInfoTable, index, 1)
	if err := mysqlCli.Select(&hashs, cmd, deviceID, api.CacheStatusSucceeded); err != nil {
		return "", err
	}

	if len(hashs) > 0 {
		return hashs[0], nil
	}

	return "", nil
}

// ResetCarfileExpirationTime reset expiration time with carfile record
func ResetCarfileExpirationTime(carfileHash string, expirationTime time.Time) error {
	tx := mysqlCli.MustBegin()

	cmd := fmt.Sprintf(`UPDATE %s SET expiration_time=? WHERE carfile_hash=?`, carfileInfoTable)
	tx.MustExec(cmd, expirationTime, carfileHash)

	err := tx.Commit()
	if err != nil {
		err = tx.Rollback()
		return err
	}

	return nil
}

func MinExpirationTime() (time.Time, error) {
	query := fmt.Sprintf(`SELECT MIN(expiration_time) FROM %s`, carfileInfoTable)

	var out time.Time
	if err := mysqlCli.Get(&out, query); err != nil {
		return out, err
	}

	return out, nil
}

func ExpiredCarfiles() ([]*api.CarfileRecordInfo, error) {
	query := fmt.Sprintf(`SELECT * FROM %s WHERE expiration_time <= NOW()`, carfileInfoTable)

	var out []*api.CarfileRecordInfo
	if err := mysqlCli.Select(&out, query); err != nil {
		return nil, err
	}

	return out, nil
}

// SucceededCachesCount get succeeded caches count
func SucceededCachesCount() (int, error) {
	query := fmt.Sprintf(`SELECT count(carfile_hash) FROM %s WHERE status=?`, replicaInfoTable)

	var count int
	if err := mysqlCli.Get(&count, query, api.CacheStatusSucceeded); err != nil {
		return 0, err
	}

	return count, nil
}

// LoadReplicaInfo load replica info with id
func LoadReplicaInfo(id string) (*api.ReplicaInfo, error) {
	var cache api.ReplicaInfo
	query := fmt.Sprintf("SELECT * FROM %s WHERE id=? ", replicaInfoTable)
	if err := mysqlCli.Get(&cache, query, id); err != nil {
		return nil, err
	}

	return &cache, nil
}

// RemoveCarfileRecord remove carfile
func RemoveCarfileRecord(carfileHash string) error {
	tx := mysqlCli.MustBegin()
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
func RemoveCarfileReplica(deviceID, carfileHash string) error {
	tx := mysqlCli.MustBegin()

	// cache info
	cCmd := fmt.Sprintf(`DELETE FROM %s WHERE device_id=? AND carfile_hash=?`, replicaInfoTable)
	tx.MustExec(cCmd, deviceID, carfileHash)

	var count int
	cmd := fmt.Sprintf("SELECT count(*) FROM %s WHERE carfile_hash=? AND status=? AND is_candidate=?", replicaInfoTable)
	err := tx.Get(&count, cmd, carfileHash, api.CacheStatusSucceeded, false)
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
func LoadCarfileRecordsWithNodes(deviceIDs []string) (hashs []string, err error) {
	tx := mysqlCli.MustBegin()

	// get carfiles
	getCarfilesCmd := fmt.Sprintf(`select carfile_hash from %s WHERE device_id in (?) GROUP BY carfile_hash`, replicaInfoTable)
	carfilesQuery, args, err := sqlx.In(getCarfilesCmd, deviceIDs)
	if err != nil {
		return
	}

	carfilesQuery = mysqlCli.Rebind(carfilesQuery)
	tx.Select(&hashs, carfilesQuery, args...)

	err = tx.Commit()
	if err != nil {
		err = tx.Rollback()
		return
	}

	return
}

// RemoveReplicaInfoWithNodes remove replica info with nodes
func RemoveReplicaInfoWithNodes(deviceIDs []string) error {
	tx := mysqlCli.MustBegin()

	// remove cache
	cmd := fmt.Sprintf(`DELETE FROM %s WHERE device_id in (?)`, replicaInfoTable)
	query, args, err := sqlx.In(cmd, deviceIDs)
	if err != nil {
		return err
	}

	query = mysqlCli.Rebind(query)
	tx.MustExec(query, args...)

	err = tx.Commit()
	if err != nil {
		err = tx.Rollback()
		return err
	}

	return nil
}

func BindNodeAllocateInfo(secret, deviceID string, nodeType api.NodeType) error {
	info := api.NodeAllocateInfo{
		Secret:     secret,
		DeviceID:   deviceID,
		NodeType:   int(nodeType),
		CreateTime: time.Now().Format("2006-01-02 15:04:05"),
	}

	_, err := mysqlCli.NamedExec(`INSERT INTO node_allocate_info (device_id, secret, create_time, node_type)
	VALUES (:device_id, :secret, :create_time, :node_type)`, info)

	return err
}

func GetNodeAllocateInfo(deviceID, key string, out interface{}) error {
	if key != "" {
		query := fmt.Sprintf(`SELECT %s FROM node_allocate_info WHERE device_id=?`, key)
		if err := mysqlCli.Get(out, query, deviceID); err != nil {
			return err
		}

		return nil
	}

	query := "SELECT * FROM node_allocate_info WHERE device_id=?"
	if err := mysqlCli.Get(out, query, deviceID); err != nil {
		return err
	}

	return nil
}

// download info
func SetBlockDownloadInfo(info *api.BlockDownloadInfo) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (id, device_id, block_cid, carfile_cid, block_size, speed, reward, status, failed_reason, client_ip, created_time, complete_time) 
				VALUES (:id, :device_id, :block_cid, :carfile_cid, :block_size, :speed, :reward, :status, :failed_reason, :client_ip, :created_time, :complete_time) ON DUPLICATE KEY UPDATE device_id=:device_id, speed=:speed, reward=:reward, status=:status, failed_reason=:failed_reason, complete_time=:complete_time`, blockDownloadInfo)

	_, err := mysqlCli.NamedExec(query, info)
	if err != nil {
		return err
	}

	return nil
}

func GetBlockDownloadInfoByDeviceID(deviceID string) ([]*api.BlockDownloadInfo, error) {
	query := fmt.Sprintf(`SELECT * FROM %s WHERE device_id = ? and TO_DAYS(created_time) >= TO_DAYS(NOW()) ORDER BY created_time DESC`, blockDownloadInfo)

	var out []*api.BlockDownloadInfo
	if err := mysqlCli.Select(&out, query, deviceID); err != nil {
		return nil, err
	}

	return out, nil
}

func GetBlockDownloadInfoByID(id string) (*api.BlockDownloadInfo, error) {
	query := fmt.Sprintf(`SELECT * FROM %s WHERE id = ?`, blockDownloadInfo)

	var out []*api.BlockDownloadInfo
	if err := mysqlCli.Select(&out, query, id); err != nil {
		return nil, err
	}

	if len(out) > 0 {
		return out[0], nil
	}
	return nil, nil
}

func GetNodesByUserDownloadBlockIn(minute int) ([]string, error) {
	starTime := time.Now().Add(time.Duration(minute) * time.Minute * -1)

	query := fmt.Sprintf(`SELECT device_id FROM %s WHERE complete_time > ? group by device_id`, blockDownloadInfo)

	var out []string
	if err := mysqlCli.Select(&out, query, starTime); err != nil {
		return nil, err
	}

	return out, nil
}

func GetCacheInfosWithNode(deviceID string, index, count int) (info *api.NodeCacheRsp, err error) {
	info = &api.NodeCacheRsp{}

	cmd := fmt.Sprintf("SELECT count(id) FROM %s WHERE device_id=?", replicaInfoTable)
	err = mysqlCli.Get(&info.TotalCount, cmd, deviceID)
	if err != nil {
		return
	}

	cmd = fmt.Sprintf("SELECT carfile_hash,status FROM %s WHERE device_id=? order by id asc LIMIT %d,%d", replicaInfoTable, index, count)
	if err = mysqlCli.Select(&info.Caches, cmd, deviceID); err != nil {
		return
	}

	return
}

func SetNodeUpdateInfo(info *api.NodeAppUpdateInfo) error {
	sqlString := fmt.Sprintf(`INSERT INTO %s (node_type, app_name, version, hash, download_url) VALUES (:node_type, :app_name, :version, :hash, :download_url) ON DUPLICATE KEY UPDATE app_name=:app_name, version=:version, hash=:hash, download_url=:download_url`, nodeUpdateInfo)
	_, err := mysqlCli.NamedExec(sqlString, info)
	return err
}

func GetNodeUpdateInfos() (map[int]*api.NodeAppUpdateInfo, error) {
	query := fmt.Sprintf(`SELECT * FROM %s`, nodeUpdateInfo)

	var out []*api.NodeAppUpdateInfo
	if err := mysqlCli.Select(&out, query); err != nil {
		return nil, err
	}

	ret := make(map[int]*api.NodeAppUpdateInfo)
	for _, info := range out {
		ret[info.NodeType] = info
	}
	return ret, nil
}

func DeleteNodeUpdateInfo(nodeType int) error {
	deleteString := fmt.Sprintf(`DELETE FROM %s WHERE node_type=?`, nodeUpdateInfo)
	_, err := mysqlCli.Exec(deleteString, nodeType)
	return err
}

// IsNilErr Is NilErr
func IsNilErr(err error) bool {
	return err.Error() == errNotFind
}

func GetNodes(cursor int, count int) ([]*api.NodeInfo, int64, error) {
	var total int64
	countSQL := "SELECT count(*) FROM node"
	err := mysqlCli.Get(&total, countSQL)
	if err != nil {
		return nil, 0, err
	}

	queryString := "SELECT device_id, is_online FROM node order by device_id asc limit ?,?"

	if count > loadNodeInfoMaxCount {
		count = loadNodeInfoMaxCount
	}

	var out []*api.NodeInfo
	err = mysqlCli.Select(&out, queryString, cursor, count)
	if err != nil {
		return nil, 0, err
	}

	return out, total, nil
}

func GetBlockDownloadInfos(deviceID string, startTime time.Time, endTime time.Time, cursor, count int) ([]api.BlockDownloadInfo, int64, error) {
	query := fmt.Sprintf(`SELECT * FROM %s WHERE device_id = ? and created_time between ? and ? limit ?,?`, blockDownloadInfo)

	var total int64
	countSQL := fmt.Sprintf(`SELECT count(*) FROM %s WHERE device_id = ? and created_time between ? and ?`, blockDownloadInfo)
	if err := mysqlCli.Get(&total, countSQL, deviceID, startTime, endTime); err != nil {
		return nil, 0, err
	}

	if count > loadBlockDownloadMaxCount {
		count = loadBlockDownloadMaxCount
	}

	var out []api.BlockDownloadInfo
	if err := mysqlCli.Select(&out, query, deviceID, startTime, endTime, cursor, count); err != nil {
		return nil, 0, err
	}

	return out, total, nil
}

func GetReplicaInfos(startTime time.Time, endTime time.Time, cursor, count int) (*api.ListCacheInfosRsp, error) {
	var total int64
	countSQL := fmt.Sprintf(`SELECT count(*) FROM %s WHERE end_time between ? and ?`, replicaInfoTable)
	if err := mysqlCli.Get(&total, countSQL, startTime, endTime); err != nil {
		return nil, err
	}

	if count > loadReplicaInfoMaxCount {
		count = loadReplicaInfoMaxCount
	}

	query := fmt.Sprintf(`SELECT * FROM %s WHERE end_time between ? and ? limit ?,?`, replicaInfoTable)

	var out []*api.ReplicaInfo
	if err := mysqlCli.Select(&out, query, startTime, endTime, cursor, count); err != nil {
		return nil, err
	}

	return &api.ListCacheInfosRsp{Datas: out, Total: total}, nil
}