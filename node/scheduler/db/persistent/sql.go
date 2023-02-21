package persistent

import (
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/linguohua/titan/api"
	"golang.org/x/xerrors"
)

// TypeSQL Sql
func TypeSQL() string {
	return "Sql"
}

type sqlDB struct {
	cli *sqlx.DB
	url string
}

const errNotFind = "Not Found"

var (
	// eventInfoTable    = "carfile_event_%s"
	carfileInfoTable  = "carfiles_%s"
	cacheInfoTable    = "replicas_%s"
	blockDownloadInfo = "block_download_info_%s"
	nodeUpdateInfo    = "node_update_info"
)

// InitSQL init sql
func InitSQL(url string) (DB, error) {
	url = fmt.Sprintf("%s?parseTime=true&loc=Local", url)

	db := &sqlDB{url: url}
	database, err := sqlx.Open("mysql", url)
	if err != nil {
		return nil, err
	}

	if err := database.Ping(); err != nil {
		return nil, err
	}

	db.cli = database

	err = db.setAllNodeOffline()
	return db, err
}

// node info
func (sd sqlDB) SetNodeInfo(deviceID string, info *NodeInfo) error {
	info.DeviceID = deviceID
	info.ServerID = serverID
	// info.CreateTime = time.Now().Format("2006-01-02 15:04:05")

	var count int64
	cmd := "SELECT count(device_id) FROM node WHERE device_id=?"
	err := sd.cli.Get(&count, cmd, deviceID)
	if err != nil {
		return err
	}

	if count == 0 {
		_, err = sd.cli.NamedExec(`INSERT INTO node (device_id, last_time, geo, node_type, is_online, address, server_id,private_key)
                VALUES (:device_id, :last_time, :geo, :node_type, :is_online, :address, :server_id,:private_key)`, info)
		return err
	}

	// update
	_, err = sd.cli.NamedExec(`UPDATE node SET last_time=:last_time,geo=:geo,is_online=:is_online,address=:address,server_id=:server_id,quitted=:quitted WHERE device_id=:device_id`, info)
	return err
}

func (sd sqlDB) SetNodeOffline(deviceID string, lastTime time.Time) error {
	info := &NodeInfo{
		DeviceID: deviceID,
		LastTime: lastTime,
		IsOnline: false,
	}

	_, err := sd.cli.NamedExec(`UPDATE node SET last_time=:last_time,is_online=:is_online WHERE device_id=:device_id`, info)

	return err
}

func (sd sqlDB) GetNodePrivateKey(deviceID string) (string, error) {
	var privateKey string
	query := "SELECT private_key FROM node WHERE device_id=?"
	if err := sd.cli.Get(&privateKey, query, deviceID); err != nil {
		return "", err
	}

	return privateKey, nil
}

func (sd sqlDB) GetOfflineNodes() ([]*NodeInfo, error) {
	list := make([]*NodeInfo, 0)

	cmd := "SELECT device_id,last_time FROM node WHERE quitted=? AND is_online=? AND server_id=?"
	if err := sd.cli.Select(&list, cmd, false, false, serverID); err != nil {
		return nil, err
	}

	return list, nil
}

func (sd sqlDB) SetNodesQuit(deviceIDs []string) error {
	tx := sd.cli.MustBegin()

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

// Validate Result
func (sd sqlDB) AddValidateResultInfos(infos []*api.ValidateResult) error {
	tx := sd.cli.MustBegin()
	for _, info := range infos {
		query := "INSERT INTO validate_result (round_id, device_id, validator_id, status, start_time, server_id) VALUES (?, ?, ?, ?, ?, ?)"
		tx.MustExec(query, info.RoundID, info.DeviceID, info.ValidatorID, info.Status, info.StartTime, serverID)
	}

	err := tx.Commit()
	if err != nil {
		err = tx.Rollback()
		return err
	}

	return nil
}

func (sd sqlDB) SetTimeoutToValidateInfos(roundID int64, deviceIDs []string) error {
	tx := sd.cli.MustBegin()

	updateCachesCmd := `UPDATE validate_result SET status=?,end_time=NOW() WHERE round_id=? AND device_id in (?)`
	query, args, err := sqlx.In(updateCachesCmd, api.ValidateStatusTimeOut, roundID, deviceIDs)
	if err != nil {
		return err
	}

	// cache info
	query = sd.cli.Rebind(query)
	tx.MustExec(query, args...)

	err = tx.Commit()
	if err != nil {
		err = tx.Rollback()
		return err
	}

	return nil
}

func (sd sqlDB) UpdateValidateResultInfo(info *api.ValidateResult) error {
	if info.Status == api.ValidateStatusSuccess {
		query := "UPDATE validate_result SET block_number=:block_number,status=:status, duration=:duration, bandwidth=:bandwidth, end_time=NOW() WHERE round_id=:round_id AND device_id=:device_id"
		_, err := sd.cli.NamedExec(query, info)
		return err
	}

	query := "UPDATE validate_result SET status=:status, end_time=NOW() WHERE round_id=:round_id AND device_id=:device_id"
	_, err := sd.cli.NamedExec(query, info)
	return err
}

func (sd sqlDB) SummaryValidateMessage(startTime, endTime time.Time, pageNumber, pageSize int) (*api.SummeryValidateResult, error) {
	res := new(api.SummeryValidateResult)
	var infos []api.ValidateResult
	query := fmt.Sprintf("SELECT%s%s%v%s%v%s%d,%d;",
		" *, (duration/1e3 * bandwidth) AS `upload_traffic` FROM validate_result",
		" WHERE start_time >= '",
		startTime,
		"' AND end_time <= '",
		endTime,
		"' LIMIT ",
		(pageNumber-1)*pageSize,
		pageSize)
	err := sd.cli.Select(&infos, query)
	if err != nil {
		return nil, err
	}

	res.ValidateResultInfos = infos

	countQuery := fmt.Sprintf("SELECT%s%s%v%s%v%s;",
		" COUNT(*) FROM validate_result",
		" WHERE start_time >= '",
		startTime,
		"' AND end_time <= '",
		endTime,
		"'",
	)

	var count int
	err = sd.cli.Get(&count, countQuery)
	if err != nil {
		return nil, err
	}

	res.Total = count

	return res, nil
}

// cache data info
func (sd sqlDB) CreateCarfileReplicaInfo(cInfo *api.CarfileReplicaInfo) error {
	area := sd.replaceArea()
	cTableName := fmt.Sprintf(cacheInfoTable, area)

	cmd := fmt.Sprintf("INSERT INTO %s (id, carfile_hash, device_id, status, is_candidate) VALUES (:id, :carfile_hash, :device_id, :status, :is_candidate)", cTableName)
	_, err := sd.cli.NamedExec(cmd, cInfo)
	return err
}

func (sd sqlDB) UpdateCarfileReplicaStatus(hash string, deviceIDs []string, status api.CacheStatus) error {
	area := sd.replaceArea()
	cTableName := fmt.Sprintf(cacheInfoTable, area)

	tx := sd.cli.MustBegin()

	cmd := fmt.Sprintf("UPDATE %s SET status=? WHERE carfile_hash=? AND device_id in (?) ", cTableName)
	query, args, err := sqlx.In(cmd, status, hash, deviceIDs)
	if err != nil {
		return err
	}

	// cache info
	query = sd.cli.Rebind(query)
	tx.MustExec(query, args...)

	err = tx.Commit()
	if err != nil {
		err = tx.Rollback()
		return err
	}

	return nil
}

func (sd sqlDB) UpdateCarfileReplicaInfo(cInfo *api.CarfileReplicaInfo) error {
	area := sd.replaceArea()
	cTableName := fmt.Sprintf(cacheInfoTable, area)

	cmd := fmt.Sprintf("UPDATE %s SET done_size=:done_size,done_blocks=:done_blocks,status=:status,end_time=:end_time WHERE id=:id", cTableName)
	_, err := sd.cli.NamedExec(cmd, cInfo)

	return err
}

func (sd sqlDB) UpdateCarfileRecordCachesInfo(dInfo *api.CarfileRecordInfo) error {
	area := sd.replaceArea()
	dTableName := fmt.Sprintf(carfileInfoTable, area)
	cTableName := fmt.Sprintf(cacheInfoTable, area)

	var count int
	cmd := fmt.Sprintf("SELECT count(*) FROM %s WHERE carfile_hash=? AND status=? And is_candidate=?", cTableName)
	err := sd.cli.Get(&count, cmd, dInfo.CarfileHash, api.CacheStatusSucceeded, false)
	if err != nil {
		return err
	}

	dInfo.CurReliability = count

	// update
	cmd = fmt.Sprintf("UPDATE %s SET total_size=:total_size,total_blocks=:total_blocks,cur_reliability=:cur_reliability,end_time=NOW(),need_reliability=:need_reliability,expired_time=:expired_time WHERE carfile_hash=:carfile_hash", dTableName)
	_, err = sd.cli.NamedExec(cmd, dInfo)

	return err
}

func (sd sqlDB) CreateOrUpdateCarfileRecordInfo(info *api.CarfileRecordInfo, isUpdate bool) error {
	area := sd.replaceArea()

	tableName := fmt.Sprintf(carfileInfoTable, area)

	if isUpdate {
		cmd := fmt.Sprintf("UPDATE %s SET need_reliability=:need_reliability,expired_time=:expired_time WHERE carfile_hash=:carfile_hash", tableName)
		_, err := sd.cli.NamedExec(cmd, info)
		return err
	}

	cmd := fmt.Sprintf("INSERT INTO %s (carfile_hash, carfile_cid, need_reliability,expired_time) VALUES (:carfile_hash, :carfile_cid, :need_reliability, :expired_time)", tableName)
	_, err := sd.cli.NamedExec(cmd, info)
	return err
}

func (sd sqlDB) CarfileRecordExisted(hash string) (bool, error) {
	area := sd.replaceArea()

	var count int
	cmd := fmt.Sprintf("SELECT count(carfile_hash) FROM %s WHERE carfile_hash=?", fmt.Sprintf(carfileInfoTable, area))
	err := sd.cli.Get(&count, cmd, hash)
	return count > 0, err
}

func (sd sqlDB) GetCarfileInfo(hash string) (*api.CarfileRecordInfo, error) {
	area := sd.replaceArea()

	var info api.CarfileRecordInfo
	cmd := fmt.Sprintf("SELECT * FROM %s WHERE carfile_hash=?", fmt.Sprintf(carfileInfoTable, area))
	err := sd.cli.Get(&info, cmd, hash)
	return &info, err
}

func (sd sqlDB) GetCarfileCidWithPage(page int) (info *api.DataListInfo, err error) {
	area := sd.replaceArea()
	num := 20

	info = &api.DataListInfo{}

	cmd := fmt.Sprintf("SELECT count(carfile_cid) FROM %s ;", fmt.Sprintf(carfileInfoTable, area))
	err = sd.cli.Get(&info.Cids, cmd)
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

	cmd = fmt.Sprintf("SELECT * FROM %s LIMIT %d,%d", fmt.Sprintf(carfileInfoTable, area), (num * (page - 1)), num)
	if err = sd.cli.Select(&info.CarfileRecords, cmd); err != nil {
		return
	}

	return
}

func (sd sqlDB) GetCachesWithCandidate(hash string) ([]string, error) {
	area := sd.replaceArea()

	var out []string
	query := fmt.Sprintf(`SELECT device_id FROM %s WHERE carfile_hash=? AND status=? AND is_candidate=?`,
		fmt.Sprintf(cacheInfoTable, area))

	if err := sd.cli.Select(&out, query, hash, api.CacheStatusSucceeded, true); err != nil {
		return nil, err
	}

	return out, nil
}

func (sd sqlDB) GetCarfileReplicaInfosWithHash(hash string, isSuccess bool) ([]*api.CarfileReplicaInfo, error) {
	area := sd.replaceArea()

	var out []*api.CarfileReplicaInfo
	if isSuccess {
		query := fmt.Sprintf(`SELECT * FROM %s WHERE carfile_hash=? AND status=?`,
			fmt.Sprintf(cacheInfoTable, area))

		if err := sd.cli.Select(&out, query, hash, api.CacheStatusSucceeded); err != nil {
			return nil, err
		}
	} else {
		query := fmt.Sprintf(`SELECT * FROM %s WHERE carfile_hash=? `,
			fmt.Sprintf(cacheInfoTable, area))

		if err := sd.cli.Select(&out, query, hash); err != nil {
			return nil, err
		}
	}

	return out, nil
}

func (sd sqlDB) GetRandCarfileWithNode(deviceID string) (string, error) {
	query := fmt.Sprintf(`SELECT count(carfile_hash) FROM %s WHERE device_id=? AND status=?`,
		fmt.Sprintf(cacheInfoTable, sd.replaceArea()))

	var count int
	if err := sd.cli.Get(&count, query, deviceID, api.CacheStatusSucceeded); err != nil {
		return "", err
	}

	if count < 1 {
		return "", xerrors.Errorf("node %s no cache", deviceID)
	}

	// rand count
	index := myRand.Intn(count)

	var hashs []string
	cmd := fmt.Sprintf("SELECT carfile_hash FROM %s WHERE device_id=? AND status=? LIMIT %d,%d", fmt.Sprintf(cacheInfoTable, sd.replaceArea()), index, 1)
	if err := sd.cli.Select(&hashs, cmd, deviceID, api.CacheStatusSucceeded); err != nil {
		return "", err
	}

	// fmt.Println("hashs :", hashs)

	if len(hashs) > 0 {
		return hashs[0], nil
	}

	return "", nil
}

func (sd sqlDB) ChangeExpiredTimeWhitCarfile(carfileHash string, expiredTime time.Time) error {
	tx := sd.cli.MustBegin()

	cmd := fmt.Sprintf(`UPDATE %s SET expired_time=? WHERE carfile_hash=?`, fmt.Sprintf(carfileInfoTable, sd.replaceArea()))
	tx.MustExec(cmd, expiredTime, carfileHash)

	err := tx.Commit()
	if err != nil {
		err = tx.Rollback()
		return err
	}

	return nil
}

func (sd sqlDB) GetMinExpiredTime() (time.Time, error) {
	query := fmt.Sprintf(`SELECT MIN(expired_time) FROM %s`,
		fmt.Sprintf(carfileInfoTable, sd.replaceArea()))

	var out time.Time
	if err := sd.cli.Get(&out, query); err != nil {
		return out, err
	}

	return out, nil
}

func (sd sqlDB) GetExpiredCarfiles() ([]*api.CarfileRecordInfo, error) {
	query := fmt.Sprintf(`SELECT * FROM %s WHERE expired_time <= NOW()`,
		fmt.Sprintf(carfileInfoTable, sd.replaceArea()))

	var out []*api.CarfileRecordInfo
	if err := sd.cli.Select(&out, query); err != nil {
		return nil, err
	}

	return out, nil
}

func (sd sqlDB) GetUndoneCarfiles(page int) (info *api.DataListInfo, err error) {
	area := sd.replaceArea()

	info = &api.DataListInfo{}
	if page < 0 {
		cmd := fmt.Sprintf("SELECT * FROM %s WHERE cur_reliability < need_reliability ", fmt.Sprintf(carfileInfoTable, area))
		if err = sd.cli.Select(&info.CarfileRecords, cmd); err != nil {
			return
		}

		return
	}

	num := 20

	cmd := fmt.Sprintf("SELECT count(carfile_cid) FROM %s WHERE cur_reliability < need_reliability", fmt.Sprintf(carfileInfoTable, area))
	err = sd.cli.Get(&info.Cids, cmd)
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

	cmd = fmt.Sprintf("SELECT * FROM %s WHERE cur_reliability < need_reliability LIMIT %d,%d", fmt.Sprintf(carfileInfoTable, area), (num * (page - 1)), num)
	if err = sd.cli.Select(&info.CarfileRecords, cmd); err != nil {
		return
	}

	return
}

func (sd sqlDB) GetSucceededCachesCount() (int, error) {
	query := fmt.Sprintf(`SELECT count(carfile_hash) FROM %s WHERE status=?`,
		fmt.Sprintf(cacheInfoTable, sd.replaceArea()))

	var count int
	if err := sd.cli.Get(&count, query, api.CacheStatusSucceeded); err != nil {
		return 0, err
	}

	return count, nil
}

func (sd sqlDB) GetReplicaInfo(id string) (*api.CarfileReplicaInfo, error) {
	area := sd.replaceArea()

	var cache api.CarfileReplicaInfo
	// query := fmt.Sprintf("SELECT * FROM %s WHERE device_id=? AND carfile_hash=?", fmt.Sprintf(cacheInfoTable, area))
	query := fmt.Sprintf("SELECT * FROM %s WHERE id=? ", fmt.Sprintf(cacheInfoTable, area))
	if err := sd.cli.Get(&cache, query, id); err != nil {
		return nil, err
	}

	return &cache, nil
}

func (sd sqlDB) RemoveCarfileRecord(carfileHash string) error {
	area := sd.replaceArea()
	cTableName := fmt.Sprintf(cacheInfoTable, area)
	dTableName := fmt.Sprintf(carfileInfoTable, area)

	tx := sd.cli.MustBegin()
	// cache info
	cCmd := fmt.Sprintf(`DELETE FROM %s WHERE carfile_hash=? `, cTableName)
	tx.MustExec(cCmd, carfileHash)

	// data info
	dCmd := fmt.Sprintf(`DELETE FROM %s WHERE carfile_hash=?`, dTableName)
	tx.MustExec(dCmd, carfileHash)

	err := tx.Commit()
	if err != nil {
		err = tx.Rollback()
		return err
	}

	return nil
}

// remove cache info and update data info
func (sd sqlDB) RemoveCarfileReplica(deviceID, carfileHash string) error {
	area := sd.replaceArea()
	cTableName := fmt.Sprintf(cacheInfoTable, area)
	dTableName := fmt.Sprintf(carfileInfoTable, area)

	tx := sd.cli.MustBegin()

	// cache info
	cCmd := fmt.Sprintf(`DELETE FROM %s WHERE device_id=? AND carfile_hash=?`, cTableName)
	tx.MustExec(cCmd, deviceID, carfileHash)

	var count int
	cmd := fmt.Sprintf("SELECT count(*) FROM %s WHERE carfile_hash=? AND status=? AND is_candidate=?", cTableName)
	err := tx.Get(&count, cmd, carfileHash, api.CacheStatusSucceeded, false)
	if err != nil {
		return err
	}

	dCmd := fmt.Sprintf("UPDATE %s SET cur_reliability=? WHERE carfile_hash=?", dTableName)
	tx.MustExec(dCmd, count, carfileHash)

	err = tx.Commit()
	if err != nil {
		err = tx.Rollback()
		return err
	}

	return nil
}

func (sd sqlDB) UpdateCacheInfoOfQuitNode(deviceIDs []string) (carfileRecords []*api.CarfileRecordInfo, err error) {
	area := sd.replaceArea()
	cTableName := fmt.Sprintf(cacheInfoTable, area)
	dTableName := fmt.Sprintf(carfileInfoTable, area)

	tx := sd.cli.MustBegin()

	// get carfiles
	getCarfilesCmd := fmt.Sprintf(`select * from (
		select carfile_hash from %s WHERE device_id in (?) GROUP BY carfile_hash )as a 
		LEFT JOIN %s as b on a.carfile_hash = b.carfile_hash`, cTableName, dTableName)
	carfilesQuery, args, err := sqlx.In(getCarfilesCmd, deviceIDs)
	if err != nil {
		return
	}

	carfilesQuery = sd.cli.Rebind(carfilesQuery)
	tx.Select(&carfileRecords, carfilesQuery, args...)

	// remove cache
	removeCachesCmd := fmt.Sprintf(`DELETE FROM %s WHERE device_id in (?)`, cTableName)
	removeCacheQuery, args, err := sqlx.In(removeCachesCmd, deviceIDs)
	if err != nil {
		return
	}

	removeCacheQuery = sd.cli.Rebind(removeCacheQuery)
	tx.MustExec(removeCacheQuery, args...)

	// update carfiles record
	for _, carfileRecord := range carfileRecords {
		var count int
		cmd := fmt.Sprintf("SELECT count(*) FROM %s WHERE carfile_hash=? AND status=? AND is_candidate=?", cTableName)
		err := tx.Get(&count, cmd, carfileRecord.CarfileHash, api.CacheStatusSucceeded, false)
		if err != nil {
			continue
		}

		cmdD := fmt.Sprintf(`UPDATE %s SET cur_reliability=? WHERE carfile_hash=?`, dTableName)
		tx.MustExec(cmdD, count, carfileRecord.CarfileHash)
	}

	err = tx.Commit()
	if err != nil {
		err = tx.Rollback()
		return
	}

	return
}

// temporary node register
func (sd sqlDB) BindRegisterInfo(secret, deviceID string, nodeType api.NodeType) error {
	info := api.NodeRegisterInfo{
		Secret:     secret,
		DeviceID:   deviceID,
		NodeType:   int(nodeType),
		CreateTime: time.Now().Format("2006-01-02 15:04:05"),
	}

	_, err := sd.cli.NamedExec(`INSERT INTO register (device_id, secret, create_time, node_type)
	VALUES (:device_id, :secret, :create_time, :node_type)`, info)

	return err
}

func (sd sqlDB) GetRegisterInfo(deviceID, key string, out interface{}) error {
	if key != "" {
		query := fmt.Sprintf(`SELECT %s FROM register WHERE device_id=?`, key)
		// query := "SELECT * FROM register WHERE device_id=?"
		if err := sd.cli.Get(out, query, deviceID); err != nil {
			return err
		}

		return nil
	}

	query := "SELECT * FROM register WHERE device_id=?"
	if err := sd.cli.Get(out, query, deviceID); err != nil {
		return err
	}

	return nil
}

// download info
func (sd sqlDB) SetBlockDownloadInfo(info *api.BlockDownloadInfo) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (id, device_id, block_cid, carfile_cid, block_size, speed, reward, status, failed_reason, client_ip, created_time, complete_time) 
				VALUES (:id, :device_id, :block_cid, :carfile_cid, :block_size, :speed, :reward, :status, :failed_reason, :client_ip, :created_time, :complete_time) ON DUPLICATE KEY UPDATE device_id=:device_id, speed=:speed, reward=:reward, status=:status, failed_reason=:failed_reason, complete_time=:complete_time`, fmt.Sprintf(blockDownloadInfo, sd.replaceArea()))

	_, err := sd.cli.NamedExec(query, info)
	if err != nil {
		return err
	}

	return nil
}

func (sd sqlDB) GetBlockDownloadInfoByDeviceID(deviceID string) ([]*api.BlockDownloadInfo, error) {
	query := fmt.Sprintf(`SELECT * FROM %s WHERE device_id = ? and TO_DAYS(created_time) >= TO_DAYS(NOW()) ORDER BY created_time DESC`,
		fmt.Sprintf(blockDownloadInfo, sd.replaceArea()))

	var out []*api.BlockDownloadInfo
	if err := sd.cli.Select(&out, query, deviceID); err != nil {
		return nil, err
	}

	return out, nil
}

func (sd sqlDB) GetBlockDownloadInfoByID(id string) (*api.BlockDownloadInfo, error) {
	query := fmt.Sprintf(`SELECT * FROM %s WHERE id = ?`,
		fmt.Sprintf(blockDownloadInfo, sd.replaceArea()))

	var out []*api.BlockDownloadInfo
	if err := sd.cli.Select(&out, query, id); err != nil {
		return nil, err
	}

	if len(out) > 0 {
		return out[0], nil
	}
	return nil, nil
}

func (sd sqlDB) GetNodesByUserDownloadBlockIn(minute int) ([]string, error) {
	starTime := time.Now().Add(time.Duration(minute) * time.Minute * -1)

	query := fmt.Sprintf(`SELECT device_id FROM %s WHERE complete_time > ? group by device_id`, fmt.Sprintf(blockDownloadInfo, sd.replaceArea()))

	var out []string
	if err := sd.cli.Select(&out, query, starTime); err != nil {
		return nil, err
	}

	return out, nil
}

func (sd sqlDB) GetCacheInfosWithNode(deviceID string, index, count int) (info *api.NodeCacheRsp, err error) {
	area := sd.replaceArea()

	info = &api.NodeCacheRsp{}

	cmd := fmt.Sprintf("SELECT count(id) FROM %s WHERE device_id=?", fmt.Sprintf(cacheInfoTable, area))
	err = sd.cli.Get(&info.TotalCount, cmd, deviceID)
	if err != nil {
		return
	}

	cmd = fmt.Sprintf("SELECT carfile_hash,status FROM %s WHERE device_id=? LIMIT %d,%d", fmt.Sprintf(cacheInfoTable, area), index, count)
	if err = sd.cli.Select(&info.Caches, cmd, deviceID); err != nil {
		return
	}

	return
}

func (sd sqlDB) SetNodeUpdateInfo(info *api.NodeAppUpdateInfo) error {
	// devInfo := &deviceInfo{DeviceID: deviceID, SchedulerURL: schedulerURL, AreaID: areaID, Online: online}
	sqlString := fmt.Sprintf(`INSERT INTO %s (node_type, app_name, version, hash, download_url) VALUES (:node_type, :app_name, :version, :hash, :download_url) ON DUPLICATE KEY UPDATE app_name=:app_name, version=:version, hash=:hash, download_url=:download_url`, nodeUpdateInfo)
	_, err := sd.cli.NamedExec(sqlString, info)
	return err
}

func (sd sqlDB) GetNodeUpdateInfos() (map[int]*api.NodeAppUpdateInfo, error) {
	query := fmt.Sprintf(`SELECT * FROM %s`, nodeUpdateInfo)

	var out []*api.NodeAppUpdateInfo
	if err := sd.cli.Select(&out, query); err != nil {
		return nil, err
	}

	ret := make(map[int]*api.NodeAppUpdateInfo)
	for _, info := range out {
		ret[info.NodeType] = info
	}
	return ret, nil
}

func (sd sqlDB) DeleteNodeUpdateInfo(nodeType int) error {
	deleteString := fmt.Sprintf(`DELETE FROM %s WHERE node_type=?`, nodeUpdateInfo)
	_, err := sd.cli.Exec(deleteString, nodeType)
	return err
}

// IsNilErr Is NilErr
func (sd sqlDB) IsNilErr(err error) bool {
	return err.Error() == errNotFind
}

func (sd sqlDB) setAllNodeOffline() error {
	info := &NodeInfo{IsOnline: false, ServerID: serverID}
	_, err := sd.cli.NamedExec(`UPDATE node SET is_online=:is_online WHERE server_id=:server_id`, info)

	return err
}

func (sd sqlDB) replaceArea() string {
	str := strings.ToLower(serverArea)
	str = strings.Replace(str, "-", "_", -1)

	return str
}
