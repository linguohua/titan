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
	eventInfoTable    = "event_info_%s"
	dataInfoTable     = "data_info_%s"
	blockInfoTable    = "block_info_%s"
	cacheInfoTable    = "cache_info_%s"
	blockDownloadInfo = "block_download_info_%s"
	messageInfoTable  = "message_info_%s"
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

// IsNilErr Is NilErr
func (sd sqlDB) IsNilErr(err error) bool {
	return err.Error() == errNotFind
}

func (sd sqlDB) SetNodeAuthInfo(aInfo *api.DownloadServerAccessAuth) error {
	info := &NodeInfo{
		URL:        aInfo.URL,
		DeviceID:   aInfo.DeviceID,
		PrivateKey: aInfo.PrivateKey,
	}

	_, err := sd.cli.NamedExec(`UPDATE node SET private_key=:private_key,url=:url WHERE device_id=:device_id`, info)

	return err
}

func (sd sqlDB) GetNodeAuthInfo(deviceID string) (*api.DownloadServerAccessAuth, error) {
	info := &NodeInfo{DeviceID: deviceID}

	rows, err := sd.cli.NamedQuery(`SELECT private_key,url FROM node WHERE device_id=:device_id`, info)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if rows.Next() {
		err = rows.StructScan(info)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, xerrors.New(errNotFind)
	}

	return &api.DownloadServerAccessAuth{URL: info.URL, PrivateKey: info.PrivateKey, DeviceID: deviceID}, err
}

func (sd sqlDB) SetNodeInfo(deviceID string, info *NodeInfo) error {
	info.DeviceID = deviceID
	info.ServerName = serverName
	// info.CreateTime = time.Now().Format("2006-01-02 15:04:05")

	_, err := sd.GetNodeInfo(deviceID)
	if err != nil {
		if sd.IsNilErr(err) {
			_, err = sd.cli.NamedExec(`INSERT INTO node (device_id, last_time, geo, node_type, is_online, address, server_name,private_key, url)
                VALUES (:device_id, :last_time, :geo, :node_type, :is_online, :address, :server_name,:private_key,:url)`, info)
		}
		return err
	}

	// update
	_, err = sd.cli.NamedExec(`UPDATE node SET last_time=:last_time,geo=:geo,is_online=:is_online,address=:address,server_name=:server_name,url=:url  WHERE device_id=:device_id`, info)

	return err
}

func (sd sqlDB) setAllNodeOffline() error {
	info := &NodeInfo{IsOnline: 0, ServerName: serverName}
	_, err := sd.cli.NamedExec(`UPDATE node SET is_online=:is_online WHERE server_name=:server_name`, info)

	return err
}

func (sd sqlDB) GetNodeInfo(deviceID string) (*NodeInfo, error) {
	info := &NodeInfo{DeviceID: deviceID}

	rows, err := sd.cli.NamedQuery(`SELECT * FROM node WHERE device_id=:device_id`, info)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if rows.Next() {
		err = rows.StructScan(info)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, xerrors.New(errNotFind)
	}

	return info, err
}

func (sd sqlDB) SetValidateResultInfo(info *ValidateResult) error {
	nowTime := time.Now().Format("2006-01-02 15:04:05")

	if ValidateStatus(info.Status) == ValidateStatusCreate || ValidateStatus(info.Status) == ValidateStatusOther {
		info.StartTime = nowTime
		info.ServerName = serverName

		_, err := sd.cli.NamedExec(`INSERT INTO validate_result (round_id, device_id, validator_id, status, start_time, server_name, msg)
                VALUES (:round_id, :device_id, :validator_id, :status, :start_time, :server_name, :msg)`, info)
		return err

	} else if ValidateStatus(info.Status) > ValidateStatusCreate {
		info.EndTime = nowTime

		_, err := sd.cli.NamedExec(`UPDATE validate_result SET end_time=:end_time,status=:status,msg=:msg  WHERE device_id=:device_id AND round_id=:round_id`, info)

		return err
	}

	return xerrors.Errorf("SetValidateResultInfo err deviceid:%s ,status:%d, roundID:%s, serverName:%s", info.DeviceID, info.Status, info.RoundID, info.ServerName)
}

func (sd sqlDB) SetNodeToValidateErrorList(sID, deviceID string) error {
	_, err := sd.cli.NamedExec(`INSERT INTO validate_err (round_id, device_id)
                VALUES (:round_id, :device_id)`, map[string]interface{}{
		"round_id":  sID,
		"device_id": deviceID,
	})
	return err
}

func (sd sqlDB) GetBlocksFID(deviceID string) (map[int]string, error) {
	area := sd.ReplaceArea()

	m := make(map[int]string)

	info := &BlockInfo{
		DeviceID: deviceID,
	}

	cmd := fmt.Sprintf(`SELECT * FROM %s WHERE device_id=:device_id`, fmt.Sprintf(blockInfoTable, area))
	rows, err := sd.cli.NamedQuery(cmd, info)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		i := &BlockInfo{}
		err = rows.StructScan(i)
		if err == nil && i.FID > 0 {
			m[i.FID] = i.CID
		}
	}

	return m, nil
}

func (sd sqlDB) GetBlocksInRange(startFid, endFid int, deviceID string) (map[int]string, error) {
	area := sd.ReplaceArea()

	m := make(map[int]string)

	info := &BlockInfo{
		DeviceID: deviceID,
	}

	cmd := fmt.Sprintf(`SELECT cid,fid FROM %s WHERE device_id=:device_id and status=%d and fid between %d and %d`, fmt.Sprintf(blockInfoTable, area), CacheStatusSuccess, startFid, endFid)
	rows, err := sd.cli.NamedQuery(cmd, info)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		i := &BlockInfo{}
		err = rows.StructScan(i)
		if err == nil && i.FID > 0 {
			m[i.FID] = i.CID
		}
	}

	return m, nil
}

func (sd sqlDB) GetBlocksBiggerThan(startFid int, deviceID string) (map[int]string, error) {
	area := sd.ReplaceArea()

	m := make(map[int]string)

	info := &BlockInfo{
		DeviceID: deviceID,
	}

	cmd := fmt.Sprintf(`SELECT cid,fid FROM %s WHERE device_id=:device_id and status=%d and fid > %d`, fmt.Sprintf(blockInfoTable, area), CacheStatusSuccess, startFid)
	rows, err := sd.cli.NamedQuery(cmd, info)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		i := &BlockInfo{}
		err = rows.StructScan(i)
		if err == nil && i.FID > 0 {
			m[i.FID] = i.CID
		}
	}

	return m, nil
}

func (sd sqlDB) GetDeviceBlockNum(deviceID string) (int64, error) {
	area := sd.ReplaceArea()

	var count int64
	cmd := fmt.Sprintf("SELECT count(id) FROM %s WHERE device_id=? AND status=? ;", fmt.Sprintf(blockInfoTable, area))
	err := sd.cli.Get(&count, cmd, deviceID, CacheStatusSuccess)

	return count, err
}

func (sd sqlDB) RemoveCacheInfo(cacheID, carfileHash string, isDeleteData bool, reliability int) error {
	area := sd.ReplaceArea()
	cTableName := fmt.Sprintf(cacheInfoTable, area)
	dTableName := fmt.Sprintf(dataInfoTable, area)
	bTableName := fmt.Sprintf(blockInfoTable, area)

	tx := sd.cli.MustBegin()

	// cache info
	cCmd := fmt.Sprintf(`DELETE FROM %s WHERE cache_id=? `, cTableName)
	tx.MustExec(cCmd, cacheID)

	// data info
	if !isDeleteData {
		dCmd := fmt.Sprintf("UPDATE %s SET reliability=? WHERE carfile_hash=?", dTableName)
		tx.MustExec(dCmd, reliability, carfileHash)
	} else {
		dCmd := fmt.Sprintf(`DELETE FROM %s WHERE carfile_hash=?`, dTableName)
		tx.MustExec(dCmd, carfileHash)
	}

	// delete device block info
	// dCmd := fmt.Sprintf(`DELETE FROM %s WHERE cache_id=?`, fmt.Sprintf(deviceBlockTable, area))
	// tx.MustExec(dCmd, cacheID)

	// delete block info
	bCmd := fmt.Sprintf(`DELETE FROM %s WHERE cache_id=?`, bTableName)
	tx.MustExec(bCmd, cacheID)

	err := tx.Commit()
	if err != nil {
		err = tx.Rollback()
		return err
	}

	return nil
}

func (sd sqlDB) CreateCache(cInfo *CacheInfo) error {
	area := sd.ReplaceArea()
	cTableName := fmt.Sprintf(cacheInfoTable, area)
	// dTableName := fmt.Sprintf(dataInfoTable, area)

	tx := sd.cli.MustBegin()

	// cache info
	cCmd := fmt.Sprintf("INSERT INTO %s (carfile_hash, cache_id, status, expired_time, root_cache) VALUES (?, ?, ?, ?, ?)", cTableName)
	tx.MustExec(cCmd, cInfo.CarfileHash, cInfo.CacheID, cInfo.Status, cInfo.ExpiredTime, cInfo.RootCache)

	err := tx.Commit()
	if err != nil {
		err = tx.Rollback()
		return err
	}

	return nil
}

func (sd sqlDB) SaveCacheEndResults(dInfo *DataInfo, cInfo *CacheInfo) error {
	area := sd.ReplaceArea()
	dTableName := fmt.Sprintf(dataInfoTable, area)
	cTableName := fmt.Sprintf(cacheInfoTable, area)
	bTableName := fmt.Sprintf(blockInfoTable, area)

	tx := sd.cli.MustBegin()

	// data info
	dCmd := fmt.Sprintf("UPDATE %s SET total_size=?,reliability=?,cache_count=?,total_blocks=?,nodes=?,end_time=NOW() WHERE carfile_hash=?", dTableName)
	tx.MustExec(dCmd, dInfo.TotalSize, dInfo.Reliability, dInfo.CacheCount, dInfo.TotalBlocks, dInfo.Nodes, dInfo.CarfileHash)

	// cache info
	cCmd := fmt.Sprintf(`UPDATE %s SET done_size=?,done_blocks=?,reliability=?,status=?,total_size=?,total_blocks=?,nodes=?,end_time=NOW() WHERE cache_id=? `, cTableName)
	tx.MustExec(cCmd, cInfo.DoneSize, cInfo.DoneBlocks, cInfo.Reliability, cInfo.Status, cInfo.TotalSize, cInfo.TotalBlocks, cInfo.Nodes, cInfo.CacheID)

	if cInfo.Status == int(CacheStatusTimeout) {
		// blocks timeout
		bCmd := fmt.Sprintf(`UPDATE %s SET end_time=NOW() WHERE cache_id=? AND status!=?`, bTableName)
		tx.MustExec(bCmd, cInfo.CacheID, CacheStatusSuccess)
	}

	err := tx.Commit()
	if err != nil {
		err = tx.Rollback()
	}

	return err
}

func (sd sqlDB) SaveCacheingResults(dInfo *DataInfo, cInfo *CacheInfo, blockResult *BlockInfo, createBlocks []*BlockInfo) error {
	area := sd.ReplaceArea()
	bTableName := fmt.Sprintf(blockInfoTable, area)
	dTableName := fmt.Sprintf(dataInfoTable, area)
	cTableName := fmt.Sprintf(cacheInfoTable, area)

	tx := sd.cli.MustBegin()

	if dInfo != nil {
		// data info
		dCmd := fmt.Sprintf("UPDATE %s SET total_size=?,reliability=?,cache_count=?,total_blocks=? WHERE carfile_hash=?", dTableName)
		tx.MustExec(dCmd, dInfo.TotalSize, dInfo.Reliability, dInfo.CacheCount, dInfo.TotalBlocks, dInfo.CarfileHash)
	}

	if cInfo != nil {
		// cache info
		cCmd := fmt.Sprintf(`UPDATE %s SET done_size=?,done_blocks=?,reliability=?,status=?,total_size=?,total_blocks=? WHERE cache_id=? `, cTableName)
		tx.MustExec(cCmd, cInfo.DoneSize, cInfo.DoneBlocks, cInfo.Reliability, cInfo.Status, cInfo.TotalSize, cInfo.TotalBlocks, cInfo.CacheID)
	}

	if blockResult != nil {
		// block info
		bCmd := fmt.Sprintf(`UPDATE %s SET status=?,size=?,reliability=?,device_id=?,end_time=NOW() WHERE id=?`, bTableName)
		tx.MustExec(bCmd, blockResult.Status, blockResult.Size, blockResult.Reliability, blockResult.DeviceID, blockResult.ID)
	}

	if createBlocks != nil {
		for _, info := range createBlocks {
			if info.ID != "" {
				cmd := fmt.Sprintf(`UPDATE %s SET status=?,size=?,reliability=?,device_id=?,fid=?,source=? WHERE id=?`, bTableName)
				tx.MustExec(cmd, info.Status, info.Size, info.Reliability, info.DeviceID, info.FID, info.Source, info.ID)
			} else {
				cmd := fmt.Sprintf(`INSERT INTO %s (cache_id, carfile_hash, cid, device_id, status, size, reliability, id, fid, source, cid_hash) VALUES (?, ?, ?, ?, ?, ?, ?, REPLACE(UUID(),"-",""), ?, ?, ?)`, bTableName)
				tx.MustExec(cmd, info.CacheID, info.CarfileHash, info.CID, info.DeviceID, info.Status, info.Size, info.Reliability, info.FID, info.Source, info.CIDHash)
			}
		}
	}

	err := tx.Commit()
	if err != nil {
		err = tx.Rollback()
	}

	return err
}

func (sd sqlDB) SetDataInfo(info *DataInfo) error {
	area := sd.ReplaceArea()

	tableName := fmt.Sprintf(dataInfoTable, area)

	oldInfo, err := sd.GetDataInfo(info.CarfileHash)
	if err != nil && !sd.IsNilErr(err) {
		return err
	}

	if oldInfo == nil {
		cmd := fmt.Sprintf("INSERT INTO %s (carfile_hash, carfile_cid, status, need_reliability, total_blocks, expired_time) VALUES (:carfile_hash, :carfile_cid, :status, :need_reliability, :total_blocks, :expired_time)", tableName)
		_, err = sd.cli.NamedExec(cmd, info)
		return err
	}

	// update
	cmd := fmt.Sprintf("UPDATE %s SET expired_time=:expired_time,status=:status,total_size=:total_size,reliability=:reliability,cache_count=:cache_count,total_blocks=:total_blocks,need_reliability=:need_reliability WHERE carfile_hash=:carfile_hash", tableName)
	_, err = sd.cli.NamedExec(cmd, info)

	return err
}

func (sd sqlDB) GetDataInfo(hash string) (*DataInfo, error) {
	area := sd.ReplaceArea()

	info := &DataInfo{CarfileHash: hash}

	cmd := fmt.Sprintf("SELECT * FROM %s WHERE carfile_hash=:carfile_hash", fmt.Sprintf(dataInfoTable, area))

	rows, err := sd.cli.NamedQuery(cmd, info)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if rows.Next() {
		err = rows.StructScan(info)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, xerrors.New(errNotFind)
	}

	return info, err
}

func (sd sqlDB) GetDataCidWithPage(page int) (count int, totalPage int, list []string, err error) {
	area := sd.ReplaceArea()
	p := 20

	cmd := fmt.Sprintf("SELECT count(carfile_cid) FROM %s ;", fmt.Sprintf(dataInfoTable, area))
	err = sd.cli.Get(&count, cmd)
	if err != nil {
		return
	}

	totalPage = count / p
	if count%p > 0 {
		totalPage++
	}

	if totalPage == 0 {
		return
	}

	if page > totalPage {
		page = totalPage
	}

	cmd = fmt.Sprintf("SELECT carfile_cid FROM %s LIMIT %d,%d", fmt.Sprintf(dataInfoTable, area), (p * (page - 1)), p)
	if err = sd.cli.Select(&list, cmd); err != nil {
		return
	}

	return
}

func (sd sqlDB) ChangeExpiredTimeWhitCaches(carfileHash, cacheID string, expiredTime time.Time) error {
	tx := sd.cli.MustBegin()

	if cacheID == "" {
		// cmd := fmt.Sprintf(`UPDATE %s SET expired_time=DATE_ADD(expired_time,interval ? HOUR) WHERE carfile_hash=?`, fmt.Sprintf(cacheInfoTable, sd.ReplaceArea()))
		cmd := fmt.Sprintf(`UPDATE %s SET expired_time=? WHERE carfile_hash=?`, fmt.Sprintf(cacheInfoTable, sd.ReplaceArea()))
		tx.MustExec(cmd, expiredTime, carfileHash)
	} else {
		// cmd := fmt.Sprintf(`UPDATE %s SET expired_time=DATE_ADD(expired_time,interval ? HOUR) WHERE carfile_hash=? AND cache_id=?`, fmt.Sprintf(cacheInfoTable, sd.ReplaceArea()))
		cmd := fmt.Sprintf(`UPDATE %s SET expired_time=? WHERE carfile_hash=? AND cache_id=?`, fmt.Sprintf(cacheInfoTable, sd.ReplaceArea()))
		tx.MustExec(cmd, expiredTime, carfileHash, cacheID)
	}

	// cmdData := fmt.Sprintf(`UPDATE %s SET expired_time=? WHERE carfile_hash=?`, fmt.Sprintf(dataInfoTable, sd.ReplaceArea()))
	// tx.MustExec(cmdData, time, carfileHash)

	err := tx.Commit()
	if err != nil {
		err = tx.Rollback()
		return err
	}

	return nil
}

func (sd sqlDB) GetExpiredCaches() ([]*CacheInfo, error) {
	query := fmt.Sprintf(`SELECT carfile_hash,cache_id FROM %s WHERE TO_DAYS(expired_time) <= TO_DAYS(NOW())`,
		fmt.Sprintf(cacheInfoTable, sd.ReplaceArea()))

	var out []*CacheInfo
	if err := sd.cli.Select(&out, query); err != nil {
		return nil, err
	}

	return out, nil
}

func (sd sqlDB) GetCachesWithData(hash string) ([]string, error) {
	area := sd.ReplaceArea()

	list := make([]string, 0)

	i := &CacheInfo{CarfileHash: hash}

	cmd := fmt.Sprintf(`SELECT cache_id FROM %s WHERE carfile_hash=:carfile_hash`, fmt.Sprintf(cacheInfoTable, area))
	rows, err := sd.cli.NamedQuery(cmd, i)
	if err != nil {
		return list, err
	}
	defer rows.Close()

	for rows.Next() {
		info := &CacheInfo{}
		err := rows.StructScan(info)
		if err == nil {
			list = append(list, info.CacheID)
		}
	}

	return list, err
}

func (sd sqlDB) GetCacheInfo(cacheID string) (*CacheInfo, error) {
	area := sd.ReplaceArea()

	info := &CacheInfo{
		CacheID: cacheID,
	}

	cmd := fmt.Sprintf(`SELECT * FROM %s WHERE cache_id=:cache_id`, fmt.Sprintf(cacheInfoTable, area))

	rows, err := sd.cli.NamedQuery(cmd, info)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if rows.Next() {
		err = rows.StructScan(info)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, xerrors.New(errNotFind)
	}

	return info, err
}

func (sd sqlDB) GetBlockInfo(cacheID, hash, deviceID string) (*BlockInfo, error) {
	area := sd.ReplaceArea()

	info := &BlockInfo{
		CacheID:  cacheID,
		CIDHash:  hash,
		DeviceID: deviceID,
	}

	cmd := fmt.Sprintf(`SELECT * FROM %s WHERE cache_id=:cache_id AND cid_hash=:cid_hash AND device_id=:device_id`, fmt.Sprintf(blockInfoTable, area))

	rows, err := sd.cli.NamedQuery(cmd, info)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if rows.Next() {
		err = rows.StructScan(info)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, xerrors.New(errNotFind)
	}

	return info, err
}

func (sd sqlDB) GetUndoneBlocks(cacheID string) (map[string]string, error) {
	area := sd.ReplaceArea()

	list := make(map[string]string, 0)

	i := &BlockInfo{CacheID: cacheID, Status: int(CacheStatusSuccess)}

	cmd := fmt.Sprintf(`SELECT * FROM %s WHERE cache_id=:cache_id AND status!=:status`, fmt.Sprintf(blockInfoTable, area))
	rows, err := sd.cli.NamedQuery(cmd, i)
	if err != nil {
		return list, err
	}
	defer rows.Close()

	for rows.Next() {
		info := &BlockInfo{}
		err := rows.StructScan(info)
		if err == nil {
			list[info.CID] = info.ID
			// list = append(list, info.CID)
		}
	}

	return list, err
}

func (sd sqlDB) GetAllBlocks(cacheID string) ([]*BlockInfo, error) {
	area := sd.ReplaceArea()

	query := fmt.Sprintf(`SELECT * FROM %s WHERE cache_id=?`, fmt.Sprintf(blockInfoTable, area))
	var out []*BlockInfo
	if err := sd.cli.Select(&out, query, cacheID); err != nil {
		return nil, err
	}

	return out, nil
}

func (sd sqlDB) GetBloackCountWithStatus(cacheID string, status int) (int, error) {
	area := sd.ReplaceArea()

	var count int
	cmd := fmt.Sprintf("SELECT count(id) FROM %s WHERE cache_id=? AND status=?;", fmt.Sprintf(blockInfoTable, area))
	err := sd.cli.Get(&count, cmd, cacheID, status)

	return count, err
}

func (sd sqlDB) GetCachesSize(cacheID string, status int) (int, error) {
	area := sd.ReplaceArea()

	var count int
	cmd := fmt.Sprintf("SELECT sum(size) FROM %s WHERE cache_id=? AND status=?;", fmt.Sprintf(blockInfoTable, area))
	err := sd.cli.Get(&count, cmd, cacheID, status)

	return count, err
}

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

func (sd sqlDB) GetRegisterInfo(deviceID string) (*api.NodeRegisterInfo, error) {
	info := &api.NodeRegisterInfo{
		DeviceID: deviceID,
	}

	rows, err := sd.cli.NamedQuery(`SELECT * FROM register WHERE device_id=:device_id`, info)
	if err != nil {
		return info, err
	}
	defer rows.Close()
	if rows.Next() {
		err = rows.StructScan(info)
		if err != nil {
			return info, err
		}
	} else {
		return info, xerrors.New(errNotFind)
	}

	return info, err
}

func (sd sqlDB) GetNodesWithCache(hash string, isSuccess bool) ([]string, error) {
	area := sd.ReplaceArea()

	var out []string
	if isSuccess {
		cmd := fmt.Sprintf(`SELECT device_id FROM %s WHERE cid_hash=? AND status=?`, fmt.Sprintf(blockInfoTable, area))
		if err := sd.cli.Select(&out, cmd, hash, int(CacheStatusSuccess)); err != nil {
			return nil, err
		}
	} else {
		cmd := fmt.Sprintf(`SELECT device_id FROM %s WHERE cid_hash=? `, fmt.Sprintf(blockInfoTable, area))
		if err := sd.cli.Select(&out, cmd, hash); err != nil {
			return nil, err
		}
	}

	return out, nil
}

func (sd sqlDB) ReplaceArea() string {
	str := strings.ToLower(serverArea)
	str = strings.Replace(str, "-", "_", -1)

	return str
}

func (sd sqlDB) GetNodesFromData(hash string) (int, error) {
	area := sd.ReplaceArea()

	query := fmt.Sprintf("SELECT DISTINCT device_id FROM %s WHERE carfile_hash=?", fmt.Sprintf(blockInfoTable, area))
	var out []*BlockInfo
	if err := sd.cli.Select(&out, query, hash); err != nil {
		return 0, err
	}

	return len(out), nil
}

func (sd sqlDB) GetNodesFromCache(cacheID string) (int, error) {
	area := sd.ReplaceArea()

	query := fmt.Sprintf("SELECT DISTINCT device_id FROM %s WHERE cache_id=?", fmt.Sprintf(blockInfoTable, area))
	var out []*BlockInfo
	if err := sd.cli.Select(&out, query, cacheID); err != nil {
		return 0, err
	}

	return len(out), nil
}

func (sd sqlDB) AddDownloadInfo(info *api.BlockDownloadInfo) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (block_cid, device_id, block_size, speed, reward) 
				VALUES (:block_cid, :device_id, :block_size, :speed, :reward)`, fmt.Sprintf(blockDownloadInfo, sd.ReplaceArea()))

	_, err := sd.cli.NamedExec(query, info)
	if err != nil {
		return err
	}

	return nil
}

func (sd sqlDB) GetDownloadInfoByDeviceID(deviceID string) ([]*api.BlockDownloadInfo, error) {
	query := fmt.Sprintf(`SELECT * FROM %s WHERE device_id = ? and created_time >= TO_DAYS(NOW()) ORDER BY created_time DESC`,
		fmt.Sprintf(blockDownloadInfo, sd.ReplaceArea()))

	var out []*api.BlockDownloadInfo
	if err := sd.cli.Select(&out, query, deviceID); err != nil {
		return nil, err
	}

	return out, nil
}

func (sd sqlDB) SetEventInfo(info *api.EventInfo) error {
	area := sd.ReplaceArea()

	tableName := fmt.Sprintf(eventInfoTable, area)

	cmd := fmt.Sprintf("INSERT INTO %s (cid, device_id, user, event, msg, cache_id) VALUES (:cid, :device_id, :user, :event, :msg, :cache_id)", tableName)
	_, err := sd.cli.NamedExec(cmd, info)
	return err
}

func (sd sqlDB) GetEventInfos(page int) (count int, totalPage int, out []api.EventInfo, err error) {
	area := "cn_gd_shenzhen"
	p := 20

	cmd := fmt.Sprintf("SELECT count(cid) FROM %s ;", fmt.Sprintf(eventInfoTable, area))
	err = sd.cli.Get(&count, cmd)
	if err != nil {
		return
	}

	totalPage = count / p
	if count%p > 0 {
		totalPage++
	}

	if totalPage == 0 {
		return
	}

	if page > totalPage {
		page = totalPage
	}

	cmd = fmt.Sprintf("SELECT * FROM %s LIMIT %d,%d", fmt.Sprintf(eventInfoTable, area), (p * (page - 1)), p)
	if err = sd.cli.Select(&out, cmd); err != nil {
		return
	}

	return
}

func (sd sqlDB) SetMessageInfo(infos []*MessageInfo) error {
	area := sd.ReplaceArea()
	tableName := fmt.Sprintf(messageInfoTable, area)

	tx := sd.cli.MustBegin()

	for _, info := range infos {
		if info.ID != "" {
			cmd := fmt.Sprintf(`UPDATE %s SET cid=?,target=?,cache_id=?,carfile_cid=?,status=?,size=?,msg_type=?,source=?,created_time=?,end_time=? WHERE id=?`, tableName)
			tx.MustExec(cmd, info.CID, info.Target, info.CacheID, info.CarfileCid, info.Status, info.Size, info.Type, info.Source, info.CreateTime, info.EndTime, info.ID)
		} else {
			cmd := fmt.Sprintf(`INSERT INTO %s (cid, target, cache_id, carfile_cid, status, size, msg_type, source, created_time, end_time, id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, REPLACE(UUID(),"-",""))`, tableName)
			tx.MustExec(cmd, info.CID, info.Target, info.CacheID, info.CarfileCid, info.Status, info.Size, info.Type, info.Source, info.CreateTime, info.EndTime)
		}
	}

	err := tx.Commit()
	if err != nil {
		err = tx.Rollback()
	}

	return err
}

func (sd sqlDB) GetDownloadInfoBySN(sn int64) (*api.BlockDownloadInfo, error) {
	query := fmt.Sprintf(`SELECT * FROM %s WHERE sn = ?`, fmt.Sprintf(blockDownloadInfo, sd.ReplaceArea()))

	var out *api.BlockDownloadInfo
	if err := sd.cli.Select(&out, query, sn); err != nil {
		return nil, err
	}

	return out, nil
}
