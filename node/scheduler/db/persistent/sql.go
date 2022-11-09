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

const errNodeNotFind = "Not Found"

var (
	deviceBlockTable  = "device_blocks_%s"
	dataInfoTable     = "data_info_%s"
	blockInfoTable    = "block_info_%s"
	cacheInfoTable    = "cache_info_%s"
	blockDownloadInfo = "block_download_info_%s"
)

// InitSQL init sql
func InitSQL(url string) (DB, error) {
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
	return err.Error() == errNodeNotFind
}

func (sd sqlDB) SetNodeAuthInfo(aInfo *api.DownloadServerAccessAuth) error {
	info := &NodeInfo{
		URL:         aInfo.URL,
		DeviceID:    aInfo.DeviceID,
		SecurityKey: aInfo.SecurityKey,
	}

	_, err := sd.cli.NamedExec(`UPDATE node SET security=:security,url=:url WHERE device_id=:device_id`, info)

	return err
}

func (sd sqlDB) GetNodeAuthInfo(deviceID string) (*api.DownloadServerAccessAuth, error) {
	info := &NodeInfo{DeviceID: deviceID}

	rows, err := sd.cli.NamedQuery(`SELECT security,url FROM node WHERE device_id=:device_id`, info)
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
		return nil, xerrors.New(errNodeNotFind)
	}

	return &api.DownloadServerAccessAuth{URL: info.URL, SecurityKey: info.SecurityKey, DeviceID: deviceID}, err
}

func (sd sqlDB) SetNodeInfo(deviceID string, info *NodeInfo) error {
	info.DeviceID = deviceID
	info.ServerName = serverName
	info.CreateTime = time.Now().Format("2006-01-02 15:04:05")

	_, err := sd.GetNodeInfo(deviceID)
	if err != nil {
		if sd.IsNilErr(err) {
			_, err = sd.cli.NamedExec(`INSERT INTO node (device_id, last_time, geo, node_type, is_online, address, server_name, create_time)
                VALUES (:device_id, :last_time, :geo, :node_type, :is_online, :address, :server_name, :create_time)`, info)
		}
		return err
	}

	// update
	_, err = sd.cli.NamedExec(`UPDATE node SET last_time=:last_time,geo=:geo,is_online=:is_online,address=:address,server_name=:server_name  WHERE device_id=:device_id`, info)

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
		return nil, xerrors.New(errNodeNotFind)
	}

	return info, err
}

func (sd sqlDB) SetValidateResultInfo(info *ValidateResult) error {
	nowTime := time.Now().Format("2006-01-02 15:04:05")

	if ValidateStatus(info.Status) == ValidateStatusCreate || ValidateStatus(info.Status) == ValidateStatusOther {
		info.StratTime = nowTime
		info.ServerName = serverName

		_, err := sd.cli.NamedExec(`INSERT INTO validate_result (round_id, device_id, validator_id, status, strat_time, server_name, msg)
                VALUES (:round_id, :device_id, :validator_id, :status, :strat_time, :server_name, :msg)`, info)
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

func (sd sqlDB) GetBlocksFID(deviceID string) (map[string]string, error) {
	area := sd.ReplaceArea()

	m := make(map[string]string)

	info := &NodeBlocks{
		DeviceID: deviceID,
	}

	cmd := fmt.Sprintf(`SELECT * FROM %s WHERE device_id=:device_id`, fmt.Sprintf(deviceBlockTable, area))
	rows, err := sd.cli.NamedQuery(cmd, info)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		i := &NodeBlocks{}
		err = rows.StructScan(i)
		if err == nil {
			m[i.FID] = i.CID
		}
	}

	return m, nil
}

func (sd sqlDB) GetBlockCidWithFid(deviceID, fid string) (string, error) {
	area := sd.ReplaceArea()

	info := &NodeBlocks{
		FID:      fid,
		DeviceID: deviceID,
	}

	cmd := fmt.Sprintf(`SELECT * FROM %s WHERE fid=:fid AND device_id=:device_id`, fmt.Sprintf(deviceBlockTable, area))
	rows, err := sd.cli.NamedQuery(cmd, info)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	if rows.Next() {
		err = rows.StructScan(info)
		if err != nil {
			return "", err
		}
	} else {
		return "", xerrors.New(errNodeNotFind)
	}

	return info.CID, err
}

func (sd sqlDB) GetDeviceBlockNum(deviceID string) (int64, error) {
	area := sd.ReplaceArea()

	var count int64
	cmd := fmt.Sprintf("SELECT count(*) FROM %s WHERE device_id=? ;", fmt.Sprintf(deviceBlockTable, area))
	err := sd.cli.Get(&count, cmd, deviceID)

	return count, err
}

func (sd sqlDB) RemoveAndUpdateCacheInfo(cacheID, carfileID, rootCacheID string, isDeleteData bool, reliability int) error {
	area := sd.ReplaceArea()
	cTableName := fmt.Sprintf(cacheInfoTable, area)
	dTableName := fmt.Sprintf(dataInfoTable, area)
	bTableName := fmt.Sprintf(blockInfoTable, area)

	tx := sd.cli.MustBegin()

	// cache info
	cmd1 := fmt.Sprintf(`DELETE FROM %s WHERE cache_id=? AND carfile_id=?`, cTableName)
	tx.MustExec(cmd1, cacheID, carfileID)

	// data info
	if !isDeleteData {
		dCmd := fmt.Sprintf("UPDATE %s SET reliability=?,root_cache_id=? WHERE cid=?", dTableName)
		tx.MustExec(dCmd, reliability, rootCacheID, carfileID)
	} else {
		cmd1 := fmt.Sprintf(`DELETE FROM %s WHERE cid=?`, dTableName)
		tx.MustExec(cmd1, carfileID)
	}

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
	cCmd := fmt.Sprintf("INSERT INTO %s (carfile_id, cache_id, status) VALUES (?, ?, ?)", cTableName)
	tx.MustExec(cCmd, cInfo.CarfileID, cInfo.CacheID, cInfo.Status)

	// data info
	// dCmd := fmt.Sprintf("UPDATE %s SET cache_ids=? WHERE cid=?", dTableName)
	// tx.MustExec(dCmd, dInfo.CacheIDs, dInfo.CID)

	err := tx.Commit()
	if err != nil {
		err = tx.Rollback()
		return err
	}

	// info, err := sd.GetCacheInfo(cInfo.CacheID, cInfo.CarfileID)
	// if err != nil {
	// 	return 0, err
	// }

	return nil
}

func (sd sqlDB) SaveCacheEndResults(dInfo *DataInfo, cInfo *CacheInfo) error {
	area := sd.ReplaceArea()
	dTableName := fmt.Sprintf(dataInfoTable, area)
	cTableName := fmt.Sprintf(cacheInfoTable, area)

	tx := sd.cli.MustBegin()

	// data info
	dCmd := fmt.Sprintf("UPDATE %s SET total_size=?,reliability=?,cache_count=?,root_cache_id=?,total_blocks=?,nodes=? WHERE cid=?", dTableName)
	tx.MustExec(dCmd, dInfo.TotalSize, dInfo.Reliability, dInfo.CacheCount, dInfo.RootCacheID, dInfo.TotalBlocks, cInfo.Nodes, dInfo.CID)

	// cache info
	cCmd := fmt.Sprintf(`UPDATE %s SET done_size=?,done_blocks=?,reliability=?,status=?,total_size=?,total_blocks=?,nodes=? WHERE cache_id=? `, cTableName)
	tx.MustExec(cCmd, cInfo.DoneSize, cInfo.DoneBlocks, cInfo.Reliability, cInfo.Status, cInfo.TotalSize, cInfo.TotalBlocks, cInfo.Nodes, cInfo.CacheID)

	err := tx.Commit()
	if err != nil {
		err = tx.Rollback()
	}

	return err
}

func (sd sqlDB) SaveCacheingResults(dInfo *DataInfo, cInfo *CacheInfo, updateBlock *BlockInfo, fid string, createBlocks []*BlockInfo) error {
	area := sd.ReplaceArea()
	bTableName := fmt.Sprintf(blockInfoTable, area)
	dTableName := fmt.Sprintf(dataInfoTable, area)
	cTableName := fmt.Sprintf(cacheInfoTable, area)

	tx := sd.cli.MustBegin()

	// data info
	dCmd := fmt.Sprintf("UPDATE %s SET total_size=?,reliability=?,cache_count=?,root_cache_id=?,total_blocks=? WHERE cid=?", dTableName)
	tx.MustExec(dCmd, dInfo.TotalSize, dInfo.Reliability, dInfo.CacheCount, dInfo.RootCacheID, dInfo.TotalBlocks, dInfo.CID)

	// cache info
	cCmd := fmt.Sprintf(`UPDATE %s SET done_size=?,done_blocks=?,reliability=?,status=?,total_size=?,total_blocks=? WHERE cache_id=? `, cTableName)
	tx.MustExec(cCmd, cInfo.DoneSize, cInfo.DoneBlocks, cInfo.Reliability, cInfo.Status, cInfo.TotalSize, cInfo.TotalBlocks, cInfo.CacheID)

	// block info
	bCmd := fmt.Sprintf(`UPDATE %s SET status=?,size=?,reliability=?,device_id=? WHERE id=?`, bTableName)
	tx.MustExec(bCmd, updateBlock.Status, updateBlock.Size, updateBlock.Reliability, updateBlock.DeviceID, updateBlock.ID)

	if fid != "" {
		cmd1 := fmt.Sprintf(`INSERT INTO %s (cid, fid, cache_id, carfile_id, device_id) VALUES (?, ?, ?, ?, ?)`, fmt.Sprintf(deviceBlockTable, area))
		tx.MustExec(cmd1, updateBlock.CID, fid, updateBlock.CacheID, cInfo.CarfileID, updateBlock.DeviceID)
		// 	cmd1 := fmt.Sprintf(`UPDATE %s SET fid=? WHERE cid=? AND carfile_id=? AND cache_id=? AND device_id=?`, fmt.Sprintf(deviceBlockTable, area))
		// 	tx.MustExec(cmd1, fid, updateBlock.CID, cInfo.CarfileID, updateBlock.CacheID, updateBlock.DeviceID)
		// } else {
		// 	cmd1 := fmt.Sprintf(`DELETE FROM %s WHERE cid=? AND carfile_id=? AND cache_id=? AND device_id=?`, fmt.Sprintf(deviceBlockTable, area))
		// 	tx.MustExec(cmd1, updateBlock.CID, cInfo.CarfileID, updateBlock.CacheID, updateBlock.DeviceID)
	}

	if createBlocks != nil {
		for _, info := range createBlocks {
			if info.ID != 0 {
				cmd := fmt.Sprintf(`UPDATE %s SET status=?,size=?,reliability=?,device_id=? WHERE id=?`, bTableName)
				tx.MustExec(cmd, info.Status, info.Size, info.Reliability, info.DeviceID, info.ID)
			} else {
				cmd := fmt.Sprintf(`INSERT INTO %s (cache_id, cid, device_id, status, size, reliability) VALUES (?, ?, ?, ?, ?, ?)`, bTableName)
				tx.MustExec(cmd, info.CacheID, info.CID, info.DeviceID, info.Status, info.Size, info.Reliability)
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

	oldInfo, err := sd.GetDataInfo(info.CID)
	if err != nil {
		return err
	}

	if oldInfo == nil {
		cmd := fmt.Sprintf("INSERT INTO %s (cid, status, need_reliability, total_blocks) VALUES (:cid, :status, :need_reliability, :total_blocks)", tableName)
		_, err = sd.cli.NamedExec(cmd, info)
		return err
	}

	// update
	cmd := fmt.Sprintf("UPDATE %s SET status=:status,total_size=:total_size,reliability=:reliability,cache_count=:cache_count,root_cache_id=:root_cache_id,total_blocks=:total_blocks WHERE cid=:cid", tableName)
	_, err = sd.cli.NamedExec(cmd, info)

	return err
}

func (sd sqlDB) GetDataInfo(cid string) (*DataInfo, error) {
	area := sd.ReplaceArea()

	info := &DataInfo{CID: cid}

	cmd := fmt.Sprintf("SELECT * FROM %s WHERE cid=:cid", fmt.Sprintf(dataInfoTable, area))

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
		return nil, nil
	}

	return info, err
}

func (sd sqlDB) GetDataCidWithPage(page int) (count int, totalPage int, list []string, err error) {
	area := sd.ReplaceArea()
	p := 20

	cmd := fmt.Sprintf("SELECT count(*) FROM %s ;", fmt.Sprintf(dataInfoTable, area))
	err = sd.cli.Get(&count, cmd)
	if err != nil {
		return
	}

	totalPage = count / p
	if count%p > 0 {
		totalPage++
	}

	if page > totalPage {
		err = xerrors.Errorf("totalPage:%d but page:%d", totalPage, page)
		return
	}

	cmd = fmt.Sprintf("SELECT * FROM %s WHERE id>=(SELECT id FROM %s order by id limit %d,1) LIMIT %d", fmt.Sprintf(dataInfoTable, area), fmt.Sprintf(dataInfoTable, area), (p * (page - 1)), p)
	rows, err := sd.cli.Queryx(cmd)
	if err != nil {
		return
	}
	defer rows.Close()

	list = make([]string, 0)
	for rows.Next() {
		info := &DataInfo{}
		err := rows.StructScan(info)
		if err == nil {
			list = append(list, info.CID)
		}
	}

	return
}

func (sd sqlDB) GetCacheWhitData(cid string) ([]string, error) {
	area := sd.ReplaceArea()

	list := make([]string, 0)

	i := &CacheInfo{CarfileID: cid}

	cmd := fmt.Sprintf(`SELECT cache_id FROM %s WHERE carfile_id=:carfile_id`, fmt.Sprintf(cacheInfoTable, area))
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

func (sd sqlDB) GetCacheInfo(cacheID, carFileCid string) (*CacheInfo, error) {
	area := sd.ReplaceArea()

	info := &CacheInfo{
		CacheID:   cacheID,
		CarfileID: carFileCid,
	}

	cmd := fmt.Sprintf(`SELECT * FROM %s WHERE carfile_id=:carfile_id AND cache_id=:cache_id`, fmt.Sprintf(cacheInfoTable, area))

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
		return nil, nil
	}

	return info, err
}

func (sd sqlDB) SetBlockInfos(infos []*BlockInfo, carfileCid string) error {
	area := sd.ReplaceArea()
	tableName := fmt.Sprintf(blockInfoTable, area)

	tx := sd.cli.MustBegin()

	for _, info := range infos {
		if info.ID != 0 {
			cmd := fmt.Sprintf(`UPDATE %s SET status=?,size=?,reliability=?,device_id=? WHERE id=?`, tableName)
			tx.MustExec(cmd, info.Status, info.Size, info.Reliability, info.DeviceID, info.ID)

			// cmd1 := fmt.Sprintf(`UPDATE %s SET device_id=? WHERE cid=? AND cache_id=? AND carfile_id=?,`, fmt.Sprintf(deviceBlockTable, area))
			// tx.MustExec(cmd1, info.DeviceID, info.CID, info.CacheID, carfileCid)
		} else {
			cmd := fmt.Sprintf(`INSERT INTO %s (cache_id, cid, device_id, status, size, reliability) VALUES (?, ?, ?, ?, ?, ?)`, tableName)
			tx.MustExec(cmd, info.CacheID, info.CID, info.DeviceID, info.Status, info.Size, info.Reliability)

			// cmd1 := fmt.Sprintf(`INSERT INTO %s (cid, fid, cache_id, carfile_id, device_id) VALUES (?, ?, ?, ?, ?)`, fmt.Sprintf(deviceBlockTable, area))
			// tx.MustExec(cmd1, info.CID, "", info.CacheID, carfileCid, info.DeviceID)
		}
	}

	err := tx.Commit()
	if err != nil {
		err = tx.Rollback()
	}

	return err
}

func (sd sqlDB) GetBlockInfo(cacheID, cid, deviceID string) (*BlockInfo, error) {
	area := sd.ReplaceArea()

	info := &BlockInfo{
		CacheID:  cacheID,
		CID:      cid,
		DeviceID: deviceID,
	}

	cmd := fmt.Sprintf(`SELECT * FROM %s WHERE cache_id=:cache_id AND cid=:cid AND device_id=:device_id`, fmt.Sprintf(blockInfoTable, area))

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
		return nil, nil
	}

	return info, err
}

func (sd sqlDB) GetUndoneBlocks(cacheID string) (map[string]int, error) {
	area := sd.ReplaceArea()

	list := make(map[string]int, 0)

	i := &BlockInfo{CacheID: cacheID, Status: 3}

	cmd := fmt.Sprintf(`SELECT * FROM %s WHERE cache_id=:cache_id AND status<:status`, fmt.Sprintf(blockInfoTable, area))
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

func (sd sqlDB) GetAllBlocks(cacheID string) (map[string][]string, error) {
	area := sd.ReplaceArea()

	list := make(map[string][]string, 0)

	i := &BlockInfo{CacheID: cacheID}

	cmd := fmt.Sprintf(`SELECT * FROM %s WHERE cache_id=:cache_id`, fmt.Sprintf(blockInfoTable, area))
	rows, err := sd.cli.NamedQuery(cmd, i)
	if err != nil {
		return list, err
	}
	defer rows.Close()

	for rows.Next() {
		info := &BlockInfo{}
		err := rows.StructScan(info)
		if err == nil {
			cids, ok := list[info.DeviceID]
			if !ok {
				cids = make([]string, 0)
			}
			cids = append(cids, info.CID)
			list[info.DeviceID] = cids
			// list = append(list, info.CID)
		}
	}

	return list, err
}

func (sd sqlDB) GetBloackCountWhitStatus(cacheID string, status int) (int, error) {
	area := sd.ReplaceArea()

	var count int
	cmd := fmt.Sprintf("SELECT count(*) FROM %s WHERE cache_id=? AND status=?;", fmt.Sprintf(blockInfoTable, area))
	err := sd.cli.Get(&count, cmd, cacheID, status)

	return count, err

	// rows, err := sd.cli.NamedQuery(cmd, info)
	// if err != nil {
	// 	return false, err
	// }
	// defer rows.Close()

	// if rows.Next() {
	// 	return true, nil
	// }

	// return false, nil
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
		return info, xerrors.New(errNodeNotFind)
	}

	return info, err
}

func (sd sqlDB) GetNodesWithCacheList(cid string) ([]string, error) {
	area := sd.ReplaceArea()

	m := make([]string, 0)

	info := &NodeBlocks{
		CID: cid,
	}

	cmd := fmt.Sprintf(`SELECT * FROM %s WHERE cid=:cid`, fmt.Sprintf(deviceBlockTable, area))

	rows, err := sd.cli.NamedQuery(cmd, info)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		i := &NodeBlocks{}
		err = rows.StructScan(i)
		if err != nil {
			return m, err
		}
		m = append(m, i.DeviceID)
	}

	return m, nil
}

func (sd sqlDB) DeleteBlockInfos(carfileID, cacheID, deviceID string, cids []string) error {
	area := sd.ReplaceArea()
	bTableName := fmt.Sprintf(blockInfoTable, area)
	if cids == nil || len(cids) < 0 {
		return nil
	}

	tx := sd.cli.MustBegin()
	for _, cid := range cids {
		// delete device block info
		cmd1 := fmt.Sprintf(`DELETE FROM %s WHERE cid=? AND device_id=? AND cache_id=? AND carfile_id=?`, fmt.Sprintf(deviceBlockTable, area))
		tx.MustExec(cmd1, cid, deviceID, cacheID, carfileID)
		// delete block info
		bCmd := fmt.Sprintf(`DELETE FROM %s WHERE cid=? AND device_id=? AND cache_id=?`, bTableName)
		tx.MustExec(bCmd, cid, deviceID, cacheID)
	}

	err := tx.Commit()
	if err != nil {
		err = tx.Rollback()
	}

	return err
}

func (sd sqlDB) ReplaceArea() string {
	str := strings.ToLower(serverArea)
	str = strings.Replace(str, "-", "_", -1)

	return str
}

func (sd sqlDB) GetDevicesFromData(cid string) (int, error) {
	area := sd.ReplaceArea()
	// select DISTINCT size from block_info_cn_gd_shenzhen where cache_id='cn_gd_shenzhen_cache_1';
	i := &BlockInfo{CID: cid}

	cmd := fmt.Sprintf("SELECT DISTINCT device_id FROM %s WHERE cid=:cid", fmt.Sprintf(blockInfoTable, area))
	rows, err := sd.cli.NamedQuery(cmd, i)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	devices := 0

	for rows.Next() {
		info := &BlockInfo{}
		err := rows.StructScan(info)
		if err == nil {
			devices++
		}
	}

	return devices, nil
}

func (sd sqlDB) GetDevicesFromCache(cacheID string) (int, error) {
	area := sd.ReplaceArea()
	// select DISTINCT size from block_info_cn_gd_shenzhen where cache_id='cn_gd_shenzhen_cache_1';
	i := &BlockInfo{CacheID: cacheID}

	cmd := fmt.Sprintf("SELECT DISTINCT device_id FROM %s WHERE cache_id=:cache_id", fmt.Sprintf(blockInfoTable, area))
	rows, err := sd.cli.NamedQuery(cmd, i)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	devices := 0

	for rows.Next() {
		info := &BlockInfo{}
		err := rows.StructScan(info)
		if err == nil {
			devices++
		}
	}

	return devices, nil
}

func (sd sqlDB) AddDownloadInfo(deviceID string, info *api.BlockDownloadInfo) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (block_cid, device_id, block_size, speed, reward) 
				VALUES (:block_cid, :device_id, :block_size, :speed, :reward)`, fmt.Sprintf(blockDownloadInfo, sd.ReplaceArea()))

	_, err := sd.cli.NamedExec(query, info)
	if err != nil {
		return err
	}

	return nil
}

func (sd sqlDB) GetDownloadInfo(deviceID string) ([]*api.BlockDownloadInfo, error) {
	query := fmt.Sprintf(`SELECT * FROM %s WHERE device_id = ? and created_time >= TO_DAYS(NOW()) ORDER BY created_time DESC`,
		fmt.Sprintf(blockDownloadInfo, sd.ReplaceArea()))

	var out []*api.BlockDownloadInfo
	if err := sd.cli.Select(&out, query, deviceID); err != nil {
		return nil, err
	}

	return out, nil
}
