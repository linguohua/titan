package persistent

import (
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
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

	err = db.SetAllNodeOffline()
	return db, err
}

// IsNilErr Is NilErr
func (sd sqlDB) IsNilErr(err error) bool {
	return err.Error() == errNodeNotFind
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

func (sd sqlDB) AddNodeOnlineTime(deviceID string, onLineTime int64) error {
	info := &NodeInfo{OnlineTime: onLineTime, DeviceID: deviceID}
	_, err := sd.cli.NamedExec(`UPDATE node SET online_time=online_time+:online_time WHERE device_id=:device_id`, info)

	return err
}

func (sd sqlDB) AddAllNodeOnlineTime(onLineTime int64) error {
	info := &NodeInfo{OnlineTime: onLineTime, IsOnline: 1}
	_, err := sd.cli.NamedExec(`UPDATE node SET online_time=online_time+:online_time WHERE is_online=:is_online`, info)

	return err
}

func (sd sqlDB) SetAllNodeOffline() error {
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
	if rows.Next() {
		err = rows.StructScan(info)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, xerrors.New(errNodeNotFind)
	}
	rows.Close()

	return info, err
}

func (sd sqlDB) SetValidateResultInfo(info *ValidateResult) error {
	nowTime := time.Now().Format("2006-01-02 15:04:05")

	if ValidateStatus(info.Status) == ValidateStatusCreate {
		info.StratTime = nowTime
		info.ServerName = serverName

		_, err := sd.cli.NamedExec(`INSERT INTO validate_result (round_id, device_id, validator_id, status, strat_time, server_name)
                VALUES (:round_id, :device_id, :validator_id, :status, :strat_time, :server_name)`, info)
		return err

	} else if ValidateStatus(info.Status) > ValidateStatusCreate {
		info.EndTime = nowTime

		_, err := sd.cli.NamedExec(`UPDATE validate_result SET end_time=:end_time,status=:status,msg=:msg  WHERE device_id=:device_id AND round_id=:round_id`, info)

		return err
	}

	return xerrors.Errorf("SetValidateResultInfo err deviceid:%v ,status:%v, roundID:%v, serverName:%v", info.DeviceID, info.Status, info.RoundID, info.ServerName)
}

func (sd sqlDB) SetNodeToValidateErrorList(sID, deviceID string) error {
	_, err := sd.cli.NamedExec(`INSERT INTO validate_err (round_id, device_id)
                VALUES (:round_id, :device_id)`, map[string]interface{}{
		"round_id":  sID,
		"device_id": deviceID,
	})
	return err
}

var blockTbale = "blocks_%v"

func (sd sqlDB) RemoveBlockInfo(deviceID, cid string) error {
	info := NodeBlock{
		TableName: fmt.Sprintf(blockTbale, deviceID),
		CID:       cid,
	}

	cmd := fmt.Sprintf(`DELETE FROM %s WHERE cid=:cid`, info.TableName)
	_, err := sd.cli.NamedExec(cmd, info)
	return err
}

func (sd sqlDB) SetBlockInfo(deviceID, cid string, fid string, isUpdate bool) error {
	info := NodeBlock{
		TableName: fmt.Sprintf(blockTbale, deviceID),
		CID:       cid,
		FID:       fid,
	}

	if isUpdate {
		cmd := fmt.Sprintf(`UPDATE %s SET fid=:fid WHERE cid=:cid`, info.TableName)
		_, err := sd.cli.NamedExec(cmd, info)

		return err
	}

	schema := fmt.Sprintf(`
	CREATE TABLE %s (
		cid text,
		fid text
	);`, info.TableName)

	_, err := sd.cli.Exec(schema)
	if err != nil {
		if !strings.Contains(err.Error(), "already exists") {
			return err
		}
	}

	cmd := fmt.Sprintf(`INSERT INTO %s (cid, fid)
	VALUES (:cid, :fid)`, info.TableName)

	_, err = sd.cli.NamedExec(cmd, info)

	return err
}

func (sd sqlDB) GetBlockFidWithCid(deviceID, cid string) (string, error) {
	info := &NodeBlock{
		TableName: fmt.Sprintf(blockTbale, deviceID),
		CID:       cid,
	}

	cmd := fmt.Sprintf(`SELECT * FROM %s WHERE cid=:cid`, info.TableName)

	rows, err := sd.cli.NamedQuery(cmd, info)
	if err != nil {
		return "", err
	}
	if rows.Next() {
		err = rows.StructScan(info)
		if err != nil {
			return "", err
		}
	} else {
		return "", xerrors.New(errNodeNotFind)
	}
	rows.Close()

	return info.FID, err
}

func (sd sqlDB) GetBlockInfos(deviceID string) (map[string]string, error) {
	info := &NodeBlock{
		TableName: fmt.Sprintf(blockTbale, deviceID),
	}

	m := make(map[string]string)

	cmd := fmt.Sprintf(`SELECT * FROM %s`, info.TableName)

	rows, err := sd.cli.NamedQuery(cmd, info)
	if err != nil {
		return nil, err
	}
	if rows.Next() {
		err = rows.StructScan(info)
		if err == nil {
			m[info.CID] = info.FID
		}
	} else {
		return nil, xerrors.New(errNodeNotFind)
	}
	rows.Close()

	return m, nil
}

func (sd sqlDB) GetBlockCidWithFid(deviceID, fid string) (string, error) {
	info := &NodeBlock{
		TableName: fmt.Sprintf(blockTbale, deviceID),
		FID:       fid,
	}

	cmd := fmt.Sprintf(`SELECT * FROM %s WHERE fid=:fid`, info.TableName)
	rows, err := sd.cli.NamedQuery(cmd, info)
	if err != nil {
		return "", err
	}
	if rows.Next() {
		err = rows.StructScan(info)
		if err != nil {
			return "", err
		}
	} else {
		return "", xerrors.New(errNodeNotFind)
	}
	rows.Close()

	return info.CID, err
}

func (sd sqlDB) GetBlockNum(deviceID string) (int64, error) {
	info := NodeBlock{
		TableName: fmt.Sprintf(blockTbale, deviceID),
	}

	var count int64
	cmd := fmt.Sprintf("SELECT count(*) FROM %s WHERE fid>0 ;", info.TableName)
	err := sd.cli.Get(&count, cmd)

	return count, err
}
