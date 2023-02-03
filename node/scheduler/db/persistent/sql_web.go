package persistent

import (
	"fmt"
	"time"

	"github.com/linguohua/titan/api"
)

const (
	maxCount = 500
)

type webDB interface {
	GetNodes(cursor int, count int) ([]*NodeInfo, int64, error)
	GetBlockDownloadInfos(DeviceID string, startTime time.Time, endTime time.Time, cursor, count int) ([]api.BlockDownloadInfo, int64, error)
	SetNodeConnectionLog(deviceID string, status api.NodeConnectionStatus) error
	GetNodeConnectionLogs(deviceID string, startTime time.Time, endTime time.Time, cursor, count int) ([]api.NodeConnectionLog, int64, error)
	GetValidateResults(cursor int, count int) ([]api.WebValidateResult, int64, error)
	GetCacheTaskInfos(startTime time.Time, endTime time.Time, cursor, count int) (*api.ListCacheInfosRsp, error)
}

func (sd sqlDB) GetNodes(cursor int, count int) ([]*NodeInfo, int64, error) {
	var total int64
	countSQL := "SELECT count(*) FROM node"
	err := sd.cli.Get(&total, countSQL)
	if err != nil {
		return nil, 0, err
	}

	queryString := "SELECT device_id, is_online FROM node limit ?,?"

	if count > maxCount {
		count = maxCount
	}

	var out []*NodeInfo
	err = sd.cli.Select(&out, queryString, cursor, count)
	if err != nil {
		return nil, 0, err
	}

	return out, total, nil
}

func (sd sqlDB) GetBlockDownloadInfos(deviceID string, startTime time.Time, endTime time.Time, cursor, count int) ([]api.BlockDownloadInfo, int64, error) {
	query := fmt.Sprintf(`SELECT * FROM %s WHERE device_id = ? and created_time between ? and ? limit ?,?`,
		fmt.Sprintf(blockDownloadInfo, sd.replaceArea()))

	var total int64
	countSQL := fmt.Sprintf(`SELECT count(*) FROM %s WHERE device_id = ? and created_time between ? and ?`, fmt.Sprintf(blockDownloadInfo, sd.replaceArea()))
	if err := sd.cli.Get(&total, countSQL, deviceID, startTime, endTime); err != nil {
		return nil, 0, err
	}

	if count > maxCount {
		count = maxCount
	}

	var out []api.BlockDownloadInfo
	if err := sd.cli.Select(&out, query, deviceID, startTime, endTime, cursor, count); err != nil {
		return nil, 0, err
	}

	return out, total, nil
}

func (sd sqlDB) GetCacheTaskInfos(startTime time.Time, endTime time.Time, cursor, count int) (*api.ListCacheInfosRsp, error) {
	cacheTable := fmt.Sprintf(cacheInfoTable, sd.replaceArea())

	var total int64
	countSQL := fmt.Sprintf(`SELECT count(*) FROM %s WHERE end_time between ? and ?`, cacheTable)
	if err := sd.cli.Get(&total, countSQL, startTime, endTime); err != nil {
		return nil, err
	}

	if count > maxCount {
		count = maxCount
	}

	query := fmt.Sprintf(`SELECT * FROM %s WHERE end_time between ? and ? limit ?,?`,
		cacheTable)

	var out []*api.CacheTaskInfo
	if err := sd.cli.Select(&out, query, startTime, endTime, cursor, count); err != nil {
		return nil, err
	}

	return &api.ListCacheInfosRsp{Datas: out, Total: total}, nil
}

func (sd sqlDB) SetNodeConnectionLog(deviceID string, status api.NodeConnectionStatus) error {
	log := api.NodeConnectionLog{DeviceID: deviceID, Status: status, CreatedAt: time.Now()}
	insertSQL := "INSERT INTO node_connection_log (device_id, status) VALUES (:device_id, :status)"
	_, err := sd.cli.NamedExec(insertSQL, log)
	return err
}

func (sd sqlDB) GetNodeConnectionLogs(deviceID string, startTime time.Time, endTime time.Time, cursor, count int) ([]api.NodeConnectionLog, int64, error) {
	var total int64
	countSQL := "SELECT count(*) FROM node_connection_log WHERE device_id = ? and created_time between ? and ?"
	if err := sd.cli.Get(&total, countSQL, deviceID, startTime, endTime); err != nil {
		return []api.NodeConnectionLog{}, 0, err
	}

	if count > maxCount {
		count = maxCount
	}

	query := "SELECT device_id, status, created_time FROM node_connection_log WHERE device_id = ? and created_time between ? and ? limit ?,?"
	var out []api.NodeConnectionLog
	if err := sd.cli.Select(&out, query, deviceID, startTime, endTime, cursor, count); err != nil {
		return nil, 0, err
	}
	return out, total, nil
}

func (sd sqlDB) GetValidateResults(cursor int, count int) ([]api.WebValidateResult, int64, error) {
	var total int64
	countSQL := "SELECT count(*) FROM validate_result"
	if err := sd.cli.Get(&total, countSQL); err != nil {
		return []api.WebValidateResult{}, 0, err
	}

	query := "SELECT * FROM validate_result limit ?,?"
	var out []api.WebValidateResult
	if err := sd.cli.Select(&out, query, cursor, count); err != nil {
		return nil, 0, err
	}
	return out, total, nil
}
