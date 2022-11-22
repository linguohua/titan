package persistent

import (
	"fmt"

	"github.com/linguohua/titan/api"
)

type webDB interface {
	GetNodes(cursor int, count int) ([]*NodeInfo, error)
	GetBlockDownloadInfos(DeviceID string, startTime int64, endTime int64, cursor, count int) ([]api.BlockDownloadInfo, error)
}

func (sd sqlDB) GetNodes(cursor int, count int) ([]*NodeInfo, error) {
	cmd := fmt.Sprintf(`SELECT device_id FROM node limit %d,%d`, cursor, count)
	rows, err := sd.cli.NamedQuery(cmd, nil)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	nodes := make([]*NodeInfo, 0)
	for rows.Next() {
		node := &NodeInfo{}
		err = rows.StructScan(node)
		if err != nil {
			return nodes, err
		}

		nodes = append(nodes, node)
	}

	return nodes, nil
}

func (sd sqlDB) GetBlockDownloadInfos(deviceID string, startTime int64, endTime int64, cursor, count int) ([]api.BlockDownloadInfo, error) {
	query := fmt.Sprintf(`SELECT * FROM %s WHERE device_id = ? and created_time between ? and ? limit ?,?`,
		fmt.Sprintf(blockDownloadInfo, sd.ReplaceArea()))

	var out []api.BlockDownloadInfo
	if err := sd.cli.Select(&out, query, deviceID, startTime, endTime, cursor, count); err != nil {
		return nil, err
	}

	return out, nil
}
