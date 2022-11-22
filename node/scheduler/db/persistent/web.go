package persistent

import (
	"fmt"
)

type webDB interface {
	GetNodes(cursor int, count int) ([]*NodeInfo, error)
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
