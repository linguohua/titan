package persistent

import (
	"time"

	"golang.org/x/xerrors"
)

// InitDB New  DB
func InitDB(url, dbType string) (err error) {
	switch dbType {
	case TypeMySQL():
		err = InitSQL(url)
	default:
		err = xerrors.New("unknown DB type")
	}

	return err
}

// NodeInfo base info
type NodeInfo struct {
	ID         int
	DeviceID   string    `db:"device_id"`
	LastTime   time.Time `db:"last_time"`
	Geo        string    `db:"geo"`
	IsOnline   bool      `db:"is_online"`
	NodeType   string    `db:"node_type"`
	Address    string    `db:"address"`
	Port       string    `db:"port"`
	CreateTime time.Time `db:"create_time"`
	PrivateKey string    `db:"private_key"`
	Quitted    bool      `db:"quitted"`
}
