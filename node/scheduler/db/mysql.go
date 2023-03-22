package db

import (
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

func NewDB(dns string) (*sqlx.DB, error) {
	dns = fmt.Sprintf("%s?parseTime=true&loc=Local", dns)

	client, err := sqlx.Open("mysql", dns)
	if err != nil {
		return nil, err
	}

	if err = client.Ping(); err != nil {
		return nil, err
	}

	return client, nil
}

const (
	// tables
	carfileRecordTable  = "carfiles"
	replicaInfoTable    = "replicas"
	blockDownloadTable  = "block_download_info"
	edgeUpdateTable     = "edge_update_info"
	nodeInfoTable       = "node_info"
	validatorsTable     = "validators"
	nodeAllocateTable   = "node_allocate_info"
	validateResultTable = "validate_result"

	loadNodeInfosLimit      = 100
	loadBlockDownloadsLimit = 100
	loadReplicaInfosLimit   = 100
	loadValidateInfosLimit  = 100
	loadCarfileRecordsLimit = 100
)
