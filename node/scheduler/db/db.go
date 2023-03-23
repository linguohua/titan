package db

import "github.com/jmoiron/sqlx"

type SqlDB struct {
	db *sqlx.DB
}

func NewSqlDB(db *sqlx.DB) *SqlDB {
	return &SqlDB{db}
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
