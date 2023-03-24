package db

import (
	"github.com/jmoiron/sqlx"

	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("election")

// SQLDB scheduler sql db
type SQLDB struct {
	db *sqlx.DB
}

// NewSQLDB new scheduler sql db
func NewSQLDB(db *sqlx.DB) *SQLDB {
	return &SQLDB{db}
}

const (
	// tables
	carfileRecordTable  = "carfiles"
	replicaInfoTable    = "replicas"
	blockDownloadTable  = "block_download_info"
	edgeUpdateTable     = "edge_update_info"
	nodeInfoTable       = "node_info"
	validatorsTable     = "validators"
	nodeRegisterTable   = "node_register_info"
	validateResultTable = "validate_result"

	loadNodeInfosLimit      = 100
	loadBlockDownloadsLimit = 100
	loadReplicaInfosLimit   = 100
	loadValidateInfosLimit  = 100
	loadCarfileRecordsLimit = 100
)
