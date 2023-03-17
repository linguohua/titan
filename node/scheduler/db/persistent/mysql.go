package persistent

import (
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

// TypeMySQL MySql
func TypeMySQL() string {
	return "MySQL"
}

const (
	// tables
	carfileInfoTable    = "carfiles"
	replicaInfoTable    = "replicas"
	blockDownloadInfo   = "block_download_info"
	edgeUpdateInfo      = "edge_update_info"
	waitingCarfileTable = "carfile_waiting"
	nodeInfoTable       = "node_info"
	validatorsTable     = "validators"
	nodeVerifyingTable  = "node_verifying"
	downloadingTable    = "carfile_downloading"
	nodeAllocateTable   = "node_allocate_info"
	validateResultTable = "validate_result"

	loadNodeInfoMaxCount      = 100
	loadBlockDownloadMaxCount = 100
	loadReplicaInfoMaxCount   = 100
	loadValidateInfoMaxCount  = 100
	loadCarfileInfoMaxCount   = 100
)
