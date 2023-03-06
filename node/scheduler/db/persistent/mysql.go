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
	errNotFind = "Not Found"

	// tables
	carfileInfoTable    = "carfiles"
	replicaInfoTable    = "replicas"
	blockDownloadInfo   = "block_download_info"
	nodeUpdateInfo      = "node_update_info"
	waitingCarfileTable = "carfile_waiting"
	nodeInfoTable       = "node_info"
	validatorsTable     = "validators"
	nodeVerifyingTable  = "node_verifying"
	downloadingTable    = "carfile_downloading"

	// NodeTypeKey node info key
	NodeTypeKey = "node_type"
	// SecretKey node info key
	SecretKey = "secret"

	loadNodeInfoMaxCount      = 100
	loadBlockDownloadMaxCount = 100
	loadReplicaInfoMaxCount   = 100
	loadValidateInfoMaxCount  = 100
)
