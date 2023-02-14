package config

// // NOTE: ONLY PUT STRUCT DEFINITIONS IN THIS FILE
// //
// // After making edits here, run 'make cfgdoc-gen' (or 'make gen')

type EdgeCfg struct {
	// host address and port the edge node api will listen on
	ListenAddress string
	// used when 'ListenAddress' is unspecified. must be a valid duration recognized by golang's time.ParseDuration function
	Timeout string
	// deivce id
	DeviceID string
	// used auth when connect to scheduler
	Secret string
	// carfilestore path
	CarfilestorePath string
	// blockstore type
	CarfilestoreType string
	// upload file bandwidth, unit is B/s
	BandwidthUp int64
	// download file bandwidth, unit is B/s
	BandwidthDown int64
	// if true, get scheduler url from locator
	Locator bool
}

type CandidateCfg struct {
	EdgeCfg
	TcpSrvAddr string
	IpfsApiURL string
}
