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

	// used for http3 server
	CertificatePath string
	PrivateKeyPath  string

	// used for http3 client
	RootCertificatePath string
}

type CandidateCfg struct {
	EdgeCfg
	TcpSrvAddr string
	IpfsApiURL string
}

type LocatorCfg struct {
	// host address and port the edge node api will listen on
	ListenAddress string
	// used when 'ListenAddress' is unspecified. must be a valid duration recognized by golang's time.ParseDuration function
	Timeout string
	// geodb path
	GeodbPath string
	// mysql db addrss
	DBAddrss string
	// uuid
	UUID string
}

type SchedulerCfg struct {
	// host address and port the edge node api will listen on
	ListenAddress string
	// redis server address
	RedisAddrss string
	// geodb path
	GeodbPath string
	// mysql address
	PersistentDBURL string
	// server name
	ServerName string
	// area id
	AreaID string
}
