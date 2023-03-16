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
	NodeID string
	// used auth when connect to scheduler
	Secret string
	// carfilestore path
	CarfileStorePath string
	// upload file bandwidth, unit is B/s
	BandwidthUp int64
	// download file bandwidth, unit is B/s
	BandwidthDown int64
	// if true, get scheduler url from locator
	Locator bool
	// InsecureSkipVerify skip tls verify
	InsecureSkipVerify bool
	// used for http3 server
	// be used if InsecureSkipVerify is true
	CertificatePath string
	// used for http3 server
	// be used if InsecureSkipVerify is true
	PrivateKeyPath string
	// self sign certificate, use for client
	CaCertificatePath string
	// FetchTimeout get block timeout
	FetchBlockTimeout int
	// FetchBlockFailedRetry retry when get block failed
	FetchBlockRetry int
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
	GeoDBPath string
	// mysql db address
	DatabaseAddress string
	// uuid
	UUID string
	// InsecureSkipVerify skip tls verify
	InsecureSkipVerify bool
	// used for http3 server
	// be used if InsecureSkipVerify is true
	CertificatePath string
	// used for http3 server
	// be used if InsecureSkipVerify is true
	PrivateKeyPath string
	// self sign certificate, use for client
	CaCertificatePath string
}

type SchedulerCfg struct {
	// host address and port the edge node api will listen on
	ListenAddress string
	// database address
	DatabaseAddress string
	// area id
	AreaID string
	// InsecureSkipVerify skip tls verify
	InsecureSkipVerify bool
	// used for http3 server
	// be used if InsecureSkipVerify is true
	CertificatePath string
	// used for http3 server
	// be used if InsecureSkipVerify is true
	PrivateKeyPath string
	// self sign certificate, use for client
	CaCertificatePath string
	// test nat type
	SchedulerServer1 string
	// test nat type
	SchedulerServer2 string
	// config to enabled node validation, default: true
	EnableValidate bool
	// etcd server addresses
	EtcdAddresses []string
	// Cache to the number of candidate nodes (does not contain 'seedCacheCount')
	CandidateReplicaCachesCount int
}
