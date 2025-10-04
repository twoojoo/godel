package options

const (
	PetaByte int64 = 1073741824 * 1024 * 1024
	TeraByte int64 = 1073741824 * 1024
	GigaByte int64 = 1073741824
	MegaByte int64 = 1073741824 / 1024
	KiloByte int64 = 1073741824 / 1024 / 1024

	// topic defaults
	DefaultRetentionMs                 int64  = 604800000
	DefaultMaxMessageBytes             int64  = 1000001
	DefaultRetentionBytes              int64  = -1
	DefaultLogRetentionCheckIntervalMs int64  = 300000
	DefaultNumPartitions               uint32 = 1
	DefaultBasePath                    string = "./godel_data"

	// consumer defaults
	DefaultSessionTimeoutMs     int64 = 10000
	DefaultHeartbeatIntervalMs  int64 = 3000
	DefaultAutoCommitIntervalMs int64 = 5000
)
