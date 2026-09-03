package options

import "time"

// Option to be passed to NewGateway to customize the resulting instance.
type Option func(*Options)

// LogLevel sets the logging level for messages emitted by cowsql and raft.
func LogLevel(level string) Option {
	return func(options *Options) {
		options.logLevel = level
	}
}

// Latency is a coarse grain measure of how fast/reliable network links
// are. This is used to tweak the various timeouts parameters of the raft
// algorithm. See the raft.Config structure for more details. A value of 1.0
// means use the default values from hashicorp's raft package. Values closer to
// 0 reduce the values of the various timeouts (useful when running unit tests
// in-memory).
func Latency(latency float64) Option {
	return func(options *Options) {
		options.latency = latency
	}
}

// The path component to use with the cluster address for intra-cluster communication.
func DatabaseEndpoint(e string) Option {
	return func(options *Options) {
		options.databaseEndpoint = e
	}
}

// DefaultOfflineThreshold how much time a cluster member can be considered online after not responding to a heartbeat.
func DefaultOfflineThreshold(t time.Duration) Option {
	return func(options *Options) {
		options.defaultOfflineThreshold = t
	}
}

// MaxDBRetries sets the number of transaction rollback-and-retries will be attempted before giving up.
func MaxDBRetries(r int) Option {
	return func(options *Options) {
		options.maxDBRetries = r
	}
}

// Version sets the application version.
func Version(v string) Option {
	return func(options *Options) {
		options.version = v
	}
}

// RestrictTLS forces using TLS 1.2.
func RestrictTLS(v bool) Option {
	return func(options *Options) {
		options.restrictTLS = v
	}
}

// MaxVoters sets the function that determines the maximum number of voter nodes.
func MaxVoters(f func() int64) Option {
	return func(options *Options) {
		options.maxVoters = f
	}
}

// MaxStandby sets the function that determines the maximum number of standby nodes.
func MaxStandby(f func() int64) Option {
	return func(options *Options) {
		options.maxStandby = f
	}
}

// PreUpdateCheck returns a function that runs before triggering an update.
// Returned string must be a path to an executable, or an empty string.
func PreUpdateCheck(f func() (func() error, error)) Option {
	return func(options *Options) {
		options.preUpdateCheck = f
	}
}

// Create a options instance with default values.
func NewOptions() *Options {
	return &Options{
		latency:                 1.0,
		logLevel:                "ERROR",
		databaseEndpoint:        "/internal/database",
		defaultOfflineThreshold: 20 * time.Second,
		maxDBRetries:            250,
		version:                 "0.0.0",
		restrictTLS:             false,
		maxVoters:               func() int64 { return 3 },
		maxStandby:              func() int64 { return 3 },
		preUpdateCheck:          func() (func() error, error) { return func() error { return nil }, nil },
	}
}

type Options struct {
	latency  float64
	logLevel string

	databaseEndpoint string

	defaultOfflineThreshold time.Duration

	maxDBRetries int

	version string

	restrictTLS bool

	maxVoters func() int64

	maxStandby func() int64

	preUpdateCheck func() (func() error, error)
}

func (o *Options) Latency() float64 {
	return o.latency
}

func (o *Options) LogLevel() string {
	return o.logLevel
}

func (o *Options) DatabaseEndpoint() string {
	return o.databaseEndpoint
}

func (o *Options) DefaultOfflineThreshold() time.Duration {
	return o.defaultOfflineThreshold
}

func (o *Options) MaxDBRetries() int {
	return o.maxDBRetries
}

func (o *Options) Version() string {
	return o.version
}

func (o *Options) MaxVotersFunc() func() int64 {
	return o.maxVoters
}

func (o *Options) MaxStandbyFunc() func() int64 {
	return o.maxStandby
}

func (o *Options) PreUpdateCheckFunc() func() (func() error, error) {
	return o.preUpdateCheck
}

func (o *Options) RestrictTLS() bool {
	return o.restrictTLS
}
