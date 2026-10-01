package options

import (
	"log/slog"
	"time"
)

// Option to be passed to NewGateway to customize the resulting instance.
type Option func(*Options)

// Logger sets the logger for messages emitted by go-cowsql.
func Logger(logger *slog.Logger) Option {
	return func(options *Options) {
		options.logger = logger
	}
}

// DatabaseEndpoint is the path component to use with the cluster address for intra-cluster communication.
func DatabaseEndpoint(e string) Option {
	return func(options *Options) {
		options.databaseEndpoint = e
	}
}

// DefaultOfflineThreshold sets how much time a cluster member can be considered online after not responding to a heartbeat.
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
func PreUpdateCheck(f func() (func() error, error)) Option {
	return func(options *Options) {
		options.preUpdateCheck = f
	}
}

// NewOptions creates an options instance with default values.
func NewOptions() *Options {
	return &Options{
		logger:                  slog.Default(),
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

// Options represents the configurable options fields.
type Options struct {
	logger *slog.Logger

	databaseEndpoint string

	defaultOfflineThreshold time.Duration

	maxDBRetries int

	version string

	restrictTLS bool

	maxVoters func() int64

	maxStandby func() int64

	preUpdateCheck func() (func() error, error)
}

// Logger returns the logger in use.
func (o *Options) Logger() *slog.Logger {
	return o.logger
}

// DatabaseEndpoint is the path component to use with the cluster address for intra-cluster communication.
func (o *Options) DatabaseEndpoint() string {
	return o.databaseEndpoint
}

// DefaultOfflineThreshold is how much time a cluster member can be considered online after not responding to a heartbeat.
func (o *Options) DefaultOfflineThreshold() time.Duration {
	return o.defaultOfflineThreshold
}

// MaxDBRetries is the number of transaction rollback-and-retries will be attempted before giving up.
func (o *Options) MaxDBRetries() int {
	return o.maxDBRetries
}

// Version is the application version.
func (o *Options) Version() string {
	return o.version
}

// MaxVotersFunc is the function that determines the maximum number of voter nodes.
func (o *Options) MaxVotersFunc() func() int64 {
	return o.maxVoters
}

// MaxStandbyFunc is the function that determines the maximum number of standby nodes.
func (o *Options) MaxStandbyFunc() func() int64 {
	return o.maxStandby
}

// PreUpdateCheckFunc returns a function that runs before triggering an update.
func (o *Options) PreUpdateCheckFunc() func() (func() error, error) {
	return o.preUpdateCheck
}

// RestrictTLS returns whether TLS 1.2 is used.
func (o *Options) RestrictTLS() bool {
	return o.restrictTLS
}
