package config

import (
	"errors"
	"time"
)

type ConnectBackoffConfig struct {
	BaseDelay  time.Duration `yaml:"base_delay" mapstructure:"base_delay"`
	Multiplier float64       `yaml:"multiplier" mapstructure:"multiplier"`
	Jitter     float64       `yaml:"jitter" mapstructure:"jitter"`
	MaxDelay   time.Duration `yaml:"max_delay" mapstructure:"max_delay"`
}

type DBEndpointConnectParamsConfig struct {
	Backoff           *ConnectBackoffConfig `yaml:"backoff" mapstructure:"backoff"`
	MinConnectTimeout time.Duration         `yaml:"min_connect_timeout" mapstructure:"min_connect_timeout"`
}

type Config struct {
	Endpoint                string                         `yaml:"endpoint" mapstructure:"endpoint"`
	Database                string                         `yaml:"database" mapstructure:"database"`
	Folder                  string                         `yaml:"folder" mapstructure:"folder"`
	Token                   string                         `yaml:"token" mapstructure:"token"`
	UseSSL                  bool                           `yaml:"use_ssl" mapstructure:"use_ssl"`
	SessionPoolSizeLimit    int                            `yaml:"pool_size_limit" mapstructure:"pool_size_limit"`
	PreferLocalDC           bool                           `yaml:"prefer_local_dc" mapstructure:"prefer_local_dc"`
	DBEndpointConnectParams *DBEndpointConnectParamsConfig `yaml:"db_endpoint_connect_params" mapstructure:"db_endpoint_connect_params"`
	DiscoveryDialTimeout    time.Duration                  `yaml:"discovery_dial_timeout" mapstructure:"discovery_dial_timeout"`
	UseOldTypes             bool
	// DangerouslySkipDiscovery opens the driver against the configured endpoint alone. Cluster discovery
	// is the only thing the driver does over the network at startup, and it blocks until it succeeds,
	// so skipping it is what lets the process come up while YDB is unreachable. Everything discovery
	// provides goes with it: the node list, balancing across them, the nearest datacenter.
	DangerouslySkipDiscovery bool `yaml:"-" mapstructure:"-"`
}

func (c *Config) Validate() error {
	if c.Endpoint == "" {
		return errors.New("endpoint is required")
	}
	if c.Database == "" {
		return errors.New("database is required")
	}
	if c.Folder == "" {
		return errors.New("folder is required")
	}
	if connectParams := c.DBEndpointConnectParams; connectParams != nil {
		if backoff := connectParams.Backoff; backoff != nil {
			if backoff.BaseDelay <= 0 {
				return errors.New("db_endpoint_connect_params.backoff.base_delay must be positive")
			}
			if backoff.Multiplier < 1 {
				return errors.New("db_endpoint_connect_params.backoff.multiplier must be at least 1")
			}
			if backoff.Jitter < 0 || backoff.Jitter > 1 {
				return errors.New("db_endpoint_connect_params.backoff.jitter must be between 0 and 1")
			}
			if backoff.MaxDelay < backoff.BaseDelay {
				return errors.New("db_endpoint_connect_params.backoff.max_delay must not be less than base_delay")
			}
		}
		if connectParams.MinConnectTimeout < 0 {
			return errors.New("db_endpoint_connect_params.min_connect_timeout must not be negative")
		}
	}
	if c.DiscoveryDialTimeout < 0 {
		return errors.New("discovery_dial_timeout must not be negative")
	}
	return nil
}
