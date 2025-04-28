package config

import (
	"errors"
)

type Config struct {
	Endpoint             string `yaml:"endpoint" mapstructure:"endpoint"`
	Database             string `yaml:"database" mapstructure:"database"`
	Folder               string `yaml:"folder" mapstructure:"folder"`
	Token                string `yaml:"token" mapstructure:"token"`
	UseSSL               bool   `yaml:"use_ssl" mapstructure:"use_ssl"`
	SessionPoolSizeLimit int    `yaml:"pool_size_limit" mapstructure:"pool_size_limit"`
	PreferLocalDC        bool   `yaml:"prefer_local_dc" mapstructure:"prefer_local_dc"`
	UseOldTypes          bool
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
	return nil
}
