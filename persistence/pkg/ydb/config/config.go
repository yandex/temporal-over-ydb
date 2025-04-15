package config

type Config struct {
	Endpoint             string `yaml:"endpoint"`
	Database             string `yaml:"database"`
	Folder               string `yaml:"folder"`
	Token                string `yaml:"token"`
	UseSSL               bool   `yaml:"use_ssl"`
	SessionPoolSizeLimit int    `yaml:"pool_size_limit"`
	PreferLocalDC        bool   `yaml:"prefer_local_dc"`
	UseOldTypes          bool
}

func (c *Config) Validate() (bool, error) {
	return true, nil
}
