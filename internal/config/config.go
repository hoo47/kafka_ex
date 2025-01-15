package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Kafka struct {
		Brokers  []string `yaml:"brokers"`
		Consumer struct {
			GroupID         string `yaml:"group_id"`
			AutoOffsetReset string `yaml:"auto_offset_reset"`
		} `yaml:"consumer"`
		Topics struct {
			AppEvents string `yaml:"app_events"`
		} `yaml:"topics"`
	} `yaml:"kafka"`
	SchemaRegistry struct {
		URL      string `yaml:"url"`
		Subjects struct {
			AppInstall   string `yaml:"app_install"`
			AppUninstall string `yaml:"app_uninstall"`
		} `yaml:"subjects"`
	} `yaml:"schema_registry"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, err
	}

	return &config, nil
}
