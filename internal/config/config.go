package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Field struct {
	Name     string `yaml:"name"`
	Type     string `yaml:"type"`
	Required bool   `yaml:"required,omitempty"`
	Layout   string `yaml:"layout,omitempty"`
}

type Source struct {
	SourceName string `yaml:"source_name"`
	Format     string `yaml:"format"`
	Location   string `yaml:"location"`
	Delimeter  string `yaml:"delimeter"`
}

type Target struct {
	Connection string `yaml:"db,omitempty"`
	Table      string `yaml:"table,omitempty"`
}

type PipelineConfig struct {
	PipelineName string  `yaml:"pipeline_name"`
	Source       Source  `yaml:"source"`
	Target       Target  `yaml:"target"`
	Fields       []Field `yaml:"fields"`
}

func LoadConfig(path string) (*PipelineConfig, error) {

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read the file : %w", err)
	}

	var cfg PipelineConfig

	err = yaml.Unmarshal(data, &cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to parse the file : %w", err)
	}

	return &cfg, nil
}
