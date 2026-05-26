// === FILE: config.go ===
package main

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config represents the YAML configuration structure
type Config struct {
	ListenAddr   string `yaml:"listen_addr"`
	BackendA     string `yaml:"backend_a"`
	BackendB     string `yaml:"backend_b"`
	TimeoutMs    int    `yaml:"timeout_ms"`
	Priority     string `yaml:"priority"` // "A" or "B"
	LogLevel     string `yaml:"log_level"` // "debug", "info", "error"
	MaxConns     int    `yaml:"max_conns"`
	MaxBufferSize int64 `yaml:"max_buffer_size"`
}

// LoadConfig loads configuration from a YAML file
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	// Validate and set defaults
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = ":1344"
	}
	if cfg.TimeoutMs <= 0 {
		cfg.TimeoutMs = 5000
	}
	if cfg.Priority == "" {
		cfg.Priority = "A"
	}
	if cfg.LogLevel == "" {
		cfg.LogLevel = "info"
	}
	if cfg.MaxConns <= 0 {
		cfg.MaxConns = 100
	}
	if cfg.MaxBufferSize <= 0 {
		cfg.MaxBufferSize = 10 * 1024 * 1024 // 10MB default
	}

	if cfg.Priority != "A" && cfg.Priority != "B" {
		return nil, fmt.Errorf("invalid priority: must be 'A' or 'B'")
	}

	return &cfg, nil
}
