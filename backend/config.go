package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

type PeerConfig struct {
	NodeID string `json:"node_id"`
	Host   string `json:"host"`
	Port   int    `json:"port"`
}

type Config struct {
	NodeID               string       `json:"node_id"`
	Host                 string       `json:"host"`
	Port                 int          `json:"port"`
	Peers                []PeerConfig `json:"peers"`
	ReplicationFactor    int          `json:"replication_factor"`
	ReadQuorum           int          `json:"read_quorum"`
	WriteQuorum          int          `json:"write_quorum"`
	GossipInterval       time.Duration
	FailureCheckInterval time.Duration
	GossipTimeout        time.Duration
}

func LoadConfig(filename string) (*Config, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("config read error: %w", err)
	}

	var configData struct {
		*Config
		GossipIntervalMs       int `json:"gossip_interval_ms"`
		FailureCheckIntervalMs int `json:"failure_check_interval_ms"`
		GossipTimeoutMs        int `json:"gossip_timeout_ms"`
	}

	if err := json.Unmarshal(data, &configData); err != nil {
		return nil, fmt.Errorf("config parse error: %w", err)
	}

	configData.Config.GossipInterval = time.Duration(configData.GossipIntervalMs) * time.Millisecond
	configData.Config.FailureCheckInterval = time.Duration(configData.FailureCheckIntervalMs) * time.Millisecond
	configData.Config.GossipTimeout = time.Duration(configData.GossipTimeoutMs) * time.Millisecond

	if err := ValidateConfig(configData.Config); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	return configData.Config, nil
}

func ValidateConfig(cfg *Config) error {
	if cfg.ReadQuorum <= 0 || cfg.WriteQuorum <= 0 {
		return fmt.Errorf("quorums must be positive integers")
	}

	if sum := cfg.ReadQuorum + cfg.WriteQuorum; sum <= cfg.ReplicationFactor {
		return fmt.Errorf("unsafe quorum: R(%d) + W(%d) ≤ N(%d)",
			cfg.ReadQuorum, cfg.WriteQuorum, cfg.ReplicationFactor)
	}

	if cfg.GossipInterval < 100*time.Millisecond {
		return fmt.Errorf("gossip interval too short: %v", cfg.GossipInterval)
	}

	return nil
}
