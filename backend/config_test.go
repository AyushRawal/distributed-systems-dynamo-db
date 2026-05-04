package main

import (
	"testing"
	"time"
)

func TestValidateConfig(t *testing.T) {
	tests := []struct {
		name    string
		cfg     *Config
		wantErr bool
	}{
		{
			name: "valid quorum and gossip interval",
			cfg: &Config{
				ReplicationFactor: 3,
				ReadQuorum:        2,
				WriteQuorum:       2,
				GossipInterval:    1 * time.Second,
			},
			wantErr: false,
		},
		{
			name: "invalid non-positive quorum",
			cfg: &Config{
				ReplicationFactor: 3,
				ReadQuorum:        0,
				WriteQuorum:       2,
				GossipInterval:    1 * time.Second,
			},
			wantErr: true,
		},
		{
			name: "unsafe quorum R+W<=N",
			cfg: &Config{
				ReplicationFactor: 3,
				ReadQuorum:        1,
				WriteQuorum:       2,
				GossipInterval:    1 * time.Second,
			},
			wantErr: true,
		},
		{
			name: "gossip interval too short",
			cfg: &Config{
				ReplicationFactor: 3,
				ReadQuorum:        2,
				WriteQuorum:       2,
				GossipInterval:    50 * time.Millisecond,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateConfig(tt.cfg)
			if tt.wantErr && err == nil {
				t.Fatal("expected validation error but got nil")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
		})
	}
}
