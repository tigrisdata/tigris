// Copyright 2022 Tigris Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"time"

	"github.com/tigrisdata/tigris/util/log"
)

type ServerConfig struct {
	Host string
	Port int16
}

type Config struct {
	Log          log.LogConfig
	Server       ServerConfig    `yaml:"server" json:"server"`
	Auth         AuthConfig      `yaml:"auth" json:"auth"`
	Cdc          CdcConfig       `yaml:"cdc" json:"cdc"`
	Search       SearchConfig    `yaml:"search" json:"search"`
	Tracing      TracingConfig   `yaml:"tracing" json:"tracing"`
	Profiling    ProfilingConfig `yaml:"profiling" json:"profiling"`
	Metrics      MetricsConfig
	DatadogTrace DatadogTraceConfig
	FoundationDB FoundationDBConfig
}

type AuthConfig struct {
	IssuerURL                string        `mapstructure:"issuer_url" yaml:"issuer_url" json:"issuer_url"`
	Audience                 string        `mapstructure:"audience" yaml:"audience" json:"audience"`
	JWKSCacheTimeout         time.Duration `mapstructure:"jwks_cache_timeout" yaml:"jwks_cache_timeout" json:"jwks_cache_timeout"`
	LogOnly                  bool          `mapstructure:"log_only" yaml:"log_only" json:"log_only"`
	EnableNamespaceIsolation bool          `mapstructure:"enable_namespace_isolation" yaml:"enable_namespace_isolation" json:"enable_namespace_isolation"`
	AdminNamespaces          []string      `mapstructure:"admin_namespaces" yaml:"admin_namespaces" json:"admin_namespaces"`
}

type CdcConfig struct {
	Enabled        bool `mapstructure:"enabled" yaml:"enabled" json:"enabled"`
	StreamInterval time.Duration
	StreamBatch    int
	StreamBuffer   int
}

type TracingConfig struct {
	Enabled             bool    `mapstructure:"enabled" yaml:"enabled" json:"enabled"`
	SampleRate          float64 `mapstructure:"sample_rate" yaml:"sample_rate" json:"sample_rate"`
	CodeHotspotsEnabled bool    `mapstructure:"codehotspots_enabled" yaml:"codehotspots_enabled" json:"codehotspots_enabled"`
	EndpointsEnabled    bool    `mapstructure:"endpoints_enabled" yaml:"endpoints_enabled" json:"endpoints_enabled"`
	WithUDS             string  `mapstructure:"agent_socket" yaml:"agent_socket" json:"agent_socket"`
	WithAgentAddr       string  `mapstructure:"agent_addr" yaml:"agent_addr" json:"agent_addr"`
	WithDogStatsdAddr   string  `mapstructure:"dogstatsd_addr" yaml:"dogstatsd_addr" json:"dogstatsd_addr"`
}

type ProfilingConfig struct {
	Enabled         bool `mapstructure:"enabled" yaml:"enabled" json:"enabled"`
	EnableCPU       bool `mapstructure:"enable_cpu" yaml:"enable_cpu" json:"enable_cpu"`
	EnableHeap      bool `mapstructure:"enable_heap" yaml:"enable_heap" json:"enable_heap"`
	EnableBlock     bool `mapstructure:"enable_block" yaml:"enable_block" json:"enable_block"`
	EnableMutex     bool `mapstructure:"enable_mutex" yaml:"enable_mutex" json:"enable_mutex"`
	EnableGoroutine bool `mapstructure:"enable_goroutine" yaml:"enable_goroutine" json:"enable_goroutine"`
}

type MetricsConfig struct {
	// Global switch
	Enabled bool `mapstructure:"enabled" yaml:"enabled" json:"enabled"`
	// Individual metric group configs
	Grpc   GrpcMetricsConfig
	Fdb    FdbMetricsConfig
	Search SearchMetricsConfig
}

type DatadogTraceConfig struct {
	// Global switch
	Enabled bool `mapstructure:"enabled" yaml:"enabled" json:"enabled"`
}

type GrpcMetricsConfig struct {
	Enabled      bool `mapstructure:"enabled" yaml:"enabled" json:"enabled"`
	Counters     bool `mapstructure:"counters" yaml:"counters" json:"counters"`
	ResponseTime bool `mapstructure:"response_time" yaml:"response_time" json:"response_time"`
}

type FdbMetricsConfig struct {
	Enabled      bool `mapstructure:"enabled" yaml:"enabled" json:"enabled"`
	Counters     bool `mapstructure:"counters" yaml:"counters" json:"counters"`
	ResponseTime bool `mapstructure:"response_time" yaml:"response_time" json:"response_time"`
}

type SearchMetricsConfig struct {
	Enabled      bool `mapstructure:"enabled" yaml:"enabled" json:"enabled"`
	Counters     bool `mapstructure:"counters" yaml:"counters" json:"counters"`
	ResponseTime bool `mapstructure:"response_time" yaml:"response_time" json:"response_time"`
}

var DefaultConfig = Config{
	Log: log.LogConfig{
		Level:      "info",
		SampleRate: 0.01,
	},
	Server: ServerConfig{
		Host: "0.0.0.0",
		Port: 8081,
	},
	Auth: AuthConfig{
		IssuerURL:                "https://tigrisdata-dev.us.auth0.com/",
		Audience:                 "https://tigris-api",
		JWKSCacheTimeout:         5 * time.Minute,
		LogOnly:                  true,
		EnableNamespaceIsolation: false,
		AdminNamespaces:          []string{"tigris-admin"},
	},
	Cdc: CdcConfig{
		Enabled:        false,
		StreamInterval: 500 * time.Millisecond,
		StreamBatch:    100,
		StreamBuffer:   200,
	},
	Search: SearchConfig{
		Host:         "localhost",
		Port:         8108,
		ReadEnabled:  true,
		WriteEnabled: true,
	},
	Tracing: TracingConfig{
		Enabled:             false,
		SampleRate:          0.01,
		CodeHotspotsEnabled: true,
		EndpointsEnabled:    true,
	},
	Profiling: ProfilingConfig{
		Enabled:    false,
		EnableCPU:  true,
		EnableHeap: true,
	},
	Metrics: MetricsConfig{
		Enabled: true,
		Grpc: GrpcMetricsConfig{
			Enabled:      true,
			Counters:     true,
			ResponseTime: true,
		},
		Fdb: FdbMetricsConfig{
			Enabled:      true,
			Counters:     true,
			ResponseTime: true,
		},
		Search: SearchMetricsConfig{
			Enabled:      true,
			Counters:     true,
			ResponseTime: true,
		},
	},
	DatadogTrace: DatadogTraceConfig{
		Enabled: true,
	},
}

// FoundationDBConfig keeps FoundationDB configuration parameters
type FoundationDBConfig struct {
	ClusterFile string `mapstructure:"cluster_file" json:"cluster_file" yaml:"cluster_file"`
}

type SearchConfig struct {
	Host         string `mapstructure:"host" json:"host" yaml:"host"`
	Port         int16  `mapstructure:"port" json:"port" yaml:"port"`
	AuthKey      string `mapstructure:"auth_key" json:"auth_key" yaml:"auth_key"`
	ReadEnabled  bool   `mapstructure:"read_enabled" yaml:"read_enabled" json:"read_enabled"`
	WriteEnabled bool   `mapstructure:"write_enabled" yaml:"write_enabled" json:"write_enabled"`
}
