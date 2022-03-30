package config

import "time"

type PeerConfig struct {
	Addr string `yaml:"addr"`
	ID   int64  `yaml:"id"`
}

type Config struct {
	InitialCluster []PeerConfig  `yaml:"initialCluster"`
	LogPath        string        `yaml:"logPath"`
	ListenAddr     string        `yaml:"listenAddr"`
	ElectTimeout   time.Duration `yaml:"electTimeout"`
	ID             int64         `yaml:"id"`
}
