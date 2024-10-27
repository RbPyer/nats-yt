package config

import (
	"github.com/ilyakaznacheev/cleanenv"
	"log"
)

type Config struct {
	NatsConfig     `yaml:"nats"`
	YTsaurusConfig `yaml:"ytsaurus"`
}

type YTsaurusConfig struct {
	Proxy     string `yaml:"proxy"`
	Token     string `yaml:"token"`
	TablePath string `yaml:"table_path"`
}

type NatsConfig struct {
	Address      string `yaml:"address" env-default:"localhost:4222"`
	StreamName   string `yaml:"stream_name"`
	ConsumerName string `yaml:"consumer_name"`
}

func MustLoad(path string) Config {
	const op = "config.MustLoad"
	var cfg Config
	if err := cleanenv.ReadConfig(path, &cfg); err != nil {
		log.Fatalf("%s: failed to read config: %s", op, err)
	}
	return cfg
}
