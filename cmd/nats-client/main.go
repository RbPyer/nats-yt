package main

import (
	"context"
	"danilov/internal/config"
	"danilov/pkg/broker/nats"
	"danilov/pkg/logger"
	"log/slog"
	"os"
)

const configPath = "./configs/local.yaml"

func main() {
	const op = "cmd/main.main"
	ctx := context.Background()

	// init logger
	log := logger.New()
	log.Info("logger was initialized", slog.String("op", op))

	// init config
	cfg := config.MustLoad(configPath)

	broker, err := nats.New(cfg.Address)
	if err != nil {
		log.Error("failed to create new broker instance", slog.String("error", err.Error()))
		os.Exit(1)
	}
	log.Info("broker was initialized", slog.String("op", op))

	if err = broker.TestConsumer(ctx, cfg.StreamName); err != nil {
		log.Error("failed to test consumer", slog.String("error", err.Error()))
		os.Exit(1)
	}

}
