package main

import (
	"context"
	"danilov/internal/config"
	"danilov/pkg/broker/nats"
	"danilov/pkg/logger"
	"danilov/pkg/ytsaurus"
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
	log.Info("config was loaded successfully", slog.String("op", op), slog.Any("cfg", cfg))

	ytAdapter, err := ytsaurus.New(cfg.Proxy, cfg.Token)
	if err != nil {
		log.Error("failed to create new yt client", slog.String("error", err.Error()))
		os.Exit(1)
	}

	done := make(chan struct{}, 1)

	broker, err := nats.New(cfg.Address)
	if err != nil {
		log.Error("failed to create new broker instance", slog.String("error", err.Error()))
		os.Exit(1)
	}
	log.Info("broker was initialized", slog.String("op", op))

	go broker.SubStream(
		context.WithValue(ctx, "log", log),
		cfg.StreamName,
		cfg.ConsumerName,
		cfg.TablePath,
		done,
		ytAdapter,
	)

	<-done
	log.Info("broker was shut down", slog.String("op", op))
}
