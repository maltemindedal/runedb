package main

import (
	"context"
	"os"

	"github.com/maltemindedal/godis/internal/command"
	"github.com/maltemindedal/godis/internal/config"
	godislogger "github.com/maltemindedal/godis/internal/logger"
	"github.com/maltemindedal/godis/internal/server"
	"github.com/maltemindedal/godis/internal/storage"
)

func main() {
	ctx, stop := server.NotifyContext(context.Background())
	defer stop()

	cfg := config.ParseFlags()
	logger := godislogger.New(cfg.LogLevel)
	store := storage.NewStore()
	executor := command.NewExecutor(store, logger)
	srv := server.New(cfg, logger, store, executor)

	if err := srv.ListenAndServe(ctx); err != nil {
		logger.Error("godis exited with error", "error", err)
		os.Exit(1)
	}
}
