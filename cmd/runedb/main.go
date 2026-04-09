package main

import (
	"context"
	"os"

	"github.com/maltemindedal/runedb/internal/command"
	"github.com/maltemindedal/runedb/internal/config"
	runedblogger "github.com/maltemindedal/runedb/internal/logger"
	"github.com/maltemindedal/runedb/internal/server"
	"github.com/maltemindedal/runedb/internal/storage"
)

func main() {
	ctx, stop := server.NotifyContext(context.Background())
	defer stop()

	cfg := config.ParseFlags()
	logger := runedblogger.New(cfg.LogLevel)
	store := storage.NewStore()
	executor := command.NewExecutor(store, logger)
	srv := server.New(cfg, logger, store, executor)

	if err := srv.ListenAndServe(ctx); err != nil {
		logger.Error("RuneDB exited with error", "error", err)
		os.Exit(1)
	}
}