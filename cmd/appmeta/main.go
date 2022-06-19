package main

import (
	"context"

	tally "github.com/uber-go/tally/v4"
	"go.uber.org/zap"

	"appmeta/pkg/server"
	"appmeta/pkg/storage"
)

func main() {
	// TODO: use config file to load real service settings for logger and metrics
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic("failed to create logger")
	}
	scope := tally.NoopScope

	// Make sure panic is logged.
	defer func() {
		if err := recover(); err != nil {
			logger.Panic("Api server process panics.", zap.Any("panic-error", err))
		}
	}()

	logger.Info("Start Application metadata API service.")

	metaSt := storage.NewPersistence(logger, scope.SubScope("persistence"))
	svr := server.NewServer(logger, scope.SubScope("server"), "127.0.0.1:3000", metaSt)
	if err := svr.Start(context.Background()); err != nil {
		logger.Fatal("Failed to start api server", zap.Error(err))
	}
}
