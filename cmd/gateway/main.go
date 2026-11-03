// Command gateway polls Modbus devices. In this stage it prints every
// reading as JSON; stage 4 replaces printing with MQTT publishing.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/BaranTab/industrial-iot-pipeline/internal/config"
	"github.com/BaranTab/industrial-iot-pipeline/internal/poller"
	"github.com/BaranTab/industrial-iot-pipeline/internal/telemetry"
)

func main() {
	configPath := flag.String("config", "configs/gateway.yaml", "path to the YAML config")
	flag.Parse()

	// Logs go to stderr so that stdout carries only the readings.
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Error("invalid config", "err", err)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	readings := make(chan telemetry.Message, 100)
	var wg sync.WaitGroup
	for _, dev := range cfg.Devices {
		p := &poller.Poller{Site: cfg.Site, Device: dev, Dial: poller.ModbusDialer, Log: log}
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.Run(ctx, readings)
		}()
	}
	go func() {
		wg.Wait()
		close(readings)
	}()

	enc := json.NewEncoder(os.Stdout)
	for m := range readings { // ends when every poller has stopped
		enc.Encode(m)
	}
	log.Info("stopped")
}
