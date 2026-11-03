// Command gateway polls Modbus devices and publishes their readings to MQTT.
//
//	go run ./cmd/gateway -config configs/gateway.local.yaml
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/BaranTab/industrial-iot-pipeline/internal/config"
	"github.com/BaranTab/industrial-iot-pipeline/internal/poller"
	"github.com/BaranTab/industrial-iot-pipeline/internal/publisher"
	"github.com/BaranTab/industrial-iot-pipeline/internal/telemetry"
)

func main() {
	configPath := flag.String("config", "configs/gateway.yaml", "path to the YAML config")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Error("invalid config", "err", err)
		os.Exit(2)
	}

	// ctx is cancelled on Ctrl+C or `docker stop` (SIGTERM).
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pub := publisher.New(publisher.Options{
		Broker:      cfg.MQTT.Broker,
		ClientID:    cfg.MQTT.ClientID,
		TopicPrefix: cfg.MQTT.TopicPrefix,
		Site:        cfg.Site,
		QoS:         cfg.MQTT.QoS,
		BufferSize:  cfg.BufferSize,
		Log:         log,
	})
	pub.Connect()

	// One goroutine per device; all of them feed one channel.
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

	// When every poller has stopped, close the channel so the publisher
	// knows no more readings will come.
	go func() {
		wg.Wait()
		close(readings)
	}()

	log.Info("gateway started", "site", cfg.Site, "devices", len(cfg.Devices))
	pub.Run(context.Background(), readings) // returns when readings is closed

	log.Info("shutting down")
	pub.Close(5 * time.Second)
}
