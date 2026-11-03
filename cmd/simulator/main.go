// Command simulator runs fake industrial devices as Modbus TCP servers.
//
//	go run ./cmd/simulator -devices furnace-1=furnace@:5020,pump-1=pump@:5021
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/BaranTab/industrial-iot-pipeline/internal/sim"
)

type deviceSpec struct{ id, profile, addr string }

func main() {
	devicesFlag := flag.String("devices", "furnace-1=furnace@:5020,pump-1=pump@:5021",
		"comma-separated list of id=profile@listen-address")
	tick := flag.Duration("tick", 200*time.Millisecond, "simulation step")
	faultProb := flag.Float64("fault-prob", 0.002, "chance per step that a fault starts")
	seed := flag.Int64("seed", time.Now().UnixNano(), "random seed (fix it to replay a run)")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	specs, err := parseDevices(*devicesFlag)
	if err != nil {
		log.Error("bad -devices flag", "err", err)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	devices := make(map[string]*sim.Device, len(specs))
	for i, s := range specs {
		d := sim.NewDevice(sim.Profiles[s.profile], *seed+int64(i))
		srv, err := sim.Serve(s.addr, d)
		if err != nil {
			log.Error("start device", "device", s.id, "err", err)
			os.Exit(1)
		}
		defer srv.Stop()
		devices[s.id] = d
		log.Info("device listening", "device", s.id, "profile", s.profile, "addr", s.addr)
	}

	ticker := time.NewTicker(*tick)
	defer ticker.Stop()
	last := map[string]sim.Fault{}
	for {
		select {
		case <-ctx.Done():
			log.Info("shutting down")
			return
		case <-ticker.C:
			for id, d := range devices {
				d.Step(*faultProb)
				if f := d.Fault(); f != last[id] {
					log.Warn("fault changed", "device", id, "from", last[id].String(), "to", f.String())
					last[id] = f
				}
			}
		}
	}
}

func parseDevices(s string) ([]deviceSpec, error) {
	var specs []deviceSpec
	for _, part := range strings.Split(s, ",") {
		id, rest, ok1 := strings.Cut(strings.TrimSpace(part), "=")
		profile, addr, ok2 := strings.Cut(rest, "@")
		if !ok1 || !ok2 || id == "" || addr == "" {
			return nil, fmt.Errorf("%q: want id=profile@address", part)
		}
		if _, ok := sim.Profiles[profile]; !ok {
			return nil, fmt.Errorf("%q: unknown profile %q", part, profile)
		}
		specs = append(specs, deviceSpec{id, profile, addr})
	}
	return specs, nil
}
