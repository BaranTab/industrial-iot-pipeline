package poller

import (
	"context"
	"io"
	"log/slog"
	"math"
	"net"
	"testing"
	"time"

	"github.com/BaranTab/industrial-iot-pipeline/internal/config"
	"github.com/BaranTab/industrial-iot-pipeline/internal/sim"
	"github.com/BaranTab/industrial-iot-pipeline/internal/telemetry"
)

// freeAddr asks the OS for an unused local port.
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func testDevice(addr string) config.Device {
	return config.Device{
		ID: "furnace-1", Address: addr, UnitID: 1,
		PollInterval: 50 * time.Millisecond, Timeout: 500 * time.Millisecond,
		Registers: []config.Register{
			{Name: "temperature_c", Address: 0, Scale: 0.1},
			{Name: "pressure_bar", Address: 1, Scale: 0.01},
			{Name: "running", Address: 4, Scale: 1},
		},
	}
}

// TestPollerReadsSimulator runs a real Modbus server and checks that the
// poller produces correctly scaled values.
func TestPollerReadsSimulator(t *testing.T) {
	addr := freeAddr(t)
	dev := sim.NewDevice(sim.Profiles["furnace"], 1)
	srv, err := sim.Serve(addr, dev)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	out := make(chan telemetry.Message, 10)
	p := &Poller{Site: "test", Device: testDevice(addr), Dial: ModbusDialer,
		Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	go p.Run(ctx, out)

	select {
	case m := <-out:
		if m.Quality != telemetry.QualityGood {
			t.Fatalf("quality %q, error %q", m.Quality, m.Error)
		}
		if math.Abs(m.Values["temperature_c"]-1450) > 0.1 {
			t.Errorf("temperature_c = %v, want 1450", m.Values["temperature_c"])
		}
		if m.Values["running"] != 1 {
			t.Errorf("running = %v, want 1", m.Values["running"])
		}
	case <-ctx.Done():
		t.Fatal("no message within timeout")
	}
}

// TestPollerReportsOfflineDevice checks that a device fault becomes a
// "bad" message and that the poller recovers afterwards.
func TestPollerReportsOfflineDevice(t *testing.T) {
	addr := freeAddr(t)
	dev := sim.NewDevice(sim.Profiles["pump"], 1)
	dev.SetFault(sim.FaultOffline, 1_000_000)
	srv, err := sim.Serve(addr, dev)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out := make(chan telemetry.Message, 10)
	p := &Poller{Site: "test", Device: testDevice(addr), Dial: ModbusDialer,
		Log:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		MinBackoff: 20 * time.Millisecond, MaxBackoff: 50 * time.Millisecond}
	go p.Run(ctx, out)

	if m := <-out; m.Quality != telemetry.QualityBad {
		t.Fatalf("want a bad message while offline, got %q", m.Quality)
	}
	dev.SetFault(sim.FaultNone, 0)
	for {
		select {
		case m := <-out:
			if m.Quality == telemetry.QualityGood {
				return // recovered
			}
		case <-ctx.Done():
			t.Fatal("poller did not recover after the device came back")
		}
	}
}

// TestPollerBacksOffWhenReadsFail guards against a busy loop: the device
// accepts connections but fails every read, so the poller must wait
// between attempts instead of reconnecting thousands of times a second.
func TestPollerBacksOffWhenReadsFail(t *testing.T) {
	addr := freeAddr(t)
	dev := sim.NewDevice(sim.Profiles["pump"], 1)
	dev.SetFault(sim.FaultOffline, 1_000_000)
	srv, err := sim.Serve(addr, dev)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	out := make(chan telemetry.Message, 1000)
	p := &Poller{Site: "test", Device: testDevice(addr), Dial: ModbusDialer,
		Log:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		MinBackoff: 50 * time.Millisecond, MaxBackoff: time.Second}
	p.Run(ctx, out) // returns when ctx expires

	// Waits of 50+100+200 ms fit in 500 ms, so expect about 4 attempts.
	if n := len(out); n < 2 || n > 6 {
		t.Fatalf("got %d bad messages in 500ms, want 2-6 (no backoff?)", n)
	}
}
