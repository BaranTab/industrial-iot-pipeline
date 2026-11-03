package publisher

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	mochi "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/BaranTab/industrial-iot-pipeline/internal/telemetry"
)

// startBroker runs an in-memory MQTT broker on addr. Tests can stop it to
// simulate a network outage and start it again on the same address.
// The collector subscribes inside the broker (an "inline" client), so it
// never misses a message while reconnecting.
func startBroker(t *testing.T, addr string, col *collector) *mochi.Server {
	t.Helper()
	s := mochi.New(&mochi.Options{InlineClient: true, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err := s.AddHook(new(auth.AllowHook), nil); err != nil {
		t.Fatal(err)
	}
	if err := s.AddListener(listeners.NewTCP(listeners.Config{ID: "t", Address: addr})); err != nil {
		t.Fatal(err)
	}
	if err := s.Subscribe(telemetry.TelemetryWildcard("plant"), 1, col.handle); err != nil {
		t.Fatal(err)
	}
	go func() {
		if err := s.Serve(); err != nil {
			t.Logf("broker serve: %v", err)
		}
	}()
	return s
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

// collector subscribes to telemetry and records the sequence numbers it sees.
type collector struct {
	mu   sync.Mutex
	seqs []int
}

func (c *collector) handle(_ *mochi.Client, _ packets.Subscription, pk packets.Packet) {
	var msg telemetry.Message
	if err := json.Unmarshal(pk.Payload, &msg); err != nil {
		return
	}
	c.mu.Lock()
	c.seqs = append(c.seqs, int(msg.Values["seq"]))
	c.mu.Unlock()
}

// unique returns how many different sequence numbers arrived.
func (c *collector) unique() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	seen := map[int]bool{}
	for _, s := range c.seqs {
		seen[s] = true
	}
	return len(seen)
}

func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestBuffersDuringOutageAndFlushesInOrder is the most important test of
// the gateway: no reading may be lost or reordered by a broker outage.
func TestBuffersDuringOutageAndFlushesInOrder(t *testing.T) {
	addr := freeAddr(t)
	broker := "tcp://" + addr
	col := &collector{}
	srv := startBroker(t, addr, col)

	p := New(Options{Broker: broker, ClientID: "gw-test", TopicPrefix: "plant", Site: "test",
		QoS: 1, BufferSize: 1000, Timeout: time.Second,
		Log: slog.New(slog.NewTextHandler(io.Discard, nil))})
	p.Connect()
	defer p.Close(time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	in := make(chan telemetry.Message)
	go p.Run(ctx, in)

	msg := func(seq int) telemetry.Message {
		return telemetry.Message{V: 1, Site: "test", Device: "d1", Time: time.Now(),
			Quality: telemetry.QualityGood, Values: map[string]float64{"seq": float64(seq)}}
	}

	// 1. Broker up: 0..9 go straight through.
	for i := 0; i < 10; i++ {
		in <- msg(i)
	}
	waitFor(t, "first 10 messages", 3*time.Second, func() bool { return col.unique() == 10 })

	// 2. Broker down: 10..29 must be buffered.
	_ = srv.Close()
	waitFor(t, "disconnect", 3*time.Second, func() bool { return !p.client.IsConnectionOpen() })
	for i := 10; i < 30; i++ {
		in <- msg(i)
	}
	waitFor(t, "20 buffered messages", time.Second, func() bool { return p.Buffered() == 20 })

	// 3. Broker back: everything arrives, in order.
	srv = startBroker(t, addr, col)
	defer srv.Close()
	waitFor(t, "all 30 messages", 30*time.Second, func() bool { return col.unique() == 30 })

	// QoS 1 means "at least once": a message whose PUBACK was lost is sent
	// again, so duplicates are allowed. Gaps and reordering are not.
	col.mu.Lock()
	defer col.mu.Unlock()
	next := 0
	for _, s := range col.seqs {
		switch {
		case s == next:
			next++
		case s < next: // duplicate of something already seen
		default:
			t.Fatalf("got seq %d before %d: order broken: %v", s, next, col.seqs)
		}
	}
	t.Logf("received %d messages for 30 readings", len(col.seqs))
}
