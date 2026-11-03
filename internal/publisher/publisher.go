// Package publisher sends telemetry to an MQTT broker. While the broker
// is unreachable, messages wait in a bounded buffer and are flushed in
// order once the connection is back.
package publisher

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/BaranTab/industrial-iot-pipeline/internal/buffer"
	"github.com/BaranTab/industrial-iot-pipeline/internal/telemetry"
)

// Outgoing is one MQTT message waiting to be sent.
type Outgoing struct {
	Topic    string
	Payload  []byte
	Retained bool
}

// Options configures a Publisher.
type Options struct {
	Broker      string // tcp://host:1883
	ClientID    string
	TopicPrefix string
	Site        string
	QoS         byte
	BufferSize  int
	Timeout     time.Duration // per publish
	Log         *slog.Logger
}

// Publisher owns the MQTT connection of the gateway.
type Publisher struct {
	opt    Options
	client mqtt.Client
	buf    *buffer.Ring[Outgoing]
	flush  chan struct{}
	last   map[string]string // last status per device: online/offline

	// announce is set by the connect handler; the Run goroutine then
	// publishes the "online" status. Publishing from inside the handler
	// itself races with paho's reconnect logic, so we only signal there.
	announce atomic.Bool
}

// statusPayload is the retained message on .../status topics.
type statusPayload struct {
	State string    `json:"state"`
	Error string    `json:"error,omitempty"`
	Time  time.Time `json:"ts"`
}

// New creates a Publisher; call Connect before Run.
func New(opt Options) *Publisher {
	if opt.Timeout == 0 {
		opt.Timeout = 5 * time.Second
	}
	p := &Publisher{
		opt:   opt,
		buf:   buffer.New[Outgoing](opt.BufferSize),
		flush: make(chan struct{}, 1),
		last:  map[string]string{},
	}
	gatewayStatus := telemetry.StatusTopic(opt.TopicPrefix, opt.Site, "gateway")
	offline, _ := json.Marshal(statusPayload{State: "offline"})

	o := mqtt.NewClientOptions().
		AddBroker(opt.Broker).
		SetClientID(opt.ClientID).
		SetCleanSession(true).
		SetOrderMatters(false).
		SetKeepAlive(10*time.Second).
		SetProtocolVersion(4).            // MQTT 3.1.1; no slow fallback to 3.1
		SetConnectTimeout(5*time.Second). // default 30s: a half-open broker would stall reconnects
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetConnectRetryInterval(2*time.Second).
		SetMaxReconnectInterval(10*time.Second).
		// Last Will: if the gateway dies without saying goodbye, the
		// broker publishes "offline" on its behalf.
		SetWill(gatewayStatus, string(offline), 1, true).
		SetOnConnectHandler(func(mqtt.Client) {
			opt.Log.Info("broker connected", "broker", opt.Broker)
			p.announce.Store(true)
			select { // wake the flusher
			case p.flush <- struct{}{}:
			default:
			}
		}).
		SetConnectionLostHandler(func(_ mqtt.Client, err error) {
			opt.Log.Warn("broker connection lost", "err", err)
		})
	p.client = mqtt.NewClient(o)
	return p
}

// Connect starts connecting in the background. With ConnectRetry the
// client keeps trying until it succeeds, so the gateway can start before
// the broker does.
func (p *Publisher) Connect() {
	p.client.Connect()
}

// Run consumes messages until in is closed or ctx is cancelled.
func (p *Publisher) Run(ctx context.Context, in <-chan telemetry.Message) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case m, ok := <-in:
			if !ok {
				return
			}
			p.handle(m)
		case <-p.flush:
			p.flushBuffer()
		case <-ticker.C:
			p.flushBuffer()
		}
	}
}

// handle turns a poll result into telemetry and/or status messages.
func (p *Publisher) handle(m telemetry.Message) {
	state := "online"
	if m.Quality != telemetry.QualityGood {
		state = "offline"
	}
	if p.last[m.Device] != state { // publish status only when it changes
		p.last[m.Device] = state
		payload, _ := json.Marshal(statusPayload{State: state, Error: m.Error, Time: m.Time})
		p.send(Outgoing{telemetry.StatusTopic(p.opt.TopicPrefix, m.Site, m.Device), payload, true})
	}
	if m.Quality == telemetry.QualityGood {
		payload, err := json.Marshal(m)
		if err != nil {
			p.opt.Log.Error("marshal telemetry", "err", err)
			return
		}
		p.send(Outgoing{Topic: telemetry.TelemetryTopic(p.opt.TopicPrefix, m.Site, m.Device), Payload: payload})
	}
}

// send publishes directly when possible. If the broker is down, or older
// messages are still waiting, it buffers to keep the order.
func (p *Publisher) send(o Outgoing) {
	if p.buf.Len() > 0 || !p.client.IsConnectionOpen() {
		p.enqueue(o)
		return
	}
	if err := p.publish(o); err != nil {
		p.enqueue(o)
	}
}

func (p *Publisher) enqueue(o Outgoing) {
	if p.buf.Push(o) {
		p.opt.Log.Warn("buffer full, dropped oldest message", "dropped_total", p.buf.Dropped())
	}
}

// flushBuffer sends buffered messages oldest first. A message is removed
// only after the broker acknowledged it (Peek, publish, then Pop).
func (p *Publisher) flushBuffer() {
	if p.client.IsConnectionOpen() && p.announce.Swap(false) {
		online, _ := json.Marshal(statusPayload{State: "online", Time: time.Now().UTC()})
		topic := telemetry.StatusTopic(p.opt.TopicPrefix, p.opt.Site, "gateway")
		if err := p.publish(Outgoing{Topic: topic, Payload: online, Retained: true}); err != nil {
			p.announce.Store(true) // try again on the next flush
		}
	}
	sent := 0
	for p.client.IsConnectionOpen() {
		o, ok := p.buf.Peek()
		if !ok {
			break
		}
		if err := p.publish(o); err != nil {
			p.opt.Log.Warn("flush paused", "err", err, "remaining", p.buf.Len())
			break
		}
		p.buf.Pop()
		sent++
	}
	if sent > 0 {
		p.opt.Log.Info("buffer flushed", "sent", sent, "remaining", p.buf.Len())
	}
}

// publish sends one message and waits for the broker's acknowledgement
// (PUBACK for QoS 1).
func (p *Publisher) publish(o Outgoing) error {
	t := p.client.Publish(o.Topic, p.opt.QoS, o.Retained, o.Payload)
	if !t.WaitTimeout(p.opt.Timeout) {
		return errors.New("publish timed out")
	}
	if err := t.Error(); err != nil {
		return err
	}
	return nil
}

// Buffered reports how many messages wait for the broker.
func (p *Publisher) Buffered() int { return p.buf.Len() }

// Close flushes what it can within timeout, marks the gateway offline and
// disconnects cleanly (a clean disconnect does not trigger the Will).
func (p *Publisher) Close(timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for p.buf.Len() > 0 && p.client.IsConnectionOpen() && time.Now().Before(deadline) {
		p.flushBuffer()
	}
	if n := p.buf.Len(); n > 0 {
		p.opt.Log.Warn("shutting down with unsent messages", "count", n)
	}
	if p.client.IsConnectionOpen() {
		offline, _ := json.Marshal(statusPayload{State: "offline", Time: time.Now().UTC()})
		t := p.client.Publish(telemetry.StatusTopic(p.opt.TopicPrefix, p.opt.Site, "gateway"), 1, true, offline)
		t.WaitTimeout(time.Second)
	}
	p.client.Disconnect(250)
}
