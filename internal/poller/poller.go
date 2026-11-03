package poller

import (
	"context"
	"log/slog"
	"time"

	"github.com/simonvetter/modbus"

	"github.com/BaranTab/industrial-iot-pipeline/internal/config"
	"github.com/BaranTab/industrial-iot-pipeline/internal/telemetry"
)

// Client is the part of a Modbus client the poller needs. Depending on an
// interface instead of *modbus.ModbusClient lets tests use a fake.
type Client interface {
	Open() error
	Close() error
	ReadRegisters(addr, quantity uint16, regType modbus.RegType) ([]uint16, error)
}

// Dialer creates a client for a device.
type Dialer func(d config.Device) (Client, error)

// ModbusDialer creates real Modbus TCP clients.
func ModbusDialer(d config.Device) (Client, error) {
	c, err := modbus.NewClient(&modbus.ClientConfiguration{
		URL:     "tcp://" + d.Address,
		Timeout: d.Timeout,
	})
	if err != nil {
		return nil, err
	}
	if err := c.SetUnitId(d.UnitID); err != nil {
		return nil, err
	}
	return c, nil
}

// Poller polls one device.
type Poller struct {
	Site   string
	Device config.Device
	Dial   Dialer
	Log    *slog.Logger
	Now    func() time.Time // injectable clock for tests

	// Backoff limits for reconnecting.
	MinBackoff, MaxBackoff time.Duration
}

// Run polls until ctx is cancelled, sending one message per poll to out.
// A failed read produces a "bad" quality message, then the connection is
// re-opened with exponential backoff.
func (p *Poller) Run(ctx context.Context, out chan<- telemetry.Message) {
	if p.Now == nil {
		p.Now = time.Now
	}
	if p.MinBackoff == 0 {
		p.MinBackoff = time.Second
	}
	if p.MaxBackoff == 0 {
		p.MaxBackoff = 30 * time.Second
	}
	log := p.Log.With("device", p.Device.ID)
	backoff := p.MinBackoff

	for ctx.Err() == nil {
		client, err := p.connect()
		if err != nil {
			log.Warn("connect failed", "err", err, "retry_in", backoff)
			p.emit(ctx, out, p.badMessage(err))
			if !sleep(ctx, backoff) {
				return
			}
			backoff = min(backoff*2, p.MaxBackoff)
			continue
		}
		log.Info("connected", "addr", p.Device.Address)
		backoff = p.MinBackoff

		err = p.pollLoop(ctx, client, out)
		_ = client.Close()
		if err != nil {
			log.Warn("poll failed, reconnecting", "err", err)
			p.emit(ctx, out, p.badMessage(err))
		}
	}
}

func (p *Poller) connect() (Client, error) {
	c, err := p.Dial(p.Device)
	if err != nil {
		return nil, err
	}
	if err := c.Open(); err != nil {
		return nil, err
	}
	return c, nil
}

// pollLoop reads on every tick and returns on the first error.
func (p *Poller) pollLoop(ctx context.Context, c Client, out chan<- telemetry.Message) error {
	start, qty := BlockRange(p.Device.Registers)
	ticker := time.NewTicker(p.Device.PollInterval)
	defer ticker.Stop()
	for {
		raw, err := c.ReadRegisters(start, qty, modbus.INPUT_REGISTER)
		if err != nil {
			return err
		}
		values, err := Decode(p.Device.Registers, start, raw)
		if err != nil {
			return err
		}
		p.emit(ctx, out, telemetry.Message{
			V: telemetry.Version, Site: p.Site, Device: p.Device.ID,
			Time: p.Now().UTC(), Quality: telemetry.QualityGood, Values: values,
		})

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (p *Poller) badMessage(err error) telemetry.Message {
	return telemetry.Message{
		V: telemetry.Version, Site: p.Site, Device: p.Device.ID,
		Time: p.Now().UTC(), Quality: telemetry.QualityBad, Error: err.Error(),
	}
}

// emit sends without blocking forever if the consumer is gone.
func (p *Poller) emit(ctx context.Context, out chan<- telemetry.Message, m telemetry.Message) {
	select {
	case out <- m:
	case <-ctx.Done():
	}
}

// sleep waits for d or until ctx is cancelled; it reports false if cancelled.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
