// Package config loads and validates the gateway configuration.
package config

import (
	"errors"
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Register describes one Modbus input register and how to turn its raw
// value into an engineering value: value = raw * Scale.
type Register struct {
	Name    string  `yaml:"name"`    // metric name, e.g. temperature_c
	Address uint16  `yaml:"address"` // 0-based register address
	Scale   float64 `yaml:"scale"`   // multiplier applied to the raw value
	Signed  bool    `yaml:"signed"`  // interpret raw value as int16
	Unit    string  `yaml:"unit"`    // documentation only, e.g. "°C"
}

// Device is one Modbus TCP device (a PLC or a sensor gateway).
type Device struct {
	ID           string        `yaml:"id"`
	Address      string        `yaml:"address"` // host:port
	UnitID       uint8         `yaml:"unit_id"`
	PollInterval time.Duration `yaml:"poll_interval"`
	Timeout      time.Duration `yaml:"timeout"`
	Registers    []Register    `yaml:"registers"`
}

// MQTT holds broker connection settings.
type MQTT struct {
	Broker      string `yaml:"broker"` // e.g. tcp://localhost:1883
	ClientID    string `yaml:"client_id"`
	TopicPrefix string `yaml:"topic_prefix"`
	QoS         byte   `yaml:"qos"`
}

// Gateway is the root configuration object.
type Gateway struct {
	Site        string   `yaml:"site"`
	MQTT        MQTT     `yaml:"mqtt"`
	BufferSize  int      `yaml:"buffer_size"`
	MetricsAddr string   `yaml:"metrics_addr"`
	Devices     []Device `yaml:"devices"`
}

// Load reads a YAML file, applies defaults and environment overrides,
// and validates the result.
func Load(path string) (*Gateway, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var cfg Gateway
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	cfg.applyDefaults()
	if v := os.Getenv("MQTT_BROKER"); v != "" {
		cfg.MQTT.Broker = v
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (g *Gateway) applyDefaults() {
	if g.MQTT.ClientID == "" {
		g.MQTT.ClientID = "gateway-" + g.Site
	}
	if g.MQTT.TopicPrefix == "" {
		g.MQTT.TopicPrefix = "plant"
	}
	if g.MQTT.QoS == 0 {
		g.MQTT.QoS = 1
	}
	if g.BufferSize == 0 {
		g.BufferSize = 10000
	}
	if g.MetricsAddr == "" {
		g.MetricsAddr = ":9100"
	}
	for i := range g.Devices {
		d := &g.Devices[i]
		if d.PollInterval == 0 {
			d.PollInterval = time.Second
		}
		if d.Timeout == 0 {
			d.Timeout = 2 * time.Second
		}
		if d.UnitID == 0 {
			d.UnitID = 1
		}
		for j := range d.Registers {
			if d.Registers[j].Scale == 0 {
				d.Registers[j].Scale = 1
			}
		}
	}
}

// Validate returns every problem it finds, joined into one error.
func (g *Gateway) Validate() error {
	var errs []error
	if g.Site == "" {
		errs = append(errs, errors.New("site is required"))
	}
	if g.MQTT.Broker == "" {
		errs = append(errs, errors.New("mqtt.broker is required"))
	}
	if g.MQTT.QoS > 2 {
		errs = append(errs, fmt.Errorf("mqtt.qos must be 0, 1 or 2, got %d", g.MQTT.QoS))
	}
	if len(g.Devices) == 0 {
		errs = append(errs, errors.New("at least one device is required"))
	}
	seen := map[string]bool{}
	for _, d := range g.Devices {
		if d.ID == "" {
			errs = append(errs, errors.New("device id is required"))
			continue
		}
		if seen[d.ID] {
			errs = append(errs, fmt.Errorf("duplicate device id %q", d.ID))
		}
		seen[d.ID] = true
		if d.Address == "" {
			errs = append(errs, fmt.Errorf("device %q: address is required", d.ID))
		}
		if len(d.Registers) == 0 {
			errs = append(errs, fmt.Errorf("device %q: at least one register is required", d.ID))
		}
		names := map[string]bool{}
		for _, r := range d.Registers {
			if r.Name == "" {
				errs = append(errs, fmt.Errorf("device %q: register at address %d has no name", d.ID, r.Address))
			}
			if names[r.Name] {
				errs = append(errs, fmt.Errorf("device %q: duplicate register name %q", d.ID, r.Name))
			}
			names[r.Name] = true
		}
	}
	return errors.Join(errs...)
}
