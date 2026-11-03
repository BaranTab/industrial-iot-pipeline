package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeFile(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "gateway.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadAppliesDefaults(t *testing.T) {
	path := writeFile(t, `
site: test
mqtt: { broker: "tcp://localhost:1883" }
devices:
  - id: d1
    address: localhost:5020
    registers: [ { name: t, address: 0 } ]
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	d := cfg.Devices[0]
	if d.PollInterval != time.Second || d.UnitID != 1 || d.Registers[0].Scale != 1 {
		t.Fatalf("defaults not applied: %+v", d)
	}
	if cfg.MQTT.QoS != 1 || cfg.MQTT.TopicPrefix != "plant" {
		t.Fatalf("mqtt defaults not applied: %+v", cfg.MQTT)
	}
}

func TestLoadRealConfig(t *testing.T) {
	cfg, err := Load("../../configs/gateway.local.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Devices) != 2 || len(cfg.Devices[1].Registers) != 5 {
		t.Fatalf("unexpected config: %d devices", len(cfg.Devices))
	}
}

func TestValidateReportsAllProblems(t *testing.T) {
	path := writeFile(t, `
devices:
  - id: d1
    registers: [ { name: t, address: 0 }, { name: t, address: 1 } ]
  - id: d1
    address: x
`)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected an error")
	}
	for _, want := range []string{"site is required", "mqtt.broker", "address is required", "duplicate register", "duplicate device"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error should mention %q, got:\n%v", want, err)
		}
	}
}

func TestEnvOverridesBroker(t *testing.T) {
	t.Setenv("MQTT_BROKER", "tcp://mosquitto:1883")
	cfg, err := Load("../../configs/gateway.local.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MQTT.Broker != "tcp://mosquitto:1883" {
		t.Fatalf("broker = %s", cfg.MQTT.Broker)
	}
}
