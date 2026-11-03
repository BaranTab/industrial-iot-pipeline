// Package telemetry defines the message format shared by the gateway and
// the ingestor. Changing it is a contract change: bump Version.
package telemetry

import (
	"fmt"
	"time"
)

// Version of the message schema.
const Version = 1

// Quality of a reading, borrowed from OPC UA's good/bad idea.
const (
	QualityGood = "good"
	QualityBad  = "bad"
)

// Message is one poll of one device.
type Message struct {
	V       int                `json:"v"`
	Site    string             `json:"site"`
	Device  string             `json:"device"`
	Time    time.Time          `json:"ts"`
	Quality string             `json:"quality"`
	Values  map[string]float64 `json:"values,omitempty"`
	Error   string             `json:"error,omitempty"`
}

// TelemetryTopic returns e.g. plant/karabuk/furnace-1/telemetry.
func TelemetryTopic(prefix, site, device string) string {
	return fmt.Sprintf("%s/%s/%s/telemetry", prefix, site, device)
}

// StatusTopic returns e.g. plant/karabuk/furnace-1/status.
func StatusTopic(prefix, site, device string) string {
	return fmt.Sprintf("%s/%s/%s/status", prefix, site, device)
}

// TelemetryWildcard subscribes to every device of every site.
func TelemetryWildcard(prefix string) string {
	return prefix + "/+/+/telemetry"
}
