// Package poller reads Modbus devices on a schedule and turns raw
// registers into telemetry messages.
package poller

import (
	"fmt"
	"math"

	"github.com/BaranTab/industrial-iot-pipeline/internal/config"
)

// BlockRange returns the smallest contiguous register block that covers
// every configured register. Reading one block per poll is much faster
// than one request per register: each request is a network round trip.
func BlockRange(regs []config.Register) (start, quantity uint16) {
	if len(regs) == 0 {
		return 0, 0
	}
	lo, hi := regs[0].Address, regs[0].Address
	for _, r := range regs[1:] {
		lo = min(lo, r.Address)
		hi = max(hi, r.Address)
	}
	return lo, hi - lo + 1
}

// Decode maps the raw block (read from address start) to engineering
// values keyed by register name.
func Decode(regs []config.Register, start uint16, raw []uint16) (map[string]float64, error) {
	out := make(map[string]float64, len(regs))
	for _, r := range regs {
		i := int(r.Address) - int(start)
		if i < 0 || i >= len(raw) {
			return nil, fmt.Errorf("register %s at %d is outside the block read from %d (%d registers)",
				r.Name, r.Address, start, len(raw))
		}
		v := float64(raw[i])
		if r.Signed {
			v = float64(int16(raw[i])) // two's complement
		}
		// 559 * 0.1 is 55.900000000000006 in floating point; round to
		// 6 decimals so the JSON shows 55.9.
		out[r.Name] = math.Round(v*r.Scale*1e6) / 1e6
	}
	return out, nil
}
