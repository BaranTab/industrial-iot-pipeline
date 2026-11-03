package poller

import (
	"math"
	"testing"

	"github.com/BaranTab/industrial-iot-pipeline/internal/config"
)

func TestBlockRange(t *testing.T) {
	tests := []struct {
		name      string
		addrs     []uint16
		wantStart uint16
		wantQty   uint16
	}{
		{"empty", nil, 0, 0},
		{"single", []uint16{7}, 7, 1},
		{"contiguous", []uint16{0, 1, 2, 3, 4}, 0, 5},
		{"unordered with gap", []uint16{10, 3, 6}, 3, 8},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var regs []config.Register
			for _, a := range tt.addrs {
				regs = append(regs, config.Register{Address: a})
			}
			start, qty := BlockRange(regs)
			if start != tt.wantStart || qty != tt.wantQty {
				t.Fatalf("got (%d,%d) want (%d,%d)", start, qty, tt.wantStart, tt.wantQty)
			}
		})
	}
}

func TestDecode(t *testing.T) {
	regs := []config.Register{
		{Name: "temperature_c", Address: 0, Scale: 0.1},
		{Name: "pressure_bar", Address: 1, Scale: 0.01},
		{Name: "offset", Address: 2, Scale: 1, Signed: true},
	}
	raw := []uint16{14503, 120, 0xFFFE} // 0xFFFE = -2 as int16
	got, err := Decode(regs, 0, raw)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]float64{"temperature_c": 1450.3, "pressure_bar": 1.2, "offset": -2}
	for k, w := range want {
		if math.Abs(got[k]-w) > 1e-9 {
			t.Errorf("%s: got %v want %v", k, got[k], w)
		}
	}
}

func TestDecodeOutOfBlock(t *testing.T) {
	regs := []config.Register{{Name: "x", Address: 9, Scale: 1}}
	if _, err := Decode(regs, 0, []uint16{1, 2}); err == nil {
		t.Fatal("expected an error for a register outside the block")
	}
}
