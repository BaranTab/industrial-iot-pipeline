package sim

import (
	"math"
	"testing"
)

func TestDeviceStaysNearNominal(t *testing.T) {
	p := Profiles["furnace"]
	d := NewDevice(p, 42)
	for i := 0; i < 1000; i++ {
		d.Step(0) // no faults
		regs, online := d.Registers()
		if !online {
			t.Fatal("device went offline without a fault")
		}
		temp := float64(regs[0]) * 0.1
		limit := p.Temperature * (p.Swing + p.Noise + 0.001)
		if math.Abs(temp-p.Temperature) > limit {
			t.Fatalf("step %d: temperature %.1f outside %.1f ± %.1f", i, temp, p.Temperature, limit)
		}
	}
}

func TestSameSeedSameValues(t *testing.T) {
	a := NewDevice(Profiles["pump"], 7)
	b := NewDevice(Profiles["pump"], 7)
	for i := 0; i < 100; i++ {
		a.Step(0.05)
		b.Step(0.05)
	}
	ra, _ := a.Registers()
	rb, _ := b.Registers()
	if ra != rb {
		t.Fatalf("same seed gave different registers: %v vs %v", ra, rb)
	}
}

func TestOverheatRaisesTemperature(t *testing.T) {
	d := NewDevice(Profiles["furnace"], 1)
	d.Step(0)
	before, _ := d.Registers()
	d.SetFault(FaultOverheat, 100)
	for i := 0; i < 50; i++ {
		d.Step(0)
	}
	after, _ := d.Registers()
	if after[0] <= before[0] {
		t.Fatalf("overheat did not raise temperature: %d -> %d", before[0], after[0])
	}
}

func TestOfflineFault(t *testing.T) {
	d := NewDevice(Profiles["pump"], 1)
	d.SetFault(FaultOffline, 10)
	if _, online := d.Registers(); online {
		t.Fatal("device should be offline")
	}
	for i := 0; i < 10; i++ {
		d.Step(0)
	}
	if _, online := d.Registers(); !online {
		t.Fatal("device should be back online after the fault clears")
	}
}

func TestToRawClamps(t *testing.T) {
	cases := []struct {
		in   float64
		want uint16
	}{{-5, 0}, {0, 0}, {12.4, 12}, {12.5, 13}, {70000, 65535}}
	for _, c := range cases {
		if got := toRaw(c.in); got != c.want {
			t.Errorf("toRaw(%v) = %d, want %d", c.in, got, c.want)
		}
	}
}
