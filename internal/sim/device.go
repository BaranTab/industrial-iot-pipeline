// Package sim simulates industrial devices that expose their readings as
// Modbus input registers.
//
// Register map (input registers, function code 0x04):
//
//	addr  name            raw unit   scale  example raw -> value
//	0     temperature_c   0.1 °C     0.1    12345 -> 1234.5 °C
//	1     pressure_bar    0.01 bar   0.01     250 -> 2.50 bar
//	2     vibration_mm_s  0.01 mm/s  0.01     180 -> 1.80 mm/s
//	3     current_a       0.1 A      0.1      452 -> 45.2 A
//	4     running         0/1        1          1 -> running
package sim

import (
	"math"
	"math/rand"
	"sync"
)

// NumRegisters is the size of the register map above.
const NumRegisters = 5

// Fault is an abnormal condition the simulator can inject.
type Fault int

const (
	FaultNone     Fault = iota
	FaultOverheat       // temperature climbs steadily
	FaultStuck          // sensor freezes on its last value
	FaultOffline        // device stops answering Modbus requests
)

func (f Fault) String() string {
	return [...]string{"none", "overheat", "stuck", "offline"}[f]
}

// Profile holds the normal operating point of a device type.
type Profile struct {
	Temperature, Pressure, Vibration, Current float64 // nominal values
	Swing                                     float64 // relative amplitude of the slow cycle, e.g. 0.03 = ±3%
	Noise                                     float64 // relative noise, e.g. 0.005 = ±0.5%
	Period                                    float64 // length of the slow cycle in steps
}

// Profiles are loosely based on real equipment, but are not real data.
var Profiles = map[string]Profile{
	"furnace": {Temperature: 1450, Pressure: 1.2, Vibration: 1.5, Current: 320, Swing: 0.02, Noise: 0.004, Period: 600},
	"pump":    {Temperature: 55, Pressure: 6.5, Vibration: 2.8, Current: 45, Swing: 0.05, Noise: 0.01, Period: 300},
}

// Device is one simulated machine. It is safe for concurrent use: the
// simulation loop calls Step while the Modbus server calls Registers.
type Device struct {
	mu        sync.Mutex
	profile   Profile
	rng       *rand.Rand
	step      int
	values    [NumRegisters]float64 // engineering values
	fault     Fault
	faultLeft int // steps until the fault clears
}

// NewDevice creates a device with a deterministic random source, so tests
// and demos can be reproduced with the same seed.
func NewDevice(p Profile, seed int64) *Device {
	d := &Device{profile: p, rng: rand.New(rand.NewSource(seed))}
	d.values = [NumRegisters]float64{p.Temperature, p.Pressure, p.Vibration, p.Current, 1}
	return d
}

// Step advances the simulation by one tick. faultProb is the chance per
// step that a new fault starts (when none is active).
func (d *Device) Step(faultProb float64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.step++

	if d.fault == FaultNone && d.rng.Float64() < faultProb {
		d.fault = Fault(1 + d.rng.Intn(3))
		d.faultLeft = 50 + d.rng.Intn(100)
	}
	if d.fault != FaultNone {
		d.faultLeft--
		if d.faultLeft <= 0 {
			d.fault = FaultNone
		}
	}

	switch d.fault {
	case FaultStuck, FaultOffline:
		return // values freeze
	case FaultOverheat:
		d.values[0] *= 1.004 // about +0.4% per step
		d.values[2] *= 1.01  // vibration rises with it
		return
	}

	p := d.profile
	cycle := math.Sin(2 * math.Pi * float64(d.step) / p.Period)
	d.values[0] = d.wobble(p.Temperature, p.Swing, p.Noise, cycle)
	d.values[1] = d.wobble(p.Pressure, p.Swing, p.Noise, cycle)
	d.values[2] = d.wobble(p.Vibration, p.Swing, p.Noise*4, cycle) // vibration is noisier
	d.values[3] = d.wobble(p.Current, p.Swing, p.Noise, cycle)
	d.values[4] = 1
}

// wobble returns nominal value + slow cycle + random noise.
func (d *Device) wobble(nominal, swing, noise, cycle float64) float64 {
	return nominal * (1 + swing*cycle + noise*(2*d.rng.Float64()-1))
}

// Registers returns the raw register values and whether the device
// currently answers requests.
func (d *Device) Registers() (regs [NumRegisters]uint16, online bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	scales := [NumRegisters]float64{0.1, 0.01, 0.01, 0.1, 1}
	for i, v := range d.values {
		regs[i] = toRaw(v / scales[i])
	}
	return regs, d.fault != FaultOffline
}

// Fault returns the active fault, for logging.
func (d *Device) Fault() Fault {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.fault
}

// SetFault forces a fault; useful in tests and demos.
func (d *Device) SetFault(f Fault, steps int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.fault, d.faultLeft = f, steps
}

// toRaw rounds and clamps a value into the uint16 range of a register.
func toRaw(v float64) uint16 {
	switch {
	case v <= 0:
		return 0
	case v >= math.MaxUint16:
		return math.MaxUint16
	default:
		return uint16(math.Round(v))
	}
}
