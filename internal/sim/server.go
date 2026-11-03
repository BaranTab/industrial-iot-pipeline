package sim

import (
	"fmt"
	"time"

	"github.com/simonvetter/modbus"
)

// Handler answers Modbus requests for one Device. It implements
// modbus.RequestHandler; only input registers are supported, like a
// read-only sensor.
type Handler struct {
	Device *Device
}

// HandleInputRegisters serves function code 0x04.
func (h *Handler) HandleInputRegisters(req *modbus.InputRegistersRequest) ([]uint16, error) {
	regs, online := h.Device.Registers()
	if !online {
		return nil, modbus.ErrServerDeviceFailure
	}
	end := int(req.Addr) + int(req.Quantity)
	if end > NumRegisters {
		return nil, modbus.ErrIllegalDataAddress
	}
	return regs[req.Addr:end], nil
}

// The remaining function codes are not supported by this device.

func (h *Handler) HandleCoils(*modbus.CoilsRequest) ([]bool, error) {
	return nil, modbus.ErrIllegalFunction
}

func (h *Handler) HandleDiscreteInputs(*modbus.DiscreteInputsRequest) ([]bool, error) {
	return nil, modbus.ErrIllegalFunction
}

func (h *Handler) HandleHoldingRegisters(*modbus.HoldingRegistersRequest) ([]uint16, error) {
	return nil, modbus.ErrIllegalFunction
}

// Serve starts a Modbus TCP server for d on addr (e.g. "0.0.0.0:5020").
// Call Stop on the returned server to shut it down.
func Serve(addr string, d *Device) (*modbus.ModbusServer, error) {
	srv, err := modbus.NewServer(&modbus.ServerConfiguration{
		URL:        "tcp://" + addr,
		Timeout:    30 * time.Second,
		MaxClients: 5,
	}, &Handler{Device: d})
	if err != nil {
		return nil, fmt.Errorf("new modbus server: %w", err)
	}
	if err := srv.Start(); err != nil {
		return nil, fmt.Errorf("start modbus server on %s: %w", addr, err)
	}
	return srv, nil
}
