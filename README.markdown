```markdown
# ModbusTCPSimulator

A Python-based Modbus TCP server simulator designed to emulate devices like the Carlo Gavazzi EM340 energy meter. It supports multiple concurrent simulations, each configured via YAML files, with dynamic register updates, energy accumulation, and a curses-based terminal UI for real-time monitoring.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Features

- **Multiple Simulations**: Run multiple Modbus TCP servers concurrently, each with its own IP, port, slave ID, and register configuration.
- **Dynamic Registers**: Supports randomization, mathematical expressions (e.g., power = voltage * current * power factor), and energy accumulation (kWh).
- **Persistence**: Saves accumulated values (e.g., kWh) to JSON files, unique per configuration.
- **Terminal UI**: Curses-based interface to monitor simulations and register values in real-time.
- **Compatibility**: Works with Modbus clients like Modbus Poll for testing.
- **Robust Logging**: Detailed logs for debugging and diagnostics.

## Installation

### Prerequisites
- Python 3.8+
- A terminal supporting curses (Linux/macOS: built-in; Windows: requires `windows-curses`)

### Steps
1. Clone the repository:
   ```bash
   git clone https://github.com/[YourUsername]/ModbusTCPSimulator.git
   cd ModbusTCPSimulator
   ```

2. Create a virtual environment (optional but recommended):
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Copy the example configuration:
   ```bash
   cp config_example.yaml config.yaml
   ```

## Usage

1. **Prepare Configuration**:
   - Edit `config.yaml` or create new YAML files in the project directory.
   - See `config_example.yaml` for the required format.

2. **Run the Simulator**:
   ```bash
   python modbus_tcp_simulator.py
   ```

3. **Configure Simulations**:
   - Select a YAML config file by number.
   - Enter IP address (e.g., `127.0.0.1`), port (e.g., `502`), and slave ID (e.g., `1`).
   - Add multiple simulations by selecting "y" when prompted.

4. **Interact with the UI**:
   - **UP/DOWN**: Switch between simulations.
   - **'a'**: Add a new simulation.
   - **'q'**: Quit the application.

5. **Test with Modbus Client**:
   - Use Modbus Poll or similar to connect to the configured IP:port and slave ID.
   - Read registers (e.g., addresses 0, 7, 16) to verify dynamic updates.

## Configuration

The simulator uses YAML files to define registers and simulation parameters. Key fields include:

- **defaults**: Default IP, port, and slave ID.
- **registers**: List of registers with:
  - `address`: Modbus register address.
  - `name`: Unique register name.
  - `type`: Data type (`uint16`, `uint32`, `int16`, `int32`, `float32`).
  - `scale`: Scaling factor for encoding/decoding.
  - `base_value`: Initial value (optional).
  - `randomize`: Enable randomization (optional, requires `fluctuation`).
  - `expression`: Mathematical expression for computed values (optional).
  - `accumulate`: Enable accumulation (e.g., kWh, requires `source`).
  - `persist`: Save value to JSON (optional, requires `accumulate`).
  - `writable`: Allow Modbus writes (optional, requires `variable_name`).

Example (`config_example.yaml`):
```yaml
defaults:
  ip: "127.0.0.1"
  port: 502
  slave_id: 1
registers:
  - address: 99
    name: voltage_l1_n1
    description: Voltage L1-N (V)
    type: uint16
    scale: 10.0
    base_value: 230
    randomize: true
    fluctuation: 0.05
  - address: 7
    name: power_l1
    description: Active Power L1 (kW)
    type: uint16
    scale: 1000.0
    expression: "voltage_l1_n1 * current_l1 * (pf_l1 / 100.0) / 10"
```

## Logging

Logs are saved to `simulator.log` with detailed information on:
- Configuration loading
- Register initialization and updates
- Modbus server status
- UI errors

Set `logging.basicConfig(level=logging.DEBUG)` in the code for more verbose output.

## Troubleshooting

- **"Terminal too small"**: Resize your terminal to at least 80x20.
- **"Address already in use"**: Ensure no other process is using the IP:port.
- **Static values**: Check `simulator.log` for errors in randomization or expressions.
- **No YAML files found**: Place valid `.yaml` files in the project directory.

## Contributing

Contributions are welcome! Please:
1. Fork the repository.
2. Create a feature branch (`git checkout -b feature/YourFeature`).
3. Commit changes (`git commit -m "Add YourFeature"`).
4. Push to the branch (`git push origin feature/YourFeature`).
5. Open a Pull Request.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Inspired by the need to simulate Modbus TCP devices for testing.
- Built with [pymodbus](https://pymodbus.readthedocs.io/), [PyYAML](https://pyyaml.org/), and [windows-curses](https://github.com/zephyrproject-rtos/windows-curses).
```