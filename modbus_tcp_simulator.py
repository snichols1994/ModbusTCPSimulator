"""
ModbusTCPSimulator (Version 1.0)

A Modbus TCP server simulator designed to emulate devices like the Carlo Gavazzi EM340 energy meter.
It supports multiple concurrent simulations, each configured via a YAML file, with features including:
- Dynamic register updates with randomization and mathematical expressions (e.g., power = voltage * current * power factor).
- Energy accumulation (kWh) based on power registers, with persistence to JSON files.
- A curses-based terminal UI for real-time monitoring of simulations and register values.
- Compatibility with Modbus clients like Modbus Poll for testing.

Key Features:
- Configurable via YAML files specifying register maps and simulation parameters.
- Isolated register configurations per simulation to prevent data crossover.
- Persistent storage of accumulated values (e.g., kWh) in per-config JSON files.
- Robust error handling and logging for diagnostics.
- Terminal UI with navigation to monitor multiple simulations.

Dependencies:
- pymodbus>=3.6.4
- pyyaml>=6.0
- windows-curses>=2.3.1 (Windows only; Linux/macOS use standard curses)

Usage:
1. Place YAML config files in the same directory as this script.
2. Run: `python modbus_tcp_simulator.py`
3. Select a config file, enter IP, port, and slave ID.
4. Use UP/DOWN to navigate simulations, 'a' to add new ones, 'q' to quit.
5. Connect via Modbus Poll to test the server.

License: MIT
Repository: https://github.com/[YourUsername]/ModbusTCPSimulator
"""

import threading
import time
import random
import math
import logging
import yaml
import re
import curses
import asyncio
import json
import os
from typing import Dict, Any, List
from pymodbus.server import ModbusTcpServer
from pymodbus.datastore import ModbusSequentialDataBlock, ModbusSlaveContext, ModbusServerContext
from pymodbus.constants import Endian
from pymodbus.payload import BinaryPayloadBuilder, BinaryPayloadDecoder

# --- Configuration Constants ---
VERSION = "1.0"
UPDATE_INTERVAL_SECONDS = 0.3  # Interval for updating register values
LOG_FILENAME = "simulator.log"  # Log file for diagnostics
PAD_HEIGHT = 100  # Height of curses pad for UI
MIN_TERMINAL_HEIGHT = 20  # Minimum terminal height for UI
MIN_TERMINAL_WIDTH = 80   # Minimum terminal width for UI

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)-8s - %(name)-12s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    filename=LOG_FILENAME,
    filemode="w"
)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.WARNING)
console_handler.setFormatter(logging.Formatter("%(levelname)-8s - %(message)s"))
logging.getLogger().addHandler(console_handler)
log = logging.getLogger(__name__)

# --- Global State ---
simulations: List["SimulationInstance"] = []  # List of active simulation instances
lock = threading.Lock()  # Thread lock for shared state access
persisted_values: Dict[str, Dict[str, float]] = {}  # Persisted register values
selected_simulation_index = 0  # Index of currently selected simulation in UI

# --- YAML Configuration Loader ---
def load_config(file_path: str) -> Dict[str, Any]:
    """
    Load and validate a YAML configuration file for a simulation.

    Args:
        file_path (str): Path to the YAML configuration file.

    Returns:
        Dict[str, Any]: Configuration dictionary with register map and metadata.

    Raises:
        FileNotFoundError: If the config file does not exist.
        ValueError: If the config is invalid or missing required fields.
        yaml.YAMLError: If the YAML parsing fails.
    """
    try:
        with open(file_path, "r") as f:
            config_data = yaml.safe_load(f)
        if not config_data:
            raise ValueError("Configuration file is empty")

        # Validate defaults
        defaults = config_data.get("defaults", {})
        for field in ["ip", "port", "slave_id"]:
            if field not in defaults:
                raise ValueError(f"Missing required default field '{field}'")

        # Initialize register data structures
        register_map = {}
        register_names = {}
        global_variables = {}
        max_address = -1

        # Process registers
        for reg in config_data.get("registers", []):
            required_fields = ["address", "name", "description", "type", "scale"]
            for field in required_fields:
                if field not in reg:
                    raise ValueError(f"Register missing required field '{field}': {reg}")

            if reg["type"] not in ["uint16", "uint32", "int16", "int32", "float32"]:
                raise ValueError(f"Invalid type '{reg['type']}' for {reg['name']}")
            if not isinstance(reg["scale"], (int, float)) or reg["scale"] <= 0:
                raise ValueError(f"Invalid scale '{reg['scale']}' for {reg['name']}")
            if "base_value" in reg and not isinstance(reg["base_value"], (int, float)):
                raise ValueError(f"Invalid base_value '{reg['base_value']}'")
            if "persist" in reg and not isinstance(reg["persist"], bool):
                raise ValueError(f"Invalid persist '{reg['persist']}' for {reg['name']}")
            if reg.get("persist", False) and not reg.get("accumulate", False):
                raise ValueError(f"Persist requires accumulate for {reg['name']}")

            address = reg["address"]
            if address in register_map:
                raise ValueError(f"Duplicate address {address}: {reg['name']}")
            num_regs = 2 if reg["type"] in ["uint32", "int32", "float32"] else 1
            max_address = max(max_address, address + num_regs - 1)

            # Handle writable registers
            if reg.get("writable", False):
                if "variable_name" not in reg:
                    raise ValueError(f"Writable register '{reg['name']}' requires 'variable_name'")
                var_name = reg["variable_name"]
                if not isinstance(var_name, str) or not var_name:
                    raise ValueError(f"Invalid variable_name for {reg['name']}")
                if var_name in global_variables:
                    log.warning(f"Redefining global variable '{var_name}' for {reg['name']}")
                global_variables[var_name] = reg.get("base_value", 0)
                if "min_value" in reg and not isinstance(reg["min_value"], (int, float)):
                    raise ValueError(f"Invalid min_value '{reg['min_value']}'")
                if "max_value" in reg and not isinstance(reg["max_value"], (int, float)):
                    raise ValueError(f"Invalid max_value '{reg['max_value']}'")
                if "min_value" in reg and "max_value" in reg and reg["min_value"] > reg["max_value"]:
                    raise ValueError(f"min_value > max_value for {reg['name']}")

            # Validate expressions
            if "expression" in reg:
                if "," in reg["expression"]:
                    log.warning(f"Comma in expression for {reg['name']}: verify compatibility")
                if "math." in reg["expression"]:
                    log.warning(f"'math.' in expression for {reg['name']}: ensure correct usage")

            register_map[address] = reg
            register_names[reg["name"]] = address

        if not register_map:
            raise ValueError("No registers defined in config")

        # Store metadata
        config_data["_max_address_needed"] = max(10, max_address + 1)
        config_data["_register_map"] = register_map
        config_data["_register_names"] = register_names
        config_data["_global_variables"] = global_variables
        log.info(f"Loaded config from {file_path}. Max address: {config_data['_max_address_needed']}")
        return config_data
    except FileNotFoundError:
        log.error(f"Config file not found: {file_path}")
        raise
    except yaml.YAMLError as e:
        log.error(f"YAML parsing error in {file_path}: {e}")
        raise
    except Exception as e:
        log.error(f"Failed to load config {file_path}: {e}")
        raise

# --- Persistence Functions ---
def load_persisted_values(file_path: str) -> Dict[str, Dict[str, float]]:
    """
    Load persisted register values from a JSON file.

    Args:
        file_path (str): Path to the JSON file.

    Returns:
        Dict[str, Dict[str, float]]: Persisted values, keyed by simulation ID.

    Raises:
        ValueError: If the JSON file is invalid.
        Exception: For other file access or parsing errors.
    """
    try:
        if os.path.exists(file_path):
            with open(file_path, "r") as f:
                data = json.load(f)
            if not isinstance(data, dict):
                raise ValueError("Persisted values file is not a dictionary")
            for sim_id, regs in data.items():
                if not isinstance(regs, dict):
                    raise ValueError(f"Invalid register data for {sim_id}")
                for reg_name, value in regs.items():
                    if not isinstance(value, (int, float)):
                        raise ValueError(f"Invalid value '{value}' for {reg_name} in {sim_id}")
            log.info(f"Loaded persisted values from {file_path}")
            return data
        log.info(f"No persisted values found at {file_path}")
        return {}
    except Exception as e:
        log.error(f"Failed to load persisted values from {file_path}: {e}")
        return {}

def save_persisted_values(data: Dict[str, Dict[str, float]], file_path: str):
    """
    Save persisted register values to a JSON file.

    Args:
        data (Dict[str, Dict[str, float]]): Persisted values to save.
        file_path (str): Path to the JSON file.
    """
    try:
        with open(file_path, "w") as f:
            json.dump(data, f, indent=2)
        log.info(f"Saved persisted values to {file_path}")
    except Exception as e:
        log.error(f"Failed to save persisted values to {file_path}: {e}")

# --- Expression Evaluator ---
def evaluate_expression(expression: str, values: Dict[str, float], global_vars: Dict[str, float]) -> float:
    """
    Evaluate a mathematical expression using register values and global variables.

    Args:
        expression (str): Expression to evaluate (e.g., "voltage * current").
        values (Dict[str, float]): Current register values.
        global_vars (Dict[str, float]): Global variables for writable registers.

    Returns:
        float: Result of the expression, or 0.0 on error.
    """
    try:
        combined_values = {**values, **global_vars}
        used_names = {name for name in combined_values if re.search(r"\b" + re.escape(name) + r"\b", expression)}
        filtered_combined = {k: combined_values[k] for k in used_names}
        substituted_expression = expression
        for name in used_names:
            pattern = r"\b" + re.escape(name) + r"\b"
            substituted_expression = re.sub(pattern, str(filtered_combined.get(name, 0)), substituted_expression)
        allowed_globals = {"math": math, "max": max, "min": min, "__builtins__": {}}
        result = eval(substituted_expression, allowed_globals, {})
        return float(result)
    except Exception as e:
        log.error(f"Expression evaluation failed: '{expression}' (Substituted: '{substituted_expression}'): {e}")
        return 0.0

# --- Value Encoding/Decoding ---
def encode_value(value: float, reg_type: str, scale: float) -> List[int]:
    """
    Encode a scaled value into Modbus register(s).

    Args:
        value (float): Value to encode.
        reg_type (str): Register type (uint16, uint32, int16, int32, float32).
        scale (float): Scaling factor.

    Returns:
        List[int]: Encoded register values.
    """
    try:
        scaled_value = float(value) * float(scale)
    except (ValueError, TypeError):
        log.warning(f"Invalid value '{value}' for encoding, using 0")
        scaled_value = 0.0
    builder = BinaryPayloadBuilder(byteorder=Endian.BIG, wordorder=Endian.BIG)
    try:
        if reg_type == "uint16":
            builder.add_16bit_uint(max(0, min(int(round(scaled_value)), 65535)))
        elif reg_type == "uint32":
            builder.add_32bit_uint(max(0, min(int(round(scaled_value)), 0xFFFFFFFF)))
        elif reg_type == "int16":
            builder.add_16bit_int(max(-32768, min(int(round(scaled_value)), 32767)))
        elif reg_type == "int32":
            builder.add_32bit_int(max(-0x80000000, min(int(round(scaled_value)), 0x7FFFFFFF)))
        elif reg_type == "float32":
            builder.add_32bit_float(scaled_value)
        else:
            log.warning(f"Unknown type '{reg_type}' during encoding")
            return [0]
        return builder.to_registers()
    except Exception as e:
        log.error(f"Error encoding {value} (scaled: {scaled_value}) as {reg_type}: {e}")
        return [0] * (2 if reg_type in ["uint32", "int32", "float32"] else 1)

def decode_value(words: List[int], reg_type: str, scale: float) -> float:
    """
    Decode Modbus register(s) into a scaled value.

    Args:
        words (List[int]): Raw register values.
        reg_type (str): Register type.
        scale (float): Scaling factor.

    Returns:
        float: Decoded and scaled value, or 0.0 on error.
    """
    if not words:
        return 0.0
    if scale == 0:
        log.warning(f"Zero scale for decoding {reg_type}, using 1.0")
        scale = 1.0
    try:
        decoder = BinaryPayloadDecoder.fromRegisters(words, byteorder=Endian.BIG, wordorder=Endian.BIG)
        if reg_type == "uint16":
            decoded_raw = decoder.decode_16bit_uint()
        elif reg_type == "uint32":
            if len(words) >= 2:
                decoded_raw = decoder.decode_32bit_uint()
            else:
                return 0.0
        elif reg_type == "int16":
            decoded_raw = decoder.decode_16bit_int()
        elif reg_type == "int32":
            if len(words) >= 2:
                decoded_raw = decoder.decode_32bit_int()
            else:
                return 0.0
        elif reg_type == "float32":
            if len(words) >= 2:
                decoded_raw = decoder.decode_32bit_float()
            else:
                return 0.0
        else:
            log.warning(f"Unknown type '{reg_type}' during decoding")
            return 0.0
        return float(decoded_raw) / float(scale)
    except Exception as e:
        log.error(f"Error decoding {words} as {reg_type} with scale {scale}: {e}")
        return 0.0

# --- Simulation Instance ---
class SimulationInstance:
    """Manages a single Modbus TCP simulation with its own server and register configuration."""

    def __init__(self, ip: str, port: int, slave_id: int, max_registers: int, config_file: str):
        """
        Initialize a simulation instance.

        Args:
            ip (str): IP address for the Modbus server.
            port (int): Port for the Modbus server.
            slave_id (int): Modbus slave ID.
            max_registers (int): Initial maximum number of registers.
            config_file (str): Path to the YAML configuration file.
        """
        self.log = logging.getLogger(f"Sim-{slave_id}@{ip}:{port}")
        self.ip = ip
        self.port = port
        self.slave_id = slave_id
        self.config_file = config_file
        self.persist_file = f"persisted_values-{os.path.splitext(os.path.basename(config_file))[0]}.json"
        self.running = False
        self.thread_server = None
        self.thread_update = None
        self.lock = threading.Lock()
        self.values: Dict[str, float] = {}

        # Load configuration
        self.config = load_config(config_file)
        self.register_map = self.config["_register_map"]
        self.register_names = self.config["_register_names"]
        self.global_variables = self.config["_global_variables"]
        self.max_registers = max(max_registers, self.config["_max_address_needed"])

        # Initialize Modbus data block
        self.block = ModbusSequentialDataBlock(0, [0] * self.max_registers)
        self.context = ModbusSlaveContext(
            hr=self.block,
            ir=self.block,
            di=ModbusSequentialDataBlock(0, [0] * self.max_registers),
            co=ModbusSequentialDataBlock(0, [0] * self.max_registers),
            zero_mode=True
        )
        self.server_context = ModbusServerContext(slaves={self.slave_id: self.context}, single=False)
        self.modbus_server = None

        # Initialize register values
        sim_id = f"{ip}:{port}:{slave_id}"
        persisted_data = load_persisted_values(self.persist_file)
        sim_persisted = persisted_data.get(sim_id, {})
        for address, reg in self.register_map.items():
            reg_name = reg["name"]
            initial_value = float(sim_persisted.get(reg_name, reg.get("base_value", 0)))
            if reg.get("persist", False) and reg_name in sim_persisted:
                self.log.info(f"Restored persisted value for {reg_name}: {initial_value}")
            self.values[reg_name] = initial_value
            if reg.get("writable", False):
                self.global_variables[reg["variable_name"]] = initial_value
            words = encode_value(initial_value, reg["type"], reg["scale"])
            if address + len(words) <= self.max_registers:
                self.block.setValues(address, words)
                self.log.debug(f"Initialized {reg_name} (Addr:{address}) to {initial_value} (Raw:{words})")
            else:
                self.log.error(f"Initialization failed for {reg_name} (Addr:{address}): Out of bounds")
        self.log.info(f"Initialized with {self.max_registers} registers")

    def get_register_info(self, address: int) -> Dict[str, Any]:
        """
        Get information for a register at the given address.

        Args:
            address (int): Register address.

        Returns:
            Dict[str, Any]: Register configuration, or default for unknown address.
        """
        return self.register_map.get(
            address,
            {"address": address, "name": f"Unknown_{address}", "description": "Unknown", "type": "uint16", "scale": 1.0}
        ).copy()

    def save_persisted_values(self):
        """Save persistent register values to the JSON file."""
        with self.lock:
            sim_id = f"{self.ip}:{self.port}:{self.slave_id}"
            persistent_regs = {
                reg["name"]: self.values[reg["name"]]
                for _, reg in self.register_map.items()
                if reg.get("persist", False)
            }
            if persistent_regs:
                global persisted_values
                persisted_values[sim_id] = persistent_regs
                save_persisted_values(persisted_values, self.persist_file)
                self.log.debug(f"Saved persisted values for {sim_id}: {persistent_regs}")

    def _update_values(self):
        """Update register values (randomization, accumulation, writable, expressions)."""
        with self.lock:
            current_values = self.values.copy()

            # 1. Randomize non-writable registers
            for address, reg in self.register_map.items():
                if reg.get("randomize", False) and not reg.get("writable", False):
                    base_value = float(reg.get("base_value", 0))
                    fluctuation = max(0.0, min(1.0, float(reg.get("fluctuation", 0))))
                    new_value = base_value * (1 + random.uniform(-fluctuation, fluctuation))
                    self.values[reg["name"]] = new_value
                    self.log.debug(f"Randomized {reg['name']} (Addr:{address}) to {new_value:.3f}")

            # 2. Accumulate values (e.g., kWh)
            for address, reg in self.register_map.items():
                if reg.get("accumulate", False) and not reg.get("writable", False):
                    source_name = reg.get("source")
                    if source_name and source_name in self.values:
                        source_address = self.register_names.get(source_name)
                        source_reg = self.register_map.get(source_address, {})
                        source_scale = source_reg.get("scale", 1.0)
                        dest_scale = reg.get("scale", 1.0)
                        source_value = float(current_values.get(source_name, 0))
                        kWh_increment = (source_value / source_scale) * UPDATE_INTERVAL_SECONDS / 3600.0 * dest_scale
                        self.values[reg["name"]] += kWh_increment
                        self.log.debug(
                            f"Accumulated {kWh_increment:.6f} kWh for {reg['name']} (Addr:{address}), "
                            f"total: {self.values[reg['name']]:.3f}"
                        )
                    else:
                        self.log.warning(f"Accumulation failed for {reg['name']}: Source {source_name} not found")

            # 3. Update writable registers from Modbus
            for address, reg in self.register_map.items():
                if reg.get("writable", False):
                    num_registers = 2 if reg["type"] in ["uint32", "int32", "float32"] else 1
                    try:
                        words = self.context.getValues(3, address, count=num_registers)
                        if words:
                            new_value = decode_value(words, reg["type"], reg["scale"])
                            min_val, max_val = reg.get("min_value"), reg.get("max_value")
                            constrained_value = new_value
                            value_changed = False
                            if min_val is not None and constrained_value < min_val:
                                constrained_value = min_val
                                value_changed = True
                            if max_val is not None and constrained_value > max_val:
                                constrained_value = max_val
                                value_changed = True
                            if value_changed:
                                constrained_words = encode_value(constrained_value, reg["type"], reg["scale"])
                                self.context.setValues(3, address, constrained_words)
                                self.log.debug(f"Constrained {reg['name']} to {constrained_value}")
                            if abs(new_value - self.values.get(reg["name"], float("inf"))) > 1e-6:
                                self.values[reg["name"]] = new_value
                                self.global_variables[reg["variable_name"]] = new_value
                                self.log.debug(
                                    f"Updated writable {reg['name']} (Addr:{address}) from Modbus: "
                                    f"{new_value:.3f} (Raw:{words})"
                                )
                    except Exception as e:
                        self.log.error(f"Error reading writable {reg['name']} (Addr:{address}): {e}")

            # 4. Evaluate expressions
            current_eval_values = self.values.copy()
            current_global_vars = self.global_variables.copy()
            for address, reg in self.register_map.items():
                if "expression" in reg and not reg.get("writable", False):
                    try:
                        new_value = evaluate_expression(reg["expression"], current_eval_values, current_global_vars)
                        self.values[reg["name"]] = new_value
                        self.log.debug(f"Evaluated {reg['name']} (Addr:{address}) to {new_value:.3f}")
                    except Exception as e:
                        self.log.error(f"Expression evaluation failed for {reg['name']} (Addr:{address}): {e}")

            # 5. Write non-writable registers to Modbus
            for address, reg in self.register_map.items():
                if not reg.get("writable", False):
                    value = self.values.get(reg["name"], 0)
                    words = encode_value(value, reg["type"], reg["scale"])
                    try:
                        if address + len(words) <= self.max_registers:
                            self.block.setValues(address, words)
                            self.log.debug(f"Wrote {reg['name']} (Addr:{address}) to Modbus: {value:.3f} (Raw:{words})")
                        else:
                            self.log.error(f"Write failed for {reg['name']} (Addr:{address}): Out of bounds")
                    except Exception as e:
                        self.log.error(f"Error writing {reg['name']} (Addr:{address}): {e}")

    def _update_loop(self):
        """Periodically update register values in a background thread."""
        self.log.info("Started update loop")
        while self.running:
            start_time = time.monotonic()
            try:
                self._update_values()
            except Exception as e:
                self.log.exception(f"Critical error in update loop for {self.slave_id}@{self.ip}:{self.port}")
                time.sleep(UPDATE_INTERVAL_SECONDS * 5)
            elapsed = time.monotonic() - start_time
            time.sleep(max(0, UPDATE_INTERVAL_SECONDS - elapsed))
        self.log.info("Stopped update loop")

    def _run_server(self):
        """Run the Modbus TCP server in a background thread."""
        server_id = f"Sim-{self.slave_id}@{self.ip}:{self.port}"
        address = (self.ip, self.port)
        async def serve():
            try:
                self.log.info(f"{server_id} - Starting Modbus TCP server")
                self.modbus_server = ModbusTcpServer(context=self.server_context, address=address)
                self.log.info(f"{server_id} - Modbus TCP server running")
                await self.modbus_server.serve_forever()
            except asyncio.CancelledError:
                self.log.info(f"{server_id} - Server task cancelled")
            except OSError as e:
                if e.errno in [98, 48, 10048]:
                    self.log.error(f"{server_id} - Address {self.ip}:{self.port} already in use")
                else:
                    self.log.exception(f"{server_id} - Critical error in Modbus TCP server")
                self.running = False
            finally:
                self.log.info(f"{server_id} - Modbus TCP server stopped")
                self.running = False

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        server_task = loop.create_task(serve())
        try:
            while self.running and not server_task.done():
                loop.run_until_complete(asyncio.sleep(0.1))
            if not self.running and not server_task.done():
                self.log.info(f"{server_id} - Cancelling server task")
                server_task.cancel()
                loop.run_until_complete(server_task)
        except Exception as e:
            self.log.error(f"{server_id} - Error in asyncio loop: {e}")
        finally:
            if not loop.is_closed():
                for task in asyncio.all_tasks(loop):
                    if not task.done():
                        task.cancel()
                try:
                    loop.run_until_complete(asyncio.gather(*asyncio.all_tasks(loop), return_exceptions=True))
                except RuntimeError as e:
                    if "cannot schedule new futures after shutdown" in str(e):
                        log.warning(f"{server_id} - Loop closing during cleanup")
                    else:
                        raise
                loop.close()
            self.log.info(f"{server_id} - Event loop closed")

    def start(self) -> bool:
        """
        Start the simulation's server and update threads.

        Returns:
            bool: True if started successfully, False otherwise.
        """
        if self.running:
            return False
        self.running = True
        self.log.info("Starting simulation")
        self.thread_update = threading.Thread(
            target=self._update_loop, name=f"Update-{self.slave_id}", daemon=True
        )
        self.thread_server = threading.Thread(
            target=self._run_server, name=f"Server-{self.slave_id}", daemon=True
        )
        self.thread_update.start()
        self.thread_server.start()
        time.sleep(0.2)
        if not self.running:
            self.log.error(f"Failed to start Modbus server {self.slave_id}@{self.ip}:{self.port}")
            return False
        if not self.thread_update.is_alive():
            self.log.error("Update thread failed")
            self.stop()
            return False
        self.log.info(f"Simulation {self.slave_id}@{self.ip}:{self.port} started")
        return True

    def stop(self):
        """Stop the simulation and save persisted values."""
        if not self.running:
            return
        self.log.info(f"Stopping simulation {self.slave_id}@{self.ip}:{self.port}")
        self.running = False
        if self.modbus_server:
            try:
                self.modbus_server.shutdown()
                self.log.debug("Shutdown called on Modbus server")
            except Exception as e:
                self.log.warning(f"Error during server shutdown: {e}")
        if self.thread_server and self.thread_server.is_alive():
            self.thread_server.join(timeout=2)
        if self.thread_update and self.thread_update.is_alive():
            self.thread_update.join(timeout=1)
        if self.thread_server and self.thread_server.is_alive():
            self.log.warning("Server thread did not stop")
        if self.thread_update and self.thread_update.is_alive():
            self.log.warning("Update thread did not stop")
        self.save_persisted_values()
        self.log.info("Simulation stopped")

    def is_alive(self) -> bool:
        """
        Check if the simulation is running.

        Returns:
            bool: True if the server thread is alive, False otherwise.
        """
        return self.running and self.thread_server and self.thread_server.is_alive()

# --- UI Display Functions ---
def display_simulation_status(pad, simulations: List["SimulationInstance"], selected_index: int, width: int):
    """
    Display the status of all simulations in the curses pad.

    Args:
        pad: Curses pad for rendering.
        simulations: List of simulation instances.
        selected_index: Index of the currently selected simulation.
        width: Width of the pad.
    """
    try:
        pad.addstr(0, 0, f"ModbusTCPSimulator (v{VERSION})".ljust(width), curses.A_BOLD | curses.color_pair(1))
        pad.clrtoeol()
        pad.addstr(1, 0, "Press UP/DOWN to select | 'a' to add | 'q' to quit".ljust(width))
        pad.clrtoeol()
        pad.addstr(3, 0, "Running Simulations:".ljust(width))
        pad.clrtoeol()
        start_line = 4
        if not simulations:
            pad.addstr(start_line, 0, "  No simulations running".ljust(width))
            pad.clrtoeol()
        else:
            for idx, sim in enumerate(simulations):
                line_y = start_line + idx
                if line_y >= PAD_HEIGHT:
                    break
                status = "Running" if sim.is_alive() else "Stopped"
                config_name = os.path.basename(sim.config_file)
                display_str = f"Sim #{idx + 1}: {sim.ip}:{sim.port} (ID:{sim.slave_id}) Config: {config_name} Status: {status}"
                display_str = display_str[: width - 2]
                pad.move(line_y, 0)
                pad.clrtoeol()
                pad.addstr(line_y, 0, f"> {display_str}" if idx == selected_index else f"  {display_str}")
        for y in range(start_line + len(simulations), start_line + 10):
            if y < PAD_HEIGHT:
                pad.move(y, 0)
                pad.clrtoeol()
    except curses.error as e:
        log.error(f"Curses error in simulation status display: {e}")

def display_registers(pad, sim_instance: "SimulationInstance", width: int):
    """
    Display register values for the selected simulation.

    Args:
        pad: Curses pad for rendering.
        sim_instance: Simulation instance to display, or None if none selected.
        width: Width of the pad.
    """
    try:
        reg_start_y = 10
        pad.move(reg_start_y, 0)
        pad.clrtoeol()
        pad.addstr(reg_start_y, 0, "Registers (Selected Simulation):".ljust(width), curses.A_UNDERLINE)
        if not sim_instance or not sim_instance.is_alive():
            pad.addstr(reg_start_y + 1, 0, "  No simulation selected or running".ljust(width))
            for y in range(reg_start_y + 2, PAD_HEIGHT):
                pad.move(y, 0)
                pad.clrtoeol()
            return

        sorted_addresses = sorted(sim_instance.register_map.keys())
        with sim_instance.lock:
            display_values = sim_instance.values.copy()

        line_num = 0
        for address in sorted_addresses:
            line_y = reg_start_y + 1 + line_num
            if line_y >= PAD_HEIGHT:
                pad.move(line_y, 0)
                pad.clrtoeol()
                pad.addstr(line_y, 0, "... (more registers)".ljust(width), curses.A_BOLD)
                break

            pad.move(line_y, 0)
            pad.clrtoeol()
            reg_info = sim_instance.get_register_info(address)
            reg_name = reg_info.get("name", f"Unk_{address}")
            scaled_value = display_values.get(reg_name, 0.0)
            scaled_str = f"{scaled_value:.3f}" if isinstance(scaled_value, float) else str(scaled_value)
            num_regs = 2 if reg_info.get("type") in ["uint32", "int32", "float32"] else 1
            try:
                with sim_instance.lock:
                    raw_values = (
                        sim_instance.context.getValues(3, address, count=num_regs)
                        if address + num_regs <= sim_instance.max_registers
                        else ["OOB"]
                    )
            except Exception:
                raw_values = ["ERR"]
            raw_str = str(raw_values)
            writable_str = "[W]" if reg_info.get("writable", False) else ""
            persist_str = "[P]" if reg_info.get("persist", False) else ""
            display_line = (
                f" {address:<5} {writable_str:<3}{persist_str:<3} {reg_name:<20.20} "
                f"Scaled: {scaled_str:<12} Raw: {raw_str:<15.15} # {reg_info.get('description', '')}"
            )[:width]
            pad.addstr(line_y, 0, display_line, curses.A_BOLD)
            line_num += 1

        for y in range(reg_start_y + 1 + line_num, PAD_HEIGHT):
            pad.move(y, 0)
            pad.clrtoeol()
    except curses.error as e:
        log.error(f"Curses error in register display: {e}")
    except Exception as e:
        log.exception(f"Error displaying registers for {sim_instance.ip if sim_instance else 'None'}: {e}")

# --- User Configuration Input ---
def get_user_config(stdscr, sim_index: int, defaults: Dict[str, Any]) -> tuple[str, int, int, str]:
    """
    Prompt the user for simulation configuration via curses UI.

    Args:
        stdscr: Curses standard screen object.
        sim_index: Index of the simulation being configured.
        defaults: Default values for IP, port, and slave ID.

    Returns:
        tuple[str, int, int, str]: IP address, port, slave ID, and config file path.

    Raises:
        ValueError: If terminal is too small or no YAML files are found.
    """
    curses.echo()
    stdscr.clear()
    h, w = stdscr.getmaxyx()
    if h < MIN_TERMINAL_HEIGHT or w < MIN_TERMINAL_WIDTH:
        raise ValueError(f"Terminal too small: {h}x{w}. Minimum: {MIN_TERMINAL_HEIGHT}x{MIN_TERMINAL_WIDTH}")

    try:
        stdscr.addstr(0, 0, f"--- Configure Simulation #{sim_index + 1} ---")
    except curses.error as e:
        log.error(f"Curses error in config header: {e}")
        raise

    yaml_files = [f for f in os.listdir(os.path.dirname(os.path.abspath(__file__))) if f.endswith(".yaml")]
    if not yaml_files:
        raise ValueError("No YAML configuration files found in script directory")

    def display_yaml_menu(y: int):
        try:
            stdscr.addstr(y, 0, "Available Config Files:")
            for i, f in enumerate(yaml_files, 1):
                if y + i < h:
                    stdscr.addstr(y + i, 0, f"  {i}. {f}")
            stdscr.addstr(y + len(yaml_files) + 1, 0, f"Enter number (1-{len(yaml_files)}): ")
            stdscr.clrtoeol()
            stdscr.refresh()
        except curses.error as e:
            log.error(f"Curses error in YAML menu: {e}")
            raise

    def get_yaml_selection(y: int) -> str:
        while True:
            if y + len(yaml_files) + 2 >= h:
                log.error("Terminal too small for YAML selection")
                return yaml_files[0] if yaml_files else ""
            display_yaml_menu(y)
            try:
                s = stdscr.getstr(y + len(yaml_files) + 1, len(f"Enter number (1-{len(yaml_files)}): ")).decode("utf-8").strip()
                num = int(s)
                if 1 <= num <= len(yaml_files):
                    return yaml_files[num - 1]
                stdscr.addstr(y + len(yaml_files) + 2, 0, f"Invalid number. Choose 1-{len(yaml_files)}.")
                stdscr.clrtoeol()
                stdscr.refresh()
                time.sleep(0.8)
                stdscr.addstr(y + len(yaml_files) + 2, 0, "")
                stdscr.clrtoeol()
            except (ValueError, curses.error) as e:
                log.warning(f"Error in YAML selection: {e}")
                stdscr.addstr(y + len(yaml_files) + 2, 0, "Invalid input. Try again.")
                stdscr.clrtoeol()
                stdscr.refresh()
                time.sleep(0.8)
                stdscr.addstr(y + len(yaml_files) + 2, 0, "")
                stdscr.clrtoeol()

    def get_string_input(prompt: str, y: int, default: str) -> str:
        if y >= h - 1:
            log.warning(f"Terminal too small for input at y={y}")
            return default
        prompt_str = f"{prompt} [{default}]: "
        try:
            stdscr.addstr(y, 0, prompt_str)
            stdscr.clrtoeol()
            stdscr.refresh()
            max_len = max(0, w - len(prompt_str) - 1)
            s = stdscr.getstr(y, len(prompt_str), max_len).decode("utf-8").strip()
            return s if s else default
        except curses.error as e:
            log.warning(f"Curses error in string input at y={y}: {e}")
            return default

    def get_int_input(prompt: str, y: int, default: int) -> int:
        while True:
            if y >= h - 2:
                log.warning(f"Terminal too small for int input at y={y}")
                return default
            s = get_string_input(prompt, y, str(default))
            try:
                return int(s)
            except ValueError:
                try:
                    stdscr.addstr(y + 1, 0, "Invalid input. Please enter an integer.")
                    stdscr.clrtoeol()
                    stdscr.refresh()
                    time.sleep(0.8)
                    stdscr.addstr(y + 1, 0, "")
                    stdscr.clrtoeol()
                except curses.error:
                    pass

    prompt_y = 2
    config_file = get_yaml_selection(prompt_y)
    prompt_y += len(yaml_files) + 3
    ip = get_string_input("Enter IP Address", prompt_y, defaults.get("ip", "127.0.0.1"))
    prompt_y += 2
    port = get_int_input("Enter Port", prompt_y, defaults.get("port", 502))
    prompt_y += 2
    slave_id = get_int_input("Enter Slave ID", prompt_y, defaults.get("slave_id", 1))

    curses.noecho()
    stdscr.clear()
    stdscr.refresh()
    return ip, port, slave_id, config_file

# --- Main Application ---
def main(stdscr):
    """
    Main function to run the simulator with a curses-based UI.

    Args:
        stdscr: Curses standard screen object.
    """
    global selected_simulation_index, simulations

    curses.curs_set(0)
    stdscr.nodelay(True)
    stdscr.clear()
    if curses.has_colors():
        try:
            curses.start_color()
            curses.use_default_colors()
            curses.init_pair(1, curses.COLOR_BLACK, curses.COLOR_WHITE)
        except curses.error as e:
            log.error(f"Curses error initializing colors: {e}")

    scr_h, scr_w = stdscr.getmaxyx()
    if scr_h < MIN_TERMINAL_HEIGHT or scr_w < MIN_TERMINAL_WIDTH:
        log.error(f"Terminal too small: {scr_h}x{scr_w}. Minimum: {MIN_TERMINAL_HEIGHT}x{MIN_TERMINAL_WIDTH}")
        curses.endwin()
        print(f"FATAL ERROR: Terminal too small ({scr_h}x{scr_w}). Resize to {MIN_TERMINAL_HEIGHT}x{MIN_TERMINAL_WIDTH}.")
        return

    pad = curses.newpad(PAD_HEIGHT, scr_w)
    pad.keypad(True)

    simulations_configured = False
    last_defaults = {}
    while not simulations_configured:
        sim_count = len(simulations)
        try:
            ip, port, slave_id, config_file = get_user_config(stdscr, sim_count, last_defaults)
            if any(s.ip == ip and s.port == port for s in simulations):
                raise ValueError(f"IP:Port {ip}:{port} already in use")
            config = load_config(config_file)
            sim_instance = SimulationInstance(ip, port, slave_id, config["_max_address_needed"], config_file)
            last_defaults = config.get("defaults", {})
            if sim_instance.start():
                log.info(f"Started simulation {slave_id}@{ip}:{port} with {config_file}")
                with lock:
                    simulations.append(sim_instance)
                    selected_simulation_index = len(simulations) - 1
            else:
                try:
                    stdscr.clear()
                    stdscr.addstr(0, 0, f"ERROR: Failed to start {ip}:{port}. Check logs. Press any key...")
                    stdscr.refresh()
                    stdscr.nodelay(False)
                    stdscr.getch()
                    stdscr.nodelay(True)
                except curses.error as e:
                    log.error(f"Curses error in start failure message: {e}")
        except Exception as e:
            log.error(f"Configuration error: {e}")
            try:
                stdscr.clear()
                stdscr.addstr(0, 0, f"Config Error: {e}")
                stdscr.addstr(2, 0, "Retry (r) or Skip (any)?")
                stdscr.refresh()
                stdscr.nodelay(False)
                key = stdscr.getch()
                stdscr.nodelay(True)
                stdscr.clear()
                if key != ord("r"):
                    if not simulations:
                        stdscr.addstr(0, 0, "No simulations configured. Exiting.")
                        stdscr.refresh()
                        time.sleep(1.5)
                        return
                    simulations_configured = True
                continue
            except curses.error as e:
                log.error(f"Curses error handling config error: {e}")
                if not simulations:
                    curses.endwin()
                    print(f"FATAL ERROR: {e}\nCheck simulator.log.")
                    return
                simulations_configured = True

        try:
            stdscr.clear()
            stdscr.addstr(0, 0, f"Sim #{len(simulations)} ({ip}:{port}) added with {config_file}")
            stdscr.addstr(2, 0, "Add another? (y/N): ")
            stdscr.refresh()
            stdscr.nodelay(False)
            curses.echo()
            key = stdscr.getch()
            curses.noecho()
            stdscr.nodelay(True)
            stdscr.clear()
            add_more = chr(key).lower() if key != curses.ERR and key >= 0 else "n"
            if add_more != "y":
                simulations_configured = True
        except curses.error as e:
            log.error(f"Curses error in add prompt: {e}")
            simulations_configured = True

    if not simulations:
        log.warning("No simulations configured")
        try:
            stdscr.clear()
            stdscr.addstr(0, 0, "No simulations configured. Exiting.")
            stdscr.refresh()
            time.sleep(1.5)
        except curses.error as e:
            log.error(f"Curses error in no simulations message: {e}")
        return

    last_scr_h, last_scr_w = -1, -1
    try:
        while True:
            scr_h, scr_w = stdscr.getmaxyx()
            if scr_h < MIN_TERMINAL_HEIGHT or scr_w < MIN_TERMINAL_WIDTH:
                log.warning(f"Terminal too small: {scr_h}x{scr_w}")
                continue
            if scr_h != last_scr_h or scr_w != last_scr_w:
                log.info(f"Terminal resized to {scr_h}x{scr_w}")
                try:
                    pad = curses.newpad(PAD_HEIGHT, scr_w)
                    pad.keypad(True)
                    stdscr.clear()
                    stdscr.refresh()
                    last_scr_h, last_scr_w = scr_h, scr_w
                except curses.error as e:
                    log.error(f"Error recreating pad: {e}")
                    last_scr_h, last_scr_w = -1, -1

            key = stdscr.getch()
            if key == curses.KEY_UP:
                if simulations:
                    selected_simulation_index = (selected_simulation_index - 1) % len(simulations)
            elif key == curses.KEY_DOWN:
                if simulations:
                    selected_simulation_index = (selected_simulation_index + 1) % len(simulations)
            elif key in [ord("a"), ord("A")]:
                try:
                    stdscr.clear()
                    stdscr.addstr(0, 0, "--- Adding New Simulation ---")
                    stdscr.refresh()
                    sim_count = len(simulations)
                    ip, port, slave_id, config_file = get_user_config(stdscr, sim_count, last_defaults)
                    if any(s.ip == ip and s.port == port for s in simulations):
                        raise ValueError(f"IP:Port {ip}:{port} already in use")
                    config = load_config(config_file)
                    sim_instance = SimulationInstance(ip, port, slave_id, config["_max_address_needed"], config_file)
                    last_defaults = config.get("defaults", {})
                    if sim_instance.start():
                        log.info(f"Added simulation {slave_id}@{ip}:{port} with {config_file}")
                        with lock:
                            simulations.append(sim_instance)
                            selected_simulation_index = len(simulations) - 1
                        stdscr.addstr(5, 0, "Simulation added. Press any key...")
                    else:
                        stdscr.addstr(5, 0, f"Failed to start {ip}:{port}. Check logs. Press any key...")
                    stdscr.nodelay(False)
                    stdscr.getch()
                    stdscr.nodelay(True)
                    stdscr.clear()
                    stdscr.refresh()
                except Exception as e:
                    log.error(f"Failed to add simulation: {e}")
                    try:
                        stdscr.addstr(5, 0, f"Failed: {e}. Press any key...")
                        stdscr.nodelay(False)
                        stdscr.getch()
                        stdscr.nodelay(True)
                        stdscr.clear()
                        stdscr.refresh()
                    except curses.error as e:
                        log.error(f"Curses error in add failure message: {e}")
            elif key in [ord("q"), ord("Q")]:
                log.info("Quitting application")
                break
            elif key == curses.KEY_RESIZE:
                log.debug("Terminal resize event")
                continue

            pad.erase()
            if not simulations:
                selected_simulation_index = 0
            elif selected_simulation_index >= len(simulations):
                selected_simulation_index = len(simulations) - 1
            display_simulation_status(pad, simulations, selected_simulation_index, scr_w)
            sim_instance = simulations[selected_simulation_index] if simulations and 0 <= selected_simulation_index < len(simulations) else None
            display_registers(pad, sim_instance, scr_w)

            try:
                pad.refresh(0, 0, 0, 0, scr_h - 1, scr_w - 1)
            except curses.error as e:
                log.error(f"Pad refresh error: {e}. Screen: {scr_h}x{scr_w}, Pad: {PAD_HEIGHT}x{scr_w}")
                last_scr_h, last_scr_w = -1, -1

            with lock:
                active_simulations = [sim for sim in simulations if sim.is_alive()]
                if len(active_simulations) < len(simulations):
                    log.warning("Removing inactive simulations")
                    simulations[:] = active_simulations
                    if selected_simulation_index >= len(simulations):
                        selected_simulation_index = max(0, len(simulations) - 1)
                    pad.erase()

            time.sleep(0.1)

    except KeyboardInterrupt:
        log.info("Interrupted by user")
    except Exception as e:
        log.exception("Fatal error in main loop")
    finally:
        log.info("Shutting down")
        try:
            stdscr.nodelay(False)
            with lock:
                for sim in simulations[:]:
                    log.debug(f"Stopping simulation {sim.slave_id}@{sim.ip}:{sim.port}")
                    sim.stop()
            log.info("All simulations stopped")
        except curses.error as e:
            log.error(f"Curses error during shutdown: {e}")

if __name__ == "__main__":
    try:
        curses.wrapper(main)
        print("ModbusTCPSimulator finished normally.")
    except Exception as e:
        logging.exception("Unhandled exception")
        print(f"CRITICAL ERROR: {e}\nCheck simulator.log.")
