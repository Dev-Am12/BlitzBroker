#!/usr/bin/env python3
"""
tests/interop/paho_client.py

BlitzBroker interop verification -- paho-mqtt Python client round-trip.

PURPOSE
-------
Developer-run only. Starts the real blitzbroker binary as a subprocess,
uses the real paho-mqtt Python package to publish and receive a message,
and verifies the expected payload arrived.

NOT A CARGO TEST
-----------------
This script is intentionally NOT wired into `cargo test`. Making
`cargo test` depend on externally-installed tools (paho-mqtt) would break
CI on any machine without them and could be misread as an undisclosed
runtime dependency by a hackathon judge. See DECISIONS.md,
Decision 12. Run this manually after building:

    python tests/interop/paho_client.py

EXIT CODES
----------
0 -- pass, or paho-mqtt not installed (skip -- not a failure)
1 -- test ran but observed a protocol failure
"""

import sys
import os
import subprocess
import time
import socket
import threading
import platform

# ---------------------------------------------------------------------------
# Skip guard: require paho-mqtt
# ---------------------------------------------------------------------------
try:
    import paho.mqtt.client as mqtt
except ImportError:
    print("SKIP: paho-mqtt Python package not found.")
    print("      Install it to run this interop verification:")
    print("        pip install paho-mqtt")
    sys.exit(0)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BROKER_HOST = "127.0.0.1"
BROKER_PORT = 18831          # Distinct from the mosquitto.sh port (18830)
TEST_TOPIC  = "blitz/interop/paho-smoke"
TEST_PAYLOAD = f"hello-from-paho-{os.getpid()}"
TIMEOUT_SECS = 10
SUBSCRIBE_SETTLE_SECS = 0.4  # Time to let SUBSCRIBE/SUBACK complete before publishing

# ---------------------------------------------------------------------------
# Locate project root (two levels up from this script: tests/interop/ -> root)
# ---------------------------------------------------------------------------
SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(SCRIPT_DIR))

# ---------------------------------------------------------------------------
# Locate the broker binary
# ---------------------------------------------------------------------------
if platform.system() == "Windows":
    BROKER_BIN = os.path.join(PROJECT_ROOT, "target", "release", "blitzbroker.exe")
else:
    BROKER_BIN = os.path.join(PROJECT_ROOT, "target", "release", "blitzbroker")

if not os.path.isfile(BROKER_BIN):
    print(f"INFO: {BROKER_BIN} not found — building (release)...")
    result = subprocess.run(
        ["cargo", "build", "--release"],
        cwd=PROJECT_ROOT,
        check=False,
    )
    if result.returncode != 0:
        print("FAIL: cargo build --release failed. Cannot run interop test.")
        sys.exit(1)

if not os.path.isfile(BROKER_BIN):
    print(f"FAIL: Broker binary not found after build: {BROKER_BIN}")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Start the broker subprocess
# ---------------------------------------------------------------------------
print(f"INFO: Starting blitzbroker on {BROKER_HOST}:{BROKER_PORT} ...")
broker_proc = subprocess.Popen(
    [BROKER_BIN, "--host", BROKER_HOST, "--port", str(BROKER_PORT)],
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
)

def stop_broker():
    """Terminate the broker subprocess. Safe to call multiple times."""
    try:
        broker_proc.terminate()
        broker_proc.wait(timeout=3)
    except Exception:
        try:
            broker_proc.kill()
        except Exception:
            pass

# ---------------------------------------------------------------------------
# Wait for the broker to accept TCP connections
# ---------------------------------------------------------------------------
print("INFO: Waiting for broker to become ready ...")
deadline = time.monotonic() + TIMEOUT_SECS
ready = False
while time.monotonic() < deadline:
    try:
        with socket.create_connection((BROKER_HOST, BROKER_PORT), timeout=0.5):
            ready = True
            break
    except OSError:
        time.sleep(0.1)

if not ready:
    stop_broker()
    print(f"FAIL: Broker did not become ready within {TIMEOUT_SECS} seconds.")
    sys.exit(1)

print("INFO: Broker is ready.")

# ---------------------------------------------------------------------------
# Subscribe via paho-mqtt, then publish, then verify delivery
# ---------------------------------------------------------------------------
received_payloads = []
receive_event = threading.Event()

def on_connect_sub(client, userdata, flags, rc):
    if rc == 0:
        client.subscribe(TEST_TOPIC, qos=0)
    else:
        print(f"WARN: subscriber connect failed with rc={rc}")

def on_message(client, userdata, msg):
    received_payloads.append(msg.payload.decode("utf-8", errors="replace"))
    receive_event.set()

# -- Subscriber client -------------------------------------------------------
sub_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id="blitz-interop-sub", protocol=mqtt.MQTTv311)
sub_client.on_connect = on_connect_sub
sub_client.on_message = on_message

try:
    sub_client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
except Exception as e:
    stop_broker()
    print(f"FAIL: Subscriber could not connect to broker: {e}")
    sys.exit(1)

sub_client.loop_start()

# Allow time for SUBSCRIBE/SUBACK to complete before publishing.
time.sleep(SUBSCRIBE_SETTLE_SECS)

# -- Publisher client --------------------------------------------------------
pub_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id="blitz-interop-pub", protocol=mqtt.MQTTv311)

try:
    pub_client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
except Exception as e:
    sub_client.loop_stop()
    sub_client.disconnect()
    stop_broker()
    print(f"FAIL: Publisher could not connect to broker: {e}")
    sys.exit(1)

pub_client.loop_start()
pub_client.publish(TEST_TOPIC, payload=TEST_PAYLOAD, qos=0)

# Wait for the message to be received by the subscriber.
arrived = receive_event.wait(timeout=float(TIMEOUT_SECS))

pub_client.loop_stop()
pub_client.disconnect()
sub_client.loop_stop()
sub_client.disconnect()
stop_broker()

# ---------------------------------------------------------------------------
# Report result
# ---------------------------------------------------------------------------
if arrived and received_payloads and received_payloads[0] == TEST_PAYLOAD:
    print("PASS: paho-mqtt round-trip OK.")
    print(f'      Published: "{TEST_PAYLOAD}"')
    print(f'      Received:  "{received_payloads[0]}"')
    sys.exit(0)
else:
    received_str = received_payloads[0] if received_payloads else "(nothing)"
    print("FAIL: paho-mqtt round-trip FAILED.")
    print(f'      Published: "{TEST_PAYLOAD}"')
    print(f'      Received:  "{received_str}"')
    sys.exit(1)
