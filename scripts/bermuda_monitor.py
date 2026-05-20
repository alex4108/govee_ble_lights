#!/usr/bin/env python3
"""Sample Bermuda global metrics during a soak so periodic GATT load from
the color heartbeat can be correlated against advert forwarding health.

Usage (from anywhere):
    python3 scripts/bermuda_monitor.py [--interval 30] [--csv path.csv]

Reads HA_SSH_HOST / HA_ACCESS_TOKEN from environment. Source the homelab
secrets first:

    source ~/repos/alex4108/homelab/util/load-secrets.sh
    python3 scripts/bermuda_monitor.py

What to look for during the heartbeat soak (every 10 min, all 5 H617A
bulbs each open ~500ms GATT sessions across 3 ESP32 proxies):
- visible_device_count should hold steady around its baseline (typically
  >100). A dip below 20 sustained ≥30s is the *real* failure mode —
  active_proxy_count flapping 3↔2↔1 is normal and doesn't indicate
  anything bad on its own (see esp32_ble_proxy_ops.md).
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import signal
import sys
import time
import urllib.error
import urllib.request

ENTITIES = [
    "sensor.bermuda_global_visible_device_count",
    "sensor.bermuda_global_active_proxy_count",
    "sensor.bermuda_global_total_proxy_count",
]

# Below this for ≥2 consecutive samples → loud warning. This is the
# threshold used by automation.bermuda_advertisement_stall (memory:
# esp32_ble_proxy_ops.md), scaled down for tighter signal during soak.
VISIBLE_DEGRADED = 20


def fetch_state(host: str, token: str, entity: str, timeout: float = 5.0) -> str | None:
    req = urllib.request.Request(
        f"http://{host}:8123/api/states/{entity}",
        headers={"Authorization": f"Bearer {token}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.load(resp)
            return data.get("state")
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as exc:
        return f"err:{exc.__class__.__name__}"


def as_int(s: str | None) -> int | None:
    try:
        return int(float(s))  # tolerate "127.0"
    except (TypeError, ValueError):
        return None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--interval", type=int, default=30,
                        help="seconds between samples (default 30)")
    parser.add_argument("--csv", default=None,
                        help="also write CSV log to this path")
    args = parser.parse_args()

    host = os.environ.get("HA_SSH_HOST")
    token = os.environ.get("HA_ACCESS_TOKEN")
    if not host or not token:
        print("HA_SSH_HOST and HA_ACCESS_TOKEN must be exported.", file=sys.stderr)
        print("Source ~/repos/alex4108/homelab/util/load-secrets.sh first.", file=sys.stderr)
        return 2

    csv_writer = None
    csv_file = None
    if args.csv:
        csv_file = open(args.csv, "a", newline="")
        csv_writer = csv.writer(csv_file)
        if csv_file.tell() == 0:
            csv_writer.writerow(["ts_utc", "visible", "active", "total", "status"])

    def cleanup(*_):
        if csv_file:
            csv_file.flush()
            csv_file.close()
        print()
        print("Stopped.")
        sys.exit(0)

    signal.signal(signal.SIGINT, cleanup)
    signal.signal(signal.SIGTERM, cleanup)

    print(f"Sampling every {args.interval}s. Ctrl-C to stop.")
    print(f"Entities: {', '.join(ENTITIES)}")
    if args.csv:
        print(f"CSV log: {args.csv}")
    print()
    print(f"{'timestamp (UTC)':<20} {'visible':>8} {'active':>8} {'total':>6}   status")

    degraded_streak = 0
    while True:
        states = {e: fetch_state(host, token, e) for e in ENTITIES}
        visible = as_int(states[ENTITIES[0]])
        active = as_int(states[ENTITIES[1]])
        total = as_int(states[ENTITIES[2]])

        status_parts: list[str] = []
        if visible is None:
            status_parts.append("visible=N/A")
        elif visible < VISIBLE_DEGRADED:
            degraded_streak += 1
            status_parts.append(f"visible<{VISIBLE_DEGRADED} (streak={degraded_streak})")
        else:
            if degraded_streak > 0:
                status_parts.append(f"recovered (was {degraded_streak} bad)")
            degraded_streak = 0

        if total is not None and active is not None and active < total:
            status_parts.append(f"active<{total} (informational)")

        status = ", ".join(status_parts) if status_parts else "ok"
        ts = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        line = (
            f"{ts:<20} "
            f"{str(visible if visible is not None else '?'):>8} "
            f"{str(active if active is not None else '?'):>8} "
            f"{str(total if total is not None else '?'):>6}   "
            f"{status}"
        )
        if visible is not None and visible < VISIBLE_DEGRADED:
            print(f"\033[31m{line}\033[0m", flush=True)  # red
        elif status_parts and "recovered" not in status_parts[0]:
            print(f"\033[33m{line}\033[0m", flush=True)  # yellow
        else:
            print(line, flush=True)

        if csv_writer:
            csv_writer.writerow([ts, visible, active, total, status])
            csv_file.flush()

        time.sleep(args.interval)


if __name__ == "__main__":
    sys.exit(main())
