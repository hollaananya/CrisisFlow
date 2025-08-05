#!/usr/bin/env python3
import sys
import time
import json
from datetime import datetime

start_time = time.time()
total_events = 0
total_latency = 0

for line in sys.stdin:
    try:
        event = json.loads(line.strip())
        if event["disaster_type"] == "flood":
            total_events += 1

            # ✅ Convert timestamp string to datetime and calculate latency in seconds
            event_time = datetime.strptime(event["timestamp"], "%Y-%m-%d %H:%M:%S.%f")
            current_time = datetime.utcnow()
            latency = int((current_time - event_time).total_seconds())  # Convert to seconds

            # ✅ Print event processing with latency in seconds
            print(f"🌊 Processed Flood in {event['affected_zone']} | Latency: {latency} sec")

            # ✅ Accumulate for throughput calculation
            total_latency += latency

            # ✅ Print throughput every minute
            processing_time = time.time() - start_time
            if processing_time >= 60:  # Every 60 seconds
                throughput = total_events / processing_time if total_events > 0 else 0
                avg_latency = total_latency / total_events if total_events > 0 else 0

                print(f"🌊 Flood Disaster Update: Events={total_events}, "
                      f"Avg Latency={int(avg_latency)} sec, "
                      f"Throughput={throughput:.2f} events/sec")

                # ✅ Reset time and counters after reporting
                start_time = time.time()
                total_events = 0
                total_latency = 0

    except (json.JSONDecodeError, KeyError):
        continue  # Ignore malformed JSON or missing fields

