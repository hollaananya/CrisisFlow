#!/usr/bin/env python3
import sys
import json

for line in sys.stdin:
    try:
        event = json.loads(line.strip())
        if event.get("severity") == "Low":
            # Print only low-severity disasters
            print(json.dumps(event))
    except json.JSONDecodeError:
        continue  # Ignore malformed JSON lines

