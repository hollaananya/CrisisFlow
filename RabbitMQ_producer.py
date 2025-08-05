import random
import json
import time
import pika
from collections import Counter

# RabbitMQ setup
queue_name = "medium_priority_disasters"
connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
channel = connection.channel()
channel.queue_declare(queue=queue_name, durable=True)

# Target counts per minute
TARGET_COUNTS = {"High": 30, "Medium": 60, "Low": 30}
TOTAL_EVENTS_PER_MINUTE = sum(TARGET_COUNTS.values())

# Track counts dynamically
event_counts = Counter({"High": 0, "Medium": 0, "Low": 0})

def generate_disaster_data():
    disaster_types = ["fire", "flood", "power_outage"]
    affected_zones = ["A", "B", "C", "D", "E"]

    start_time = time.time()

    while True:
        # Reset counts every minute
        if time.time() - start_time >= 60:
            event_counts.clear()
            event_counts.update({"High": 0, "Medium": 0, "Low": 0})
            start_time = time.time()

        # Generate an event with classified severity
        disaster_type = random.choice(disaster_types)
        affected_zone = random.choice(affected_zones)
        event = create_event(disaster_type)
        event["affected_zone"] = affected_zone  # Add affected zone

        # Ensure the event's severity aligns with our target ratio
        if event_counts[event["severity"]] < TARGET_COUNTS[event["severity"]]:
            event_counts[event["severity"]] += 1  # Increment counter

            # Only send Medium-severity disasters to RabbitMQ
            if event["severity"] == "Medium":
                channel.basic_publish(
                    exchange="",
                    routing_key=queue_name,
                    body=json.dumps(event),
                    properties=pika.BasicProperties(delivery_mode=2)
                )
                print(f"\U0001F4E4 Sent: {event}")
            else:
                print(f"❌ Not sent (Severity: {event['severity']}): {event}")

        time.sleep(0.5)  # Adjusted to produce 120 events per minute

# Generate events based on classification functions
def create_event(disaster_type):
    event = {"disaster_type": disaster_type, "timestamp": time.time()}

    if disaster_type == "fire":
        event["affected_area"] = random.uniform(100, 1500)
        event["building_height"] = random.uniform(10, 50)
        event["occupancy_load"] = random.randint(100, 1500)
        event["fire_load"] = random.uniform(200, 700)
        event["fire_growth"] = random.choice(["t^2<75s", "t^2 75-300s", "t^2>300s"])
        event["evacuation_time"] = random.uniform(2, 10)
        event["severity"] = classify_fire(event)
    elif disaster_type == "flood":
        event["rainfall_intensity"] = random.uniform(2.5, 125)
        event["water_level"] = random.uniform(0, 2)
        event["affected_population"] = random.randint(100, 50000)
        event["severity"] = classify_flood(event)
    elif disaster_type == "power_outage":
        event["area_affected"] = random.uniform(5, 60)
        event["population_affected"] = random.randint(1000, 50000)
        event["severity"] = classify_power_outage(event)

    return event

# Severity Classification Functions (unchanged)
def classify_fire(event):
    if event["affected_area"] > 1000 or event["building_height"] > 45 or event["occupancy_load"] > 1000 or event["fire_load"] > 600 or event["fire_growth"] == "t^2<75s" or event["evacuation_time"] > 8:
        return "High"
    elif (300 <= event["affected_area"] <= 1000) or (15 <= event["building_height"] <= 45):
        return "Medium"
    return "Low"

def classify_flood(event):
    if event["rainfall_intensity"] > 124.5 or event["water_level"] > 1.5 or event["affected_population"] > 20000:
        return "High"
    elif event["water_level"] > 0.5:
        return "Medium"
    return "Low"

def classify_power_outage(event):
    if event["area_affected"] > 50 or event["population_affected"] > 30000:
        return "High"
    elif event["area_affected"] > 20:
        return "Medium"
    return "Low"

if __name__ == "__main__":
    generate_disaster_data()
