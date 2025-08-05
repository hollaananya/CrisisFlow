import random
import json
import time
from datetime import datetime

def generate_fire_data(severity):
    if severity == "High":
        affected_area = random.uniform(1000, 1500)
        building_height = random.uniform(45, 50)
        occupancy_load = random.randint(1000, 1500)
        fire_growth = "t^2<75s"
    elif severity == "Medium":
        affected_area = random.uniform(300, 1000)
        building_height = random.uniform(15, 45)
        occupancy_load = random.randint(300, 1000)
        fire_growth = "t^2 75-300s"
    else:  # Low severity
        affected_area = random.uniform(100, 300)
        building_height = random.uniform(10, 15)
        occupancy_load = random.randint(100, 300)
        fire_growth = "t^2>300s"

    return {
        "disaster_type": "fire",
        "affected_area": round(affected_area, 2),
        "building_height": round(building_height, 2),
        "affected_population": occupancy_load,
        "fire_growth": fire_growth,
        "severity": severity,
        "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")
    }

def generate_flood_data(severity):
    if severity == "High":
        rainfall_intensity = random.uniform(124.5, 150)
        water_level = random.uniform(1.5, 2)
        affected_population = random.randint(20000,30000)
    elif severity == "Medium":
        rainfall_intensity = random.uniform(35.6, 64.4)
        water_level = random.uniform(0.5, 1.5)
        affected_population = random.randint(10000,20000)
    else:  # Low severity
        rainfall_intensity = random.uniform(2.5, 35.5)
        water_level = random.uniform(0, 0.5)
        affected_population = random.randint(5000,10000)
        

    return {
        "disaster_type": "flood",
        "rainfall_intensity": round(rainfall_intensity, 2),
        "water_level": round(water_level, 2),
        "affected_population" : affected_population,
        "severity": severity,
        "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")
    }

def generate_power_outage_data(severity):
    if severity == "High":
        area_affected = random.uniform(50, 60)
        population_affected = random.randint(50000, 100000)
    elif severity == "Medium":
        area_affected = random.uniform(10, 50)
        population_affected = random.randint(10000, 50000)
    else:  # Low severity
        area_affected = random.uniform(5, 10)
        population_affected = random.randint(1000, 10000)

    return {
        "disaster_type": "power_outage",
        "area_affected": round(area_affected, 2),
        "affected_population": population_affected,
        "severity": severity,
        "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")
    }

def generate_continuous_events():
    while True:
        events = []
        
        # Generate events with balanced severity distribution
        # High severity (40 events per minute)
        for _ in range(40):
            events.append(generate_fire_data("High"))
            events.append(generate_flood_data("High"))
            events.append(generate_power_outage_data("High"))
            events.append(generate_fire_data("High"))
        
        # Medium severity (20 events per minute)
        for _ in range(40):
            events.append(generate_fire_data("Medium"))
            events.append(generate_flood_data("Medium"))
            events.append(generate_power_outage_data("Medium"))
            events.append(generate_fire_data("Medium"))
        
        # Low severity (40 events per minute)
        for _ in range(80):
            events.append(generate_fire_data("Low"))
            events.append(generate_flood_data("Low"))
            events.append(generate_power_outage_data("Low"))
            events.append(generate_fire_data("Low"))
        
        # Shuffle events to randomize order
        random.shuffle(events)
        
        # Print events at 1.5-second intervals to maintain ~40 events per minute
        for event in events:
            print(json.dumps(event))
            time.sleep(0.375)  # 40 events per minute = 1.5s interval

if __name__ == "__main__":
    generate_continuous_events()
