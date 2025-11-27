import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

locations = ["City-Center", "Downtown", "NH-48", "Airport", "Sector-21"]

while True:
    # SAME TIMESTAMP FOR BOTH PRODUCERS
    now = datetime.now()
    now = datetime.now()
    rounded_sec = now.second - (now.second % 10)
    timestamp = now.replace(second=rounded_sec, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")


    location = random.choice(locations)

    traffic_event = {
        "timestamp": timestamp,
        "location": location,
        "vehicle_count": random.randint(10, 200),
        "avg_speed": random.randint(10, 80)
    }

    producer.send("traffic_events", traffic_event)
    print("Sent traffic event:", traffic_event)

    time.sleep(2)
