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

    pollution_event = {
        "timestamp": timestamp,
        "location": location,
        "pm25": round(random.uniform(20, 250), 2),
        "pm10": round(random.uniform(50, 300), 2)
    }

    producer.send("pollution_events", pollution_event)
    print("Sent pollution event:", pollution_event)

    time.sleep(2)
