"""
IoT Spark Sensor Analytics — Synthetic Data Generator
======================================================
Generates a realistic IoT sensor dataset in CSV format.
Simulates temperature and humidity readings from multiple sensors
deployed across building locations, covering a rolling 5-day window.

The generated file (sensor_data.csv) serves as the input for the
main analytics pipeline (main.py).

Usage:
    python3 data_generator.py

Output:
    sensor_data.csv — 1,000 rows of synthetic sensor telemetry

Dependencies:
    - faker  (pip install faker)
"""

import csv
import random
from faker import Faker

# Initialize the Faker instance for realistic timestamp generation
fake = Faker()

# ─────────────────────────────────────────────────────────────────────────────
# Configuration Constants
# Centralizing these values makes it easy to extend the simulation by
# adding new locations or sensor types without modifying the core logic.
# ─────────────────────────────────────────────────────────────────────────────

# Physical deployment zones for the simulated IoT network
# Format: <Building>_<Floor> to enable building/floor-level analytics
LOCATIONS = [
    "BuildingA_Floor1",
    "BuildingA_Floor2",
    "BuildingB_Floor1",
    "BuildingB_Floor2",
]

# Sensor hardware categories — used for device performance comparison
SENSOR_TYPES = ["TypeA", "TypeB", "TypeC"]


def generate_sensor_data(num_records: int = 1000, output_file: str = "sensor_data.csv") -> None:
    """
    Generate a synthetic IoT sensor dataset and write it to a CSV file.

    Each record simulates a single telemetry reading from a deployed sensor
    at a specific building location, capturing environmental conditions
    (temperature and humidity) at a given point in time.

    Schema of the generated CSV:
        sensor_id    (int)   — Unique device identifier in range [1000, 1100]
        timestamp    (str)   — ISO-format datetime string (YYYY-MM-DD HH:MM:SS)
        temperature  (float) — Celsius reading in range [15.0, 35.0]
        humidity     (float) — Humidity percentage in range [30.0, 80.0]
        location     (str)   — One of the LOCATIONS constants
        sensor_type  (str)   — One of the SENSOR_TYPES constants

    Args:
        num_records (int):  Number of sensor readings to generate. Default: 1000.
        output_file (str):  Path for the output CSV file. Default: "sensor_data.csv".

    Returns:
        None. Writes directly to the specified output file.
    """
    # Define the column order for the CSV header
    fieldnames = ["sensor_id", "timestamp", "temperature", "humidity", "location", "sensor_type"]

    # Open the output file in write mode with UTF-8 encoding
    # newline="" is required for csv.DictWriter to handle line endings correctly
    with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        # Write the header row first
        writer.writeheader()

        for _ in range(num_records):
            # Assign a random sensor ID in a realistic IoT device ID range
            sensor_id = random.randint(1000, 1100)

            # Generate a realistic timestamp within the past 5 days
            # Using Faker ensures varied timestamps (not sequential) to
            # simulate real-world sensor reporting irregularities
            timestamp_str = fake.date_time_between(
                start_date="-5d", end_date="now"
            ).strftime("%Y-%m-%d %H:%M:%S")

            # Simulate temperature readings across a realistic indoor range
            # Values outside 18–30°C will be flagged as anomalies in analysis
            temperature_val = round(random.uniform(15.0, 35.0), 2)

            # Simulate relative humidity readings typical for indoor environments
            humidity_val = round(random.uniform(30.0, 80.0), 2)

            # Randomly assign a physical location from the deployment map
            location_val = random.choice(LOCATIONS)

            # Randomly assign a sensor hardware category
            sensor_type_val = random.choice(SENSOR_TYPES)

            # Write the complete record to the CSV
            writer.writerow({
                "sensor_id": sensor_id,
                "timestamp": timestamp_str,
                "temperature": temperature_val,
                "humidity": humidity_val,
                "location": location_val,
                "sensor_type": sensor_type_val,
            })


if __name__ == "__main__":
    # Generate the default dataset of 1,000 records
    generate_sensor_data(num_records=1000, output_file="sensor_data.csv")
    print("sensor_data.csv generated successfully — 1,000 records written.")
