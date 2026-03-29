"""
IoT Spark Sensor Analytics — Main Analysis Pipeline
=====================================================
This module runs five analytical workloads on IoT sensor data using
Apache PySpark. It reads a pre-generated CSV dataset and produces
one output CSV per analysis task.

Analytical workloads:
    1. Data Ingestion & Basic Exploration
    2. Temperature Filtering & Location Aggregation
    3. Hourly Time-Series Analysis
    4. Sensor Performance Ranking (Window Functions)
    5. Location × Hour Pivot Heatmap

Usage:
    python3 main.py

Dependencies:
    - pyspark (pip install pyspark)
    - sensor_data.csv must exist in the working directory
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, to_timestamp, dense_rank
from pyspark.sql.window import Window


def main():
    # ─────────────────────────────────────────────────────────────────────
    # Initialize Spark Session
    # SparkSession is the entry point for all PySpark functionality.
    # getOrCreate() reuses an existing session if one already exists.
    # ─────────────────────────────────────────────────────────────────────
    spark = SparkSession.builder \
        .appName("IoT Sensor Analytics") \
        .getOrCreate()

    # Suppress verbose Spark INFO logs for cleaner console output
    spark.sparkContext.setLogLevel("WARN")

    # ─────────────────────────────────────────────────────────────────────
    # MODULE 1: Data Ingestion & Basic Exploration
    # Loads the CSV dataset into a Spark DataFrame, registers it as a SQL
    # temporary view, and prints basic statistics to validate the data.
    # ─────────────────────────────────────────────────────────────────────
    print("\n=== MODULE 1: Data Ingestion & Exploration ===")

    # Read the sensor CSV with headers and automatic schema inference
    df = spark.read.csv("sensor_data.csv", header=True, inferSchema=True)

    # Register as a SQL temporary view so we can use Spark SQL queries
    df.createOrReplaceTempView("sensor_readings")

    # Print a sample of the data and record-level statistics
    print("First 5 rows:")
    df.show(5)
    print(f"Total records: {df.count()}")

    print("Distinct sensor locations:")
    # Use Spark SQL for a declarative query across the temporary view
    spark.sql("SELECT DISTINCT location FROM sensor_readings ORDER BY location").show()

    # Persist the first 5 rows as the Task 1 output
    df.limit(5).write.csv("task1_output.csv", header=True, mode="overwrite")

    # ─────────────────────────────────────────────────────────────────────
    # MODULE 2: Temperature Filtering & Location Aggregation
    # Classifies readings as in-range (18–30°C) or out-of-range, then
    # computes per-location averages for temperature and humidity.
    # ─────────────────────────────────────────────────────────────────────
    print("\n=== MODULE 2: Temperature Filtering & Aggregations ===")

    # Filter to keep only readings within the comfortable temperature band
    in_range = df.filter((col("temperature") >= 18) & (col("temperature") <= 30))

    # Derive the count of anomalous readings by subtraction
    out_of_range = df.count() - in_range.count()
    print(f"In-range readings (18–30°C): {in_range.count()}")
    print(f"Out-of-range readings:        {out_of_range}")

    # Group by physical location and compute average temperature & humidity
    # Sort descending so the hottest location appears first
    agg_df = df.groupBy("location") \
        .agg({"temperature": "avg", "humidity": "avg"}) \
        .orderBy("avg(temperature)", ascending=False)

    # Write aggregated results to the Task 2 output CSV
    agg_df.write.csv("task2_output.csv", header=True, mode="overwrite")

    # ─────────────────────────────────────────────────────────────────────
    # MODULE 3: Hourly Time-Series Analysis
    # Parses string timestamps into proper Spark TimestampType, extracts
    # the hour-of-day dimension, and aggregates temperature by hour to
    # identify intraday thermal patterns.
    # ─────────────────────────────────────────────────────────────────────
    print("\n=== MODULE 3: Time-Based Analysis ===")

    # Convert the raw string timestamp column to a proper TimestampType
    # using the exact format pattern from the data generator
    df_time = df.withColumn(
        "timestamp",
        to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")
    )

    # Update the temporary SQL view with the parsed timestamp column
    df_time.createOrReplaceTempView("sensor_readings")

    # Group by hour-of-day and compute average temperature per hour
    # Results sorted descending to highlight peak thermal hours
    hourly_avg = df_time.groupBy(hour("timestamp").alias("hour_of_day")) \
        .agg({"temperature": "avg"}) \
        .orderBy("avg(temperature)", ascending=False)

    # Write time-series results to the Task 3 output CSV
    hourly_avg.write.csv("task3_output.csv", header=True, mode="overwrite")

    # ─────────────────────────────────────────────────────────────────────
    # MODULE 4: Sensor Performance Ranking (Window Functions)
    # Aggregates temperature readings per sensor, then uses a dense_rank()
    # window function to rank sensors by average temperature — surfacing
    # the top 5 hottest-running devices for performance monitoring.
    # ─────────────────────────────────────────────────────────────────────
    print("\n=== MODULE 4: Sensor Performance Ranking ===")

    # Define a window specification ordered by average temperature descending
    # dense_rank() assigns consecutive ranks without gaps (1, 2, 2, 3...)
    window_spec = Window.orderBy(col("avg_temp").desc())

    sensor_ranking = df.groupBy("sensor_id") \
        .agg({"temperature": "avg"}) \
        .withColumnRenamed("avg(temperature)", "avg_temp") \
        .withColumn("rank_temp", dense_rank().over(window_spec)) \
        .limit(5)  # Keep only the top 5 ranked sensors

    # Write the sensor ranking to the Task 4 output CSV
    sensor_ranking.write.csv("task4_output.csv", header=True, mode="overwrite")

    # ─────────────────────────────────────────────────────────────────────
    # MODULE 5: Location × Hour Pivot Heatmap
    # Constructs a 24-column pivot table (one column per hour) across all
    # building locations, creating a temperature heatmap matrix.
    # Uses RDD operations to find the global peak (location, hour) point.
    # ─────────────────────────────────────────────────────────────────────
    print("\n=== MODULE 5: Location-Hour Pivot Heatmap ===")

    # Add a numeric hour-of-day column derived from the parsed timestamp
    df_with_hour = df_time.withColumn("hour_of_day", hour("timestamp"))

    # Pivot on hour_of_day (0–23) to create a wide matrix
    # Each cell contains the average temperature for (location, hour)
    pivot_df = df_with_hour.groupBy("location") \
        .pivot("hour_of_day", list(range(24))) \
        .agg({"temperature": "avg"})

    # Use RDD map operations to find the peak temperature point:
    # Step 1: For each location row, extract all (hour, avg_temp) pairs
    # Step 2: Find the maximum temperature pair per location
    max_row = pivot_df.rdd.map(
        lambda row: (row.location, [(h, row[str(h)]) for h in range(24)])
    ).map(
        lambda x: (x[0], max(x[1], key=lambda y: y[1] if y[1] is not None else -1))
    ).collect()

    # Identify the single global peak across all locations and hours
    max_temp_point = max(max_row, key=lambda x: x[1][1])
    print(
        f"Peak temperature: {max_temp_point[1][1]:.1f}°C  "
        f"at {max_temp_point[0]}, hour {max_temp_point[1][0]:02d}:00"
    )

    # Write the full pivot heatmap to the Task 5 output CSV
    pivot_df.write.csv("task5_output.csv", header=True, mode="overwrite")

    # ─────────────────────────────────────────────────────────────────────
    # Cleanup
    # Always stop the Spark session after all jobs complete to release
    # cluster resources gracefully.
    # ─────────────────────────────────────────────────────────────────────
    spark.stop()
    print("\nAll modules completed successfully. Outputs saved to task*_output.csv/")


if __name__ == "__main__":
    main()
