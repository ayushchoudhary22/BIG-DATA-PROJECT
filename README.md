Smart City Streaming — Real-time Traffic & Pollution Analytics

A minimal prototype that simulates city traffic and pollution telemetry, publishes them to Kafka, then consumes & processes the streams with Spark Structured Streaming. The processed results are printed to console and persisted into PostgreSQL.

Files
- [traffic_producer.py](traffic_producer.py) — simple Kafka traffic producer that pushes `traffic_events`. Uses [`traffic_producer.producer`](traffic_producer.py) and emits `traffic_event`s with a 10-second-rounded timestamp and location from [`traffic_producer.locations`](traffic_producer.py).
- [pollution_producer.py](pollution_producer.py) — simple Kafka pollution producer that pushes `pollution_events`. Uses [`pollution_producer.producer`](pollution_producer.py) and emits `pollution_event`s with a 10-second-rounded timestamp and location from [`pollution_producer.locations`](pollution_producer.py).
- [streaming_job.py](streaming_job.py) — Spark Structured Streaming job that reads both topics, applies watermarks and 10-second windowed aggregations, performs a windowed join and writes results to console and Postgres. Main items:
  - Spark session instance: [`streaming_job.spark`](streaming_job.py)
  - Schemas: [`streaming_job.traffic_schema`](streaming_job.py), [`streaming_job.pollution_schema`](streaming_job.py)
  - Window aggregations: [`streaming_job.traffic_windowed`](streaming_job.py), [`streaming_job.pollution_windowed`](streaming_job.py)
  - Windowed join: [`streaming_job.joined_stream`](streaming_job.py)
  - Postgres writer function: [`streaming_job.write_to_postgres`](streaming_job.py)
  - Running queries: [`streaming_job.debug_query`](streaming_job.py) (console) and [`streaming_job.query`](streaming_job.py) (foreachBatch to Postgres)
- [smart-city.ipynb](smart-city.ipynb) — example notebook with alternate mock file-source producers, a "fixed" streaming job, and a simple Dash visualization; includes example producer & dash code for a local / Colab-run environment.

Quick start (local dev)
1. Prereqs
   - Java 8+ (for Spark)
   - Apache Spark (>2.4, recommended 3.x)
   - Kafka (running with Zookeeper)
   - PostgreSQL (running locally)
   - Python 3.8+
   - Python libs: pyspark, kafka-python, pandas, pyarrow
     - Quick install:
       ```sh
       pip install pyspark kafka-python pandas pyarrow
       ```

2. Start Kafka + Zookeeper (example with Docker Compose)
   - Use a confluent/community Kafka docker-compose or local install. Ensure `localhost:9092` is reachable by Python & Spark code.

3. Create topics
   ```sh
   # Adjust paths/command per your Kafka distribution
   kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic traffic_events
   kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic pollution_events
   ```

4. Start PostgreSQL & prepare table
   - Example table (matching columns appended by `streaming_job.py`):
   ```sql
   CREATE DATABASE smart_city;
   CREATE TABLE traffic_pollution_analytics (
     timestamp TIMESTAMP,
     location TEXT,
     vehicle_count BIGINT,
     avg_speed DOUBLE PRECISION,
     pm25 DOUBLE PRECISION,
     pm10 DOUBLE PRECISION
   );
   ```
   - Adjust `user` & `password` used in [`streaming_job.write_to_postgres`](streaming_job.py).

5. Run producers
   ```sh
   python pollution_producer.py   # publishes to topic pollution_events
   python traffic_producer.py     # publishes to topic traffic_events
   ```
   - Each script uses `kafka-python` and the [`producer`](traffic_producer.py), [`producer`](pollution_producer.py) variables.

6. Run streaming job
   - Use spark-submit with the Kafka package & Postgres JDBC driver (adjust versions):
   ```sh
   spark-submit \
     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
     --jars /path/to/postgresql-42.2.23.jar \
     streaming_job.py
   ```
   - The job reads from Kafka, aggregates windows (10s fixed windows with 60s watermark), joins by location+window, prints to console via [`streaming_job.debug_query`](streaming_job.py) and writes to Postgres via [`streaming_job.write_to_postgres`](streaming_job.py).
   - Logs are printed; `query.awaitTermination()` prevents the script from exiting.

Notes & behavior
- Producers set the timestamp to a `10s`-rounded value (see both producers) to ensure events from both producers line up in the same stream window.
- Windowing: traffic & pollution are aggregated using 10-second tumbling windows with 60-second watermarking — see [`streaming_job.traffic_windowed`](streaming_job.py) and [`streaming_job.pollution_windowed`](streaming_job.py).
- The streaming job joins the two windowed aggregations using window equality and `location` equality — see [`streaming_job.joined_stream`](streaming_job.py).

Troubleshooting
- Kafka broker unavailable: confirm `localhost:9092` and topic creation.
- Spark & Kafka packages: ensure the package version for spark-sql-kafka-0-10 matches your Spark version.
- JDBC driver not found: add the Postgres JDBC jar to `spark-submit --jars`.
- If console output shows "Unable to load native-hadoop library", this is usually a non-fatal warning.

Optional
- The [smart-city.ipynb](smart-city.ipynb) file provides a Colab-friendly example using filesystem-based mock producers and a Dash dashboard. It also contains a `streaming_job_fixed.py` example used for local Colab runs.
