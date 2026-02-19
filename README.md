# Bid Velocity Feature — Real-Time Feature Engineering

## Feature: `bid_velocity_last_10_min`

### Meaning
Number of `bid_placed` events per minute for a given user over the last 10 minutes.

### Computation Logic
```
bid_velocity_last_10_min = count(bid_placed events in last 10 mins) / 10
```

### Raw Events Needed
- `bid_placed` — only placed bids are counted; `bid_cancelled` events are filtered out.

### State Management
The stateful processor (`BidVelocityProcessor`) maintains two pieces of per-user state:

1. **`bidCount`** (ValueState) — a running total of bids currently within the 10-minute window.
2. **`timersPerBidCount`** (MapState) — maps each expiry timestamp to the number of bids that will expire at that time.

### How It Works

1. **New bid arrives** (`handleInputRows`):
   - Compute an expiry time = event_time + 10 minutes.
   - Register an event-time timer at the expiry time (if not already registered for that instant).
   - Increment the per-bucket count in `timersPerBidCount` for that expiry time.
   - Increment the running `bidCount`.
   - Compute velocity = `bidCount / 10` and emit an output record.

2. **Timer fires** (`handleExpiredTimer`):
   - Look up how many bids expire at this timestamp from `timersPerBidCount`.
   - Subtract that count from the running `bidCount`.
   - Remove the expired bucket from `timersPerBidCount`.
   - Compute velocity = `bidCount / 10` and emit an updated output record.

This ensures the feature value is always up-to-date: it increases when new bids arrive and decreases when old bids fall outside the 10-minute window.

### Input Format
```json
{
  "event_type": "bid_placed",
  "user_id": "u123",
  "auction_id": "a456",
  "bid_amount": 150.0,
  "timestamp": "2026-03-10T10:21:00Z"
}
```

### Output Format
```json
{
  "user_id": "u123",
  "event_time": "2026-03-10T10:21:00.000Z",
  "bid_count": 1,
  "bid_velocity_last_10_min": 0.1,
  "is_timer_event": false
}
```

### How to Run

```
PS C:\CODE\class> docker compose exec spark-master bash -lc "
>> mkdir -p /tmp/.ivy2 && \
>> /opt/spark/bin/spark-submit \
>> --master spark://spark-master:7077 \
>> --deploy-mode client \
>> --conf spark.jars.ivy=/tmp/.ivy2 \
>> --conf spark.sql.streaming.stateStore.providerClass=org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider \
>> --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0 \
>> --jars /opt/spark/jars/rocksdb-state-store_2.13-4.1.0.jar \
>> /opt/spark/work-dir/main.py
>> "
```
and 

```bash
docker-compose up --build
```
Then produce sample events to the `input_topic` Kafka topic. Output features are written to the `output_topic` Kafka topic.
