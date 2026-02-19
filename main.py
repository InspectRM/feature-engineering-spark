import datetime
from pyspark.sql import functions as fn
from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle
from pyspark.sql.types import (
    LongType, StructType, StructField, StringType,
    TimestampType, Row, BooleanType, DoubleType,
)
from typing import Iterator
from pyspark.sql.session import SparkSession


# Feature configuration
AGGREGATION_WINDOW_MINUTES = 10

jobName = "BidVelocityRTMAggregator"

spark = (SparkSession.builder
         .appName(jobName)
         .getOrCreate())

spark.conf.set("spark.sql.shuffle.partitions", "2")
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "1")
spark.conf.set("spark.sql.statestore.providerClass",
               "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")


# Schema for raw auction events coming from Kafka
input_schema = StructType([
    StructField("event_type", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("auction_id", StringType(), True),
    StructField("bid_amount", DoubleType(), True),
    StructField("timestamp", StringType(), False),
])

# Output schema: bid velocity feature per user
output_schema = StructType([
    StructField("user_id", StringType(), False),
    StructField("event_time", TimestampType(), False),
    StructField("output_time", TimestampType(), False),
    StructField("bid_count", LongType(), False),
    StructField("bid_velocity_last_10_min", DoubleType(), False),
    StructField("is_timer_event", BooleanType(), False),
    StructField("timer_value", LongType(), False),
    StructField("is_timer_set", BooleanType(), False),
])


class BidVelocityProcessor(StatefulProcessor):
    """
    Stateful processor that calculates bid velocity (bids per minute)
    for each user over a sliding 10-minute window.

    bid_velocity_last_10_min = count(bid_placed events in last 10 mins) / 10

    Uses event-time timers to expire old bid events from the count
    when they fall outside the 10-minute window.
    """

    def init(self, handle: StatefulProcessorHandle) -> None:
        self.handle = handle

        # Running total of bids in the current window
        self._bidCount = handle.getValueState(
            "bidCount",
            StructType([StructField("bid_count", LongType(), False)])
        )

        # Map state: expiry timestamp -> number of bids expiring at that time
        self._timersPerBidCount = handle.getMapState(
            "timersPerBidCount",
            StructType([StructField("time", TimestampType(), False)]),
            StructType([StructField("bid_count", LongType(), False)])
        )

    def handleInputRows(self, key, rows, timerValues) -> Iterator[Row]:
        is_timer_set = False

        for row in rows:
            # Calculate expiry time (10 minutes from event time)
            expiry_dt = row.event_time + datetime.timedelta(minutes=AGGREGATION_WINDOW_MINUTES)
            expiry_ms = int(expiry_dt.timestamp() * 1000)
            timer_key = (datetime.datetime.fromtimestamp(expiry_ms / 1000.0),)

            # Register timer if first time for this expiry instant
            if not self._timersPerBidCount.containsKey(timer_key):
                self.handle.registerTimer(expiry_ms)
                is_timer_set = True

            # Update per-time bucket contributions
            if self._timersPerBidCount.containsKey(timer_key):
                current_bucket_count = self._timersPerBidCount.getValue(timer_key)[0]
                self._timersPerBidCount.updateValue(timer_key, (current_bucket_count + 1,))
            else:
                self._timersPerBidCount.updateValue(timer_key, (1,))

            # Update running total
            old_bid_count = self._bidCount.get()[0] if self._bidCount.get() is not None else 0
            new_bid_count = old_bid_count + 1
            self._bidCount.update((new_bid_count,))

            # Compute velocity: bids per minute = count / 10
            velocity = new_bid_count / AGGREGATION_WINDOW_MINUTES

            yield Row(
                user_id=key[0],
                event_time=row.event_time,
                output_time=datetime.datetime.now(),
                bid_count=new_bid_count,
                bid_velocity_last_10_min=velocity,
                is_timer_event=False,
                timer_value=expiry_ms,
                is_timer_set=is_timer_set,
            )

    def handleExpiredTimer(self, key, timerValues, expiredTimerInfo) -> Iterator[Row]:
        expiry_time_ms = expiredTimerInfo.getExpiryTimeInMs()
        timer_value = (datetime.datetime.fromtimestamp(expiry_time_ms / 1000.0),)

        # Get expired bid count for this timer bucket
        expired_bid_count = 0
        if self._timersPerBidCount.containsKey(timer_value):
            expired_bid_count = self._timersPerBidCount.getValue(timer_value)[0]

        # Current total
        old_bid_count = self._bidCount.get()[0] if self._bidCount.get() is not None else 0

        # Subtract expired bids from running total
        new_bid_count = max(0, old_bid_count - expired_bid_count)

        if new_bid_count != 0:
            self._bidCount.update((new_bid_count,))
        else:
            self._bidCount.clear()

        # Clean per-time bucket
        if self._timersPerBidCount.containsKey(timer_value):
            self._timersPerBidCount.removeKey(timer_value)

        # Compute velocity: bids per minute = count / 10
        velocity = new_bid_count / AGGREGATION_WINDOW_MINUTES

        yield Row(
            user_id=key[0],
            event_time=timer_value[0],
            output_time=datetime.datetime.now(),
            bid_count=new_bid_count,
            bid_velocity_last_10_min=velocity,
            is_timer_event=True,
            timer_value=expiry_time_ms,
            is_timer_set=True,
        )

    def close(self) -> None:
        pass


# Read raw events from Kafka
raw_data_df = (
    spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("startingOffsets", "earliest")
    .option("subscribe", "input_topic")
    .option("failOnDataLoss", False)
    .load()
)

# Parse incoming JSON events
parsed_df = (
    raw_data_df
    .selectExpr("CAST(value AS STRING) AS raw_data")
    .select(fn.from_json(fn.col("raw_data").cast("string"), input_schema).alias("raw"))
    .select(
        fn.col("raw.event_type").alias("event_type"),
        fn.col("raw.user_id").alias("user_id"),
        fn.to_timestamp(fn.col("raw.timestamp")).alias("event_time"),
        fn.col("raw.auction_id").alias("auction_id"),
        fn.col("raw.bid_amount").alias("bid_amount"),
    )
    .withWatermark("event_time", "1 second")
)

# Filter for bid_placed events only (bid velocity only needs placed bids)
filtered_df = parsed_df.filter(fn.col("event_type") == "bid_placed")

# Apply stateful processing with event-time timers
df = (
    filtered_df
    .groupBy("user_id")
    .transformWithState(
        statefulProcessor=BidVelocityProcessor(),
        outputStructType=output_schema,
        outputMode="update",
        timeMode="eventtime",
    )
)

# Prepare output for Kafka (convert to JSON)
kafka_output_df = (
    df
    .select(
        fn.col("user_id").alias("key"),
        fn.to_json(
            fn.struct(
                fn.col("user_id"),
                fn.col("event_time"),
                fn.col("bid_count"),
                fn.col("bid_velocity_last_10_min"),
                fn.col("is_timer_event"),
            )
        ).alias("value")
    )
)

# Write to Kafka
kafka_query = (
    kafka_output_df
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("topic", "output_topic")
    .option("checkpointLocation", "/tmp/checkpoints/output_topic_bid_velocity_rtm")
    .queryName("bid_velocity_rtm_kafka")
    .start()
)

kafka_query.awaitTermination()
