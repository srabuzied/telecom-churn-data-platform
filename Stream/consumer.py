"""
Telecom Churn Detection — PySpark Structured Streaming Pipeline
Production-grade rewrite v3.

Architecture:
  Kafka  ──►  3 independent raw streams  ──►  JDBC + GCS  (unchanged, working)
                                          ──►  churn_risk stream  ──►  JDBC + GCS

  Churn stream design
  -------------------
  The 4th query subscribes to ALL THREE Kafka topics simultaneously via a
  comma-separated subscribe list.  Inside foreachBatch the batch DataFrame
  already contains rows from all three topics (discriminated by a "topic"
  column that Kafka adds automatically).  We split, apply the latest-per-
  customer logic, join in pure batch context, score, and write.

  Why this is correct
  -------------------
  • No stream-stream join → no unbounded state / OOM / Py4JException.
  • A foreachBatch callback receives a plain DataFrame — all Spark batch
    operations (join, groupBy, window) are legal and safe inside it.
  • Checkpointing + watermarks on the raw streams give fault-tolerance.
  • The churn query also checkpoints independently.

ROOT CAUSES WHY churn_risk WAS EMPTY
--------------------------------------
  1. compute_churn_from_tables() was defined but NEVER called.
  2. There was NO streaming query whose foreachBatch invoked compute_churn_score().
  3. Stream-stream joins would have produced OOM/Py4J errors before any row
     could be written.
  4. event_time was replaced with current_timestamp() / fixed literal,
     discarding the real producer timestamp.
  5. JDBC subquery used SQL-Server-specific DATEADD/GETDATE syntax.

FIXES IN v3
-----------
  • compute_churn_score() now uses col("event_time") directly — the real
    producer timestamp from Kafka is preserved in the output.
  • virtual_time variable and to_timestamp(lit(...)) removed entirely.
  • TimestampType added to imports (required for flat_schema in write_churn).
  • event_time carried through the join via calls_latest alias so it is
    available when compute_churn_score() selects it.

Spark-submit example
--------------------
spark-submit \\
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \\
  --conf spark.sql.streaming.checkpointLocation=/checkpoints \\
  telecom_streaming_pipeline.py
"""

import logging
from typing import Dict, List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType
)
from pyspark.sql.functions import (
    col, from_json, to_timestamp,
    when, lit, coalesce
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("TelecomPipeline")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = "kafka:29092"
JDBC_URL = (
    "jdbc:sqlserver://192.168.1.3:1433;"
    "databaseName=TelecomStreaming;"
    "encrypt=false;"
    "trustServerCertificate=true"
)
JDBC_PROPS = {
    "user":          "spark_user",
    "password":      "Spark123!",       # Use Secrets Manager / Vault instead
    "driver":        "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "batchsize":     "1000",
    "numPartitions": "4",
    "loginTimeout":  "30",
}
GCS_BASE        = "gs://telecom-churn-data/silver/Data_From_Stream"
CHECKPOINT_BASE = "gs://telecom-churn-data/checkpoints"

TOPIC_CALLS   = "customer_care_calls"
TOPIC_NETWORK = "network_events"
TOPIC_USAGE   = "usage_events"

# ---------------------------------------------------------------------------
# SparkSession
# ---------------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("TelecomChurnPipeline")
    .config("spark.hadoop.fs.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile",
            "/opt/spark/conf/spark-gcs-key.json")
    .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------
call_schema = StructType([
    StructField("customer_id",       StringType()),
    StructField("anger_rate",        IntegerType()),
    StructField("issue_type",        StringType()),
    StructField("call_duration_sec", IntegerType()),
    StructField("resolved",          StringType()),
    StructField("event_time",        StringType()),
])

network_schema = StructType([
    StructField("customer_id",     StringType()),
    StructField("signal_strength", IntegerType()),
    StructField("dropped_calls",   IntegerType()),
    StructField("internet_speed",  IntegerType()),
    StructField("outage_flag",     IntegerType()),
    StructField("event_time",      StringType()),
])

usage_schema = StructType([
    StructField("customer_id",       StringType()),
    StructField("voice_minutes",     IntegerType()),
    StructField("data_usage_mb",     IntegerType()),
    StructField("sms_count",         IntegerType()),
    StructField("package_usage_pct", IntegerType()),
    StructField("event_time",        StringType()),
])

# ---------------------------------------------------------------------------
# Kafka reader helpers
# ---------------------------------------------------------------------------

def read_kafka_topic(topic: str, schema: StructType) -> DataFrame:
    """
    Read a single Kafka topic as a structured stream.
    Adds a 10-minute watermark for state management.
    """
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 50_000)
        .option("kafka.value.deserializer.encoding", "UTF8")
        .load()
    )
    return (
        raw
        .select(from_json(col("value").cast("string"), schema).alias("d"))
        .select("d.*")
        .withColumn("event_time", to_timestamp("event_time"))
        .withWatermark("event_time", "10 minutes")
    )


def read_kafka_multi_topic(
    topics: List[str],
    schemas: Dict[str, StructType],
) -> DataFrame:
    """
    Subscribe to multiple Kafka topics in a single readStream.
    The Kafka 'topic' column is used to route each row to the correct schema.
    Each schema is parsed independently; columns absent for a given topic
    are filled with NULL via coalesce.

    This is the foundation of the churn stream: one stream, all three topics,
    no stream-stream join required.
    """
    topic_csv = ",".join(topics)
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic_csv)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 50_000)
        .option("kafka.value.deserializer.encoding", "UTF8")
        .load()
    )

    # Parse each topic separately using its own schema, then union all frames
    parsed_frames = []
    for topic, schema in schemas.items():
        parsed = (
            raw
            .filter(col("topic") == topic)
            .select(
                from_json(col("value").cast("string"), schema).alias("d"),
                col("topic"),
            )
            .select("d.*", "topic")
            .withColumn("event_time", to_timestamp("event_time"))
        )
        parsed_frames.append(parsed)

    # Build a super-schema that is the union of all column names
    all_columns: set = set()
    for df in parsed_frames:
        all_columns.update(df.columns)
    all_columns.discard("topic")
    all_columns.discard("event_time")

    unified_frames = []
    for df in parsed_frames:
        existing = set(df.columns)
        missing  = all_columns - existing
        out = df
        for c in missing:
            out = out.withColumn(c, lit(None).cast(IntegerType()))
        unified_frames.append(out)

    combined = unified_frames[0]
    for df in unified_frames[1:]:
        combined = combined.unionByName(df, allowMissingColumns=True)

    return combined.withWatermark("event_time", "10 minutes")

# ---------------------------------------------------------------------------
# Churn score computation  (pure function — no I/O, fully unit-testable)
# ---------------------------------------------------------------------------

def compute_churn_score(df: DataFrame) -> DataFrame:
    """
    Compute churn_score and risk_level from a flat DataFrame that has columns
    from all three source schemas.

    Expected columns (all nullable — missing data scores 0):
        anger_rate, resolved, dropped_calls, outage_flag, package_usage_pct

    Scoring weights
    ---------------
    anger_rate > 7          : +30
    resolved == "No"        : +20
    dropped_calls > 2       : +20
    outage_flag == 1        : +15
    package_usage_pct < 20  : +15
    Max possible score      : 100

    Risk levels
    -----------
    >= 60  →  High
    >= 30  →  Medium
    <  30  →  Low

    Output columns
    --------------
    customer_id, churn_score, risk_level, event_time

    FIX (v3)
    --------
    event_time is now taken directly from col("event_time") — the real
    producer timestamp carried through from Kafka.
    current_timestamp(), scored_at, virtual_time, and to_timestamp(lit(...))
    are all removed. The producer's original 2026-05-31 HH:MM:SS timestamps
    are preserved exactly in the churn output.
    """
    return (
        df
        # Coalesce NULLs to safe default values so missing stream data
        # does not poison the arithmetic via NULL propagation.
        .withColumn("_anger",   coalesce(col("anger_rate"),        lit(0)))
        .withColumn("_resolvd", coalesce(col("resolved"),          lit("Yes")))
        .withColumn("_drops",   coalesce(col("dropped_calls"),     lit(0)))
        .withColumn("_outage",  coalesce(col("outage_flag"),       lit(0)))
        .withColumn("_pkguse",  coalesce(col("package_usage_pct"), lit(100)))
        .withColumn(
            "churn_score",
            when(col("_anger")   > 7,    30).otherwise(0)
            + when(col("_resolvd") == "No", 20).otherwise(0)
            + when(col("_drops")   > 2,     20).otherwise(0)
            + when(col("_outage")  == 1,    15).otherwise(0)
            + when(col("_pkguse")  < 20,    15).otherwise(0),
        )
        .withColumn(
            "risk_level",
            when(col("churn_score") >= 60, "High")
            .when(col("churn_score") >= 30, "Medium")
            .otherwise("Low"),
        )
        .select(
            "customer_id",
            "churn_score",
            "risk_level",
            # ✔ Real producer timestamp — preserved from Kafka source event.
            # The event_time column arrives from calls_latest via the join
            # in write_churn (network and usage event_time cols are dropped
            # before the join to avoid ambiguity).
            col("event_time"),
        )
    )

# ---------------------------------------------------------------------------
# Shared sink helper
# ---------------------------------------------------------------------------

def _write_jdbc_and_gcs(
    batch_df: DataFrame,
    batch_id: int,
    label: str,
    jdbc_table: str,
    gcs_path: str,
) -> None:
    """
    Write one batch to JDBC then GCS with isolated error handling so a
    JDBC failure does not prevent the GCS write and vice versa.
    """
    batch_df.cache()
    try:
        try:
            log.info("[%s] batch_id=%d | JDBC → %s ...", label, batch_id, jdbc_table)
            batch_df.write.jdbc(
                url=JDBC_URL,
                table=jdbc_table,
                mode="append",
                properties=JDBC_PROPS,
            )
            log.info("[%s] batch_id=%d | JDBC SUCCESS.", label, batch_id)
        except Exception as jdbc_err:
            log.error(
                "[%s] batch_id=%d | JDBC FAILED — %s: %s",
                label, batch_id, type(jdbc_err).__name__, jdbc_err,
                exc_info=True,
            )

        try:
            log.info("[%s] batch_id=%d | GCS → %s ...", label, batch_id, gcs_path)
            batch_df.write.mode("append").parquet(gcs_path)
            log.info("[%s] batch_id=%d | GCS SUCCESS.", label, batch_id)
        except Exception as gcs_err:
            log.error(
                "[%s] batch_id=%d | GCS FAILED — %s: %s",
                label, batch_id, type(gcs_err).__name__, gcs_err,
                exc_info=True,
            )
    finally:
        batch_df.unpersist()

# ---------------------------------------------------------------------------
# foreachBatch writers — raw streams
# ---------------------------------------------------------------------------

def write_calls(batch_df: DataFrame, batch_id: int) -> None:
    count = batch_df.count()
    log.info("[write_calls] batch_id=%d | row_count=%d", batch_id, count)
    if count == 0:
        log.info("[write_calls] batch_id=%d | Empty batch — skipping.", batch_id)
        return
    _write_jdbc_and_gcs(
        batch_df, batch_id,
        label="write_calls",
        jdbc_table="customer_care_calls_tbl",
        gcs_path=f"{GCS_BASE}/customer_care_calls/6-10-2026/",
    )


def write_network(batch_df: DataFrame, batch_id: int) -> None:
    count = batch_df.count()
    log.info("[write_network] batch_id=%d | row_count=%d", batch_id, count)
    if count == 0:
        log.info("[write_network] batch_id=%d | Empty batch — skipping.", batch_id)
        return
    _write_jdbc_and_gcs(
        batch_df, batch_id,
        label="write_network",
        jdbc_table="network_events_tbl",
        gcs_path=f"{GCS_BASE}/network_events/6-10-2026/",
    )


def write_usage(batch_df: DataFrame, batch_id: int) -> None:
    count = batch_df.count()
    log.info("[write_usage] batch_id=%d | row_count=%d", batch_id, count)
    if count == 0:
        log.info("[write_usage] batch_id=%d | Empty batch — skipping.", batch_id)
        return
    _write_jdbc_and_gcs(
        batch_df, batch_id,
        label="write_usage",
        jdbc_table="usage_events_tbl",
        gcs_path=f"{GCS_BASE}/usage_events/6-10-2026/",
    )

# ---------------------------------------------------------------------------
# Churn foreachBatch writer
# ---------------------------------------------------------------------------

def write_churn(batch_df: DataFrame, batch_id: int) -> None:
    """
    ForeachBatch writer for the churn scoring stream.

    Steps
    -----
    1.  Log total rows received and per-topic breakdown.
    2.  Guard on empty batch.
    3.  Split into three sub-DataFrames by topic.
    4.  Deduplicate each to latest record per customer_id.
        event_time is kept on calls_latest only; dropped from network and
        usage before the join to prevent column ambiguity.
    5.  Join (left outer) on customer_id — batch join, 100% safe.
    6.  Log row count after join.
    7.  Compute churn scores (event_time comes from calls_latest).
    8.  Log row count + sample before writing.
    9.  Write to JDBC + GCS via shared helper.
    """
    total = batch_df.count()
    log.info("[write_churn] batch_id=%d | total rows (all topics)=%d", batch_id, total)
    if total == 0:
        log.info("[write_churn] batch_id=%d | Empty batch — skipping.", batch_id)
        return

    # ------------------------------------------------------------------
    # Step 1: Per-topic row counts (diagnostic)
    # ------------------------------------------------------------------
    calls_raw   = batch_df.filter(col("topic") == TOPIC_CALLS)
    network_raw = batch_df.filter(col("topic") == TOPIC_NETWORK)
    usage_raw   = batch_df.filter(col("topic") == TOPIC_USAGE)

    n_calls   = calls_raw.count()
    n_network = network_raw.count()
    n_usage   = usage_raw.count()
    log.info(
        "[write_churn] batch_id=%d | topic breakdown → calls=%d  network=%d  usage=%d",
        batch_id, n_calls, n_network, n_usage,
    )

    if n_calls == 0 and n_network == 0 and n_usage == 0:
        log.warning(
            "[write_churn] batch_id=%d | No rows for any churn topic — skipping.",
            batch_id,
        )
        return

    # ------------------------------------------------------------------
    # Step 2: Deduplicate to latest record per customer_id per topic.
    #
    # event_time is retained on calls_latest so it flows through the join
    # and into compute_churn_score() as the output event_time column.
    # network and usage event_time are dropped before the join to prevent
    # "ambiguous column" errors when compute_churn_score selects event_time.
    # ------------------------------------------------------------------
    calls_latest = (
        calls_raw
        .select(
            "customer_id", "anger_rate", "resolved",
            "call_duration_sec", "issue_type", "event_time",
        )
        .orderBy(col("event_time").desc())
        .dropDuplicates(["customer_id"])
    )

    network_latest = (
        network_raw
        .select(
            "customer_id", "signal_strength", "dropped_calls",
            "internet_speed", "outage_flag",
            # event_time intentionally dropped here — calls event_time is used
        )
        .orderBy(col("event_time").desc())
        .dropDuplicates(["customer_id"])
    )

    usage_latest = (
        usage_raw
        .select(
            "customer_id", "voice_minutes", "data_usage_mb",
            "sms_count", "package_usage_pct",
            # event_time intentionally dropped here — calls event_time is used
        )
        .orderBy(col("event_time").desc())
        .dropDuplicates(["customer_id"])
    )

    log.info(
        "[write_churn] batch_id=%d | distinct customers → calls=%d  network=%d  usage=%d",
        batch_id,
        calls_latest.count(),
        network_latest.count(),
        usage_latest.count(),
    )

    # ------------------------------------------------------------------
    # Step 3: Batch-safe left outer join on customer_id.
    # calls_latest drives the join so event_time is unambiguous.
    # ------------------------------------------------------------------
    joined = (
        calls_latest
        .join(network_latest, on="customer_id", how="left")
        .join(usage_latest,   on="customer_id", how="left")
    )

    joined_count = joined.count()
    log.info(
        "[write_churn] batch_id=%d | rows after join=%d",
        batch_id, joined_count,
    )

    if joined_count == 0:
        log.warning(
            "[write_churn] batch_id=%d | Join produced 0 rows. "
            "Check that customer_id values match across topics.",
            batch_id,
        )
        return

    # ------------------------------------------------------------------
    # Step 4: Score — event_time flows from calls_latest into the output
    # ------------------------------------------------------------------
    churn_df = compute_churn_score(joined)

    pre_write_count = churn_df.count()
    log.info(
        "[write_churn] batch_id=%d | churn records to write=%d",
        batch_id, pre_write_count,
    )

    log.info("[write_churn] batch_id=%d | Sample output:", batch_id)
    for row in churn_df.limit(5).collect():
        log.info("  customer_id=%-12s  score=%-3s  risk=%-6s  event_time=%s",
                 row["customer_id"], row["churn_score"],
                 row["risk_level"],  row["event_time"])

    # ------------------------------------------------------------------
    # Step 5: Write to JDBC + GCS
    # ------------------------------------------------------------------
    _write_jdbc_and_gcs(
        churn_df, batch_id,
        label="write_churn",
        jdbc_table="churn_risk_tbl",
        gcs_path=f"{GCS_BASE}/churn_risk/6-10-2026/",
    )

# ---------------------------------------------------------------------------
# Read raw streams
# ---------------------------------------------------------------------------
calls_df   = read_kafka_topic(TOPIC_CALLS,   call_schema)
network_df = read_kafka_topic(TOPIC_NETWORK, network_schema)
usage_df   = read_kafka_topic(TOPIC_USAGE,   usage_schema)

# Multi-topic stream for churn scoring — one readStream, three topics,
# no stream-stream join anywhere.
churn_input_df = read_kafka_multi_topic(
    topics=[TOPIC_CALLS, TOPIC_NETWORK, TOPIC_USAGE],
    schemas={
        TOPIC_CALLS:   call_schema,
        TOPIC_NETWORK: network_schema,
        TOPIC_USAGE:   usage_schema,
    },
)

# ---------------------------------------------------------------------------
# Start all 4 streaming queries
# ---------------------------------------------------------------------------
query_calls = (
    calls_df.writeStream
    .foreachBatch(write_calls)
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/calls")
    .trigger(processingTime="30 seconds")
    .start()
)
log.info("query_calls started (queryId=%s)", query_calls.id)

query_network = (
    network_df.writeStream
    .foreachBatch(write_network)
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/network")
    .trigger(processingTime="30 seconds")
    .start()
)
log.info("query_network started (queryId=%s)", query_network.id)

query_usage = (
    usage_df.writeStream
    .foreachBatch(write_usage)
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/usage")
    .trigger(processingTime="30 seconds")
    .start()
)
log.info("query_usage started (queryId=%s)", query_usage.id)

# This is the query that was entirely missing from the original pipeline.
# Without it, compute_churn_score() was never called and churn_risk_tbl
# and GCS churn_risk/ were never written.
query_churn = (
    churn_input_df.writeStream
    .foreachBatch(write_churn)
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/churn")
    .trigger(processingTime="30 seconds")
    .start()
)
log.info(
    "query_churn started (queryId=%s) — churn_risk pipeline is ACTIVE.",
    query_churn.id,
)

log.info(
    "All 4 streaming queries running. "
    "Churn scoring is live via query_churn (multi-topic foreachBatch join). "
    "event_time in churn output = real producer timestamp from Kafka."
)

# ---------------------------------------------------------------------------
# Wait for any query to terminate and surface exceptions clearly
# ---------------------------------------------------------------------------
try:
    spark.streams.awaitAnyTermination()
except Exception as e:
    log.error("A streaming query terminated with an error: %s", e, exc_info=True)
    raise
