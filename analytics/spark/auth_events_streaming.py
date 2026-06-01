#!/usr/bin/env python3
"""UDB native-auth event stream processor (Apache Spark Structured Streaming).

This is the downstream half of UDB's event-driven native auth services. The
authn/authz/apikey services write domain events into the transactional outbox
(`udb_system.outbox_events`); the UDB CDC engine relays each row to Apache Kafka
on a per-domain topic. This Spark job subscribes to those topics, decodes the
canonical ``EventEnvelope`` (proto ``udb.events.v1.EventEnvelope`` — see
``proto/udb/events/v1/udb_events.proto``), and derives the operational analytics
defined in ``proto/udb/core/analytics``: login/registration volumes, access-deny
rates, and API-key issuance — windowed per tenant.

Wire contract (the JSON value on each Kafka record):
    {
      "event_id":       "<uuid>",
      "event_type":     "udb.authn.user.registered.v1",
      "occurred_at":    "<rfc3339>",
      "tenant_id":      "<tenant>",
      "correlation_id": "<corr>",
      "document_id":    "<aggregate id == partition key>",
      "payload":        { ...domain event fields... }
    }

Run (local against the integration Kafka):
    spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
      analytics/spark/auth_events_streaming.py \
      --bootstrap localhost:9092 \
      --output ./_spark_out/auth_metrics \
      --checkpoint ./_spark_out/_chk

Environment overrides: UDB_KAFKA_BOOTSTRAP, UDB_SPARK_OUTPUT, UDB_SPARK_CHECKPOINT.
"""

from __future__ import annotations

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

# Native auth event topics (exact names from the event protos /
# `src/runtime/service/auth_service/events.rs::topics`). Listed for reference;
# the job subscribes by PATTERN so it tolerates topics that don't yet exist.
AUTHN_TOPICS = [
    "udb.authn.user.registered.v1",
    "udb.authn.user.login.v1",
    "udb.authn.session.revoked.v1",
    "udb.authn.user.locked.v1",
    "udb.authn.user.password.changed.v1",
    "udb.authn.otp.sent.v1",
    "udb.authn.user.status.changed.v1",
    "udb.authn.user.email.verified.v1",
]
AUTHZ_TOPICS = [
    "udb.authz.role.created.v1",
    "udb.authz.role.assigned.v1",
    "udb.authz.role.revoked.v1",
    "udb.authz.role.updated.v1",
    "udb.authz.access.denied.v1",
]
APIKEY_TOPICS = [
    "udb.apikey.created.v1",
    "udb.apikey.revoked.v1",
    "udb.apikey.updated.v1",
]
ALL_TOPICS = AUTHN_TOPICS + AUTHZ_TOPICS + APIKEY_TOPICS

# Java regex matching every native auth topic — used with `subscribePattern` so
# the job consumes existing topics without requiring all of them to be created.
TOPIC_PATTERN = r"udb\.(authn|authz|apikey)\..*"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="UDB auth event Spark consumer")
    parser.add_argument(
        "--bootstrap",
        default=os.environ.get("UDB_KAFKA_BOOTSTRAP", "localhost:9092"),
        help="Kafka bootstrap servers",
    )
    parser.add_argument(
        "--output",
        default=os.environ.get("UDB_SPARK_OUTPUT", "./_spark_out/auth_metrics"),
        help="Output path for the windowed metrics (parquet)",
    )
    parser.add_argument(
        "--checkpoint",
        default=os.environ.get("UDB_SPARK_CHECKPOINT", "./_spark_out/_chk"),
        help="Structured Streaming checkpoint location",
    )
    parser.add_argument(
        "--window",
        default="1 minute",
        help="Tumbling window size for the aggregates",
    )
    parser.add_argument(
        "--console",
        action="store_true",
        help="Also stream the aggregates to the console (debugging)",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        default=os.environ.get("SPARK_RUN_ONCE", "0") in ("1", "true", "yes"),
        help="Process all currently-available records (Trigger.AvailableNow) and "
        "exit — used by the end-to-end integration test for a bounded run.",
    )
    return parser.parse_args()


def build_stream(spark: SparkSession, bootstrap: str, starting_offsets: str = "latest"):
    """Subscribe to the auth topics (by pattern) and extract EventEnvelope fields.

    Extraction uses `get_json_object` rather than a rigid schema so the variable
    per-event `payload` object is tolerated. `tenant_id` lives inside `payload`
    (it is a domain-event field), matching the real envelope produced by
    `OutboxAuthEventSink` and the CDC engine.
    """
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribePattern", TOPIC_PATTERN)
        .option("startingOffsets", starting_offsets)
        .load()
    )
    value = F.col("value").cast("string")
    return (
        raw.select(F.col("topic"), value.alias("value"))
        .withColumn("event_id", F.get_json_object("value", "$.event_id"))
        .withColumn("event_type", F.get_json_object("value", "$.event_type"))
        .withColumn("tenant_id", F.get_json_object("value", "$.payload.tenant_id"))
        .withColumn(
            "event_time",
            F.coalesce(
                F.to_timestamp(F.get_json_object("value", "$.timestamp")),
                F.current_timestamp(),
            ),
        )
    )


def windowed_metrics(events, window: str):
    """Per-tenant, per-event-type counts over a tumbling event-time window.

    This produces the raw feed behind the analytics events
    (`PipelineSnapshotCommittedEvent`, `DailySummaryGeneratedEvent`): e.g. logins
    and registrations per tenant, and access-deny rates for security dashboards.
    """
    return (
        events.withWatermark("event_time", "10 minutes")
        .groupBy(
            F.window("event_time", window).alias("window"),
            F.col("tenant_id"),
            F.col("event_type"),
        )
        .agg(F.count("*").alias("event_count"))
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "tenant_id",
            "event_type",
            "event_count",
        )
    )


def main() -> None:
    args = parse_args()
    spark = (
        SparkSession.builder.appName("udb-auth-events")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # One-shot runs read from the start of the topics so a bounded test sees
    # events produced before the job launched; streaming runs tail new events.
    starting = "earliest" if args.once else "latest"
    events = build_stream(spark, args.bootstrap, starting)
    metrics = windowed_metrics(events, args.window)

    writer = (
        metrics.writeStream.outputMode("append")
        .format("parquet")
        .option("path", args.output)
        .option("checkpointLocation", args.checkpoint)
    )
    if args.once:
        # Trigger.AvailableNow: process everything currently in Kafka, then stop.
        query = writer.trigger(availableNow=True).start()
        if args.console:
            (
                events.writeStream.outputMode("append")
                .format("console")
                .option("truncate", "false")
                .trigger(availableNow=True)
                .start()
                .awaitTermination()
            )
        query.awaitTermination()
        return

    query = writer.start()
    if args.console:
        (
            metrics.writeStream.outputMode("update")
            .format("console")
            .option("truncate", "false")
            .start()
        )
    query.awaitTermination()


if __name__ == "__main__":
    main()
