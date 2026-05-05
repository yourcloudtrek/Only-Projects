import apache_beam as beam
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    StandardOptions,
    GoogleCloudOptions,
    SetupOptions
)
import json

# =========================
# CONFIG
# =========================
PROJECT_ID = "triple-nectar-494911-q1"
REGION = "us-central1"

TEMP_LOCATION = "gs://demo-pubsub-filterring-bucket/temp"
STAGING_LOCATION = "gs://demo-pubsub-filterring-bucket/staging"

HIGH_SUB = f"projects/{PROJECT_ID}/subscriptions/high-value-order-sub"
LOW_SUB  = f"projects/{PROJECT_ID}/subscriptions/low-value-order-sub"

HIGH_TABLE = f"{PROJECT_ID}:Dataflow_Demos.high_value_orders"
LOW_TABLE  = f"{PROJECT_ID}:Dataflow_Demos.low_value_orders"

BQ_SCHEMA = "order_id:STRING,item_name:STRING,price:INT64"


# =========================
# SAFE PARSER
# =========================
def safe_parse(message):
    """Decode Pub/Sub message safely and enforce schema types"""
    try:
        data = json.loads(message.decode("utf-8"))

        return {
            "order_id": str(data.get("order_id", "")),
            "item_name": str(data.get("item_name", "")),
            "price": int(float(data.get("price", 0)))  # handles "100" or 100
        }

    except Exception as e:
        # Return a safe fallback record (prevents worker crash)
        return {
            "order_id": "INVALID",
            "item_name": "INVALID",
            "price": 0
        }


# =========================
# PIPELINE OPTIONS
# =========================
options = PipelineOptions()

gcp_options = options.view_as(GoogleCloudOptions)
gcp_options.project = PROJECT_ID
gcp_options.region = REGION
gcp_options.job_name = "pubsub-filtering-pipeline"

gcp_options.temp_location = TEMP_LOCATION
gcp_options.staging_location = STAGING_LOCATION

options.view_as(StandardOptions).streaming = True
options.view_as(StandardOptions).runner = "DataflowRunner"

options.view_as(SetupOptions).save_main_session = True


# =========================
# PIPELINE
# =========================
with beam.Pipeline(options=options) as p:

    # -------------------------
    # HIGH VALUE STREAM
    # -------------------------
    high_stream = (
        p
        | "Read High Sub" >> beam.io.ReadFromPubSub(subscription=HIGH_SUB)
        | "Parse High JSON" >> beam.Map(safe_parse)
    )

    # -------------------------
    # LOW VALUE STREAM
    # -------------------------
    low_stream = (
        p
        | "Read Low Sub" >> beam.io.ReadFromPubSub(subscription=LOW_SUB)
        | "Parse Low JSON" >> beam.Map(safe_parse)
    )

    # -------------------------
    # WRITE HIGH TABLE
    # -------------------------
    high_stream | "Write High BQ" >> beam.io.WriteToBigQuery(
        HIGH_TABLE,
        schema=BQ_SCHEMA,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
    )

    # -------------------------
    # WRITE LOW TABLE
    # -------------------------
    low_stream | "Write Low BQ" >> beam.io.WriteToBigQuery(
        LOW_TABLE,
        schema=BQ_SCHEMA,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
    )