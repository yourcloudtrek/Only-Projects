import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import window
import csv
from datetime import datetime

def parse_csv(line):
    row = next(csv.reader([line]))
    return {
        "order_id": row[0],
        "item_name": row[1],
        "ordered_date": row[2]
    }

def add_timestamp(record):
    dt = datetime.strptime(record["ordered_date"], "%Y-%m-%dT%H:%M:%S")
    return beam.window.TimestampedValue(record, dt.timestamp())

def format_simple(record, window=beam.DoFn.WindowParam):
    start = window.start.to_utc_datetime()
    
    # Convert to simple window number
    window_id = int(start.second / 10) + 1
    
    return f"Window {window_id} -> Order:{record['order_id']} Item:{record['item_name']}"

options = PipelineOptions(
    runner="DataflowRunner",
    project="triple-nectar-494911-q1",
    region="us-central1",
    temp_location="gs://dataflow-concepts-demo/temp",
    staging_location="gs://dataflow-concepts-demo/staging",
    job_name="windowing-demo-simple-dataflow"
)

with beam.Pipeline(options=options) as p:

    events = (
        p
        | "ReadCSV" >> beam.io.ReadFromText(
            "gs://dataflow-concepts-demo/input/orders.csv",
            skip_header_lines=1
        )
        | "ParseCSV" >> beam.Map(parse_csv)
        | "AddTimestamp" >> beam.Map(add_timestamp)
    )

    windowed = (
        events
        | "Windowing" >> beam.WindowInto(window.FixedWindows(10))
        | "Format" >> beam.Map(format_simple)
    )

    windowed | "WriteToGCS" >> beam.io.WriteToText(
        "gs://dataflow-concepts-demo/output/windowed_orders",
        file_name_suffix=".txt",
        shard_name_template="",
        
    )