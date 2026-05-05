import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import csv

# -----------------------------
# CONFIG
# -----------------------------
VALID_STATUSES = {"OK", "FAILED", "ERROR"}

INPUT_PATH = "gs://dead-letter-queue-demo/input/payments.csv"
DLQ_PATH = "gs://dead-letter-queue-demo/output/dlq/bad_records"
BQ_TABLE = "triple-nectar-494911-q1:Dataflow_Demos.valid_payments"


# -----------------------------
# VALIDATION FUNCTION
# -----------------------------
def classify(line):
    try:
        row = next(csv.reader([line]))

        # must have 5 columns
        if len(row) != 5:
            return ("INVALID", line)

        txn_id, timestamp, amount, currency, status = row

        # amount check
        if amount == "" or not amount.replace(".", "").isdigit():
            return ("INVALID", line)
        
        if currency.strip() == "":
            return ("INVALID", line)

        # status check
        if status not in VALID_STATUSES:
            return ("INVALID", line)

        # ✅ return parsed row (IMPORTANT FIX)
        return ("VALID", row)

    except:
        return ("INVALID", line)


# -----------------------------
# PIPELINE
# -----------------------------
def run():

    options = PipelineOptions(
        runner="DataflowRunner",
        project="triple-nectar-494911-q1",
        region="us-central1",
        temp_location="gs://dead-letter-queue-demo/temp",
        staging_location="gs://dead-letter-queue-demo/staging",
        job_name="payment-dlq-pipeline"
    )

    with beam.Pipeline(options=options) as p:

        results = (
            p
            | "Read CSV" >> beam.io.ReadFromText(INPUT_PATH)
            | "Classify Records" >> beam.Map(classify)
        )

        # -------------------------
        # VALID RECORDS → BIGQUERY
        # -------------------------
        valid = (
            results
            | "FilterValid" >> beam.Filter(lambda x: x and x[0] == "VALID")
            | "ToDict" >> beam.Map(lambda x: dict(zip(
                ["txn_id", "timestamp", "amount", "currency", "status"],
                x[1]
            )))
        )

        valid | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
            table=BQ_TABLE,
            schema={
                "fields": [
                    {"name": "txn_id", "type": "STRING"},
                    {"name": "timestamp", "type": "STRING"},
                    {"name": "amount", "type": "STRING"},
                    {"name": "currency", "type": "STRING"},
                    {"name": "status", "type": "STRING"},
                ]
            },
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )

        # -------------------------
        # INVALID RECORDS → DLQ
        # -------------------------
        (
            results
            | "FilterInvalid" >> beam.Filter(lambda x: x and x[0] == "INVALID")
            | "GetRawInvalid" >> beam.Map(lambda x: x[1])
            | "WriteDLQ" >> beam.io.WriteToText(
                DLQ_PATH,
                file_name_suffix=".csv"
            )
        )


if __name__ == "__main__":
    run()