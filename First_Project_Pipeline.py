
from google.cloud import bigquery
from google.cloud import storage

project_id = "your-cloud-trek"
bucket_name = "first-datapipeline-bucket-1"
local_file = "./customers.csv"
gcs_file = "customers.csv"
dataset_name = "Dataset_First_Pipeline"
table_name = "Customer_Table"
service_account_json = "path to your json file"

def upload_data_gcs():
    print("Upload file to storage")
    client = storage.Client.from_service_account_json(service_account_json, project = project_id)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(gcs_file)
    blob.upload_from_filename(local_file)
    print("File uploaded success")

def load_bq():
   client_bq =  bigquery.Client.from_service_account_json(service_account_json, project = project_id)
   uri = f"gs://{bucket_name}/{gcs_file}"
   table_id = f"{project_id}.{dataset_name}.{table_name}"
   my_job = bigquery.LoadJobConfig(
       source_format = bigquery.SourceFormat.CSV,
       skip_leading_rows=1
       
   )
   client_bq.load_table_from_uri(uri, table_id, job_config=my_job)

def run_pipeline():
       
       print("Pipeline Started!")
       upload_data_gcs()
       load_bq()
       print("Pipeline Executed Success")

run_pipeline()

