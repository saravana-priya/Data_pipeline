import json
import time
import google.auth
from google.cloud import storage
from googleapiclient.discovery import build

# Constants
PROJECT_ID = "orbital-broker-440004-b3"
BUCKET_NAME = "orbital-broker-440004-b3_data_south"
DATAFLOW_TEMPLATE = "gs://orbital-broker-440004-b3_data_south/templates/load_data_3"
DATASET_TABLE = "orbital-broker-440004-b3:sales_data.daily_sales_load"
DATA_PREFIX = "daily_sales_data/"  # Directory where CSVs are uploaded
ARCHIVE_PREFIX = "archive/daily_sales_data/"  # Archive directory
REGION = "asia-south1"
POLL_INTERVAL = 120  # Seconds between Dataflow job status checks
MAX_WAIT_TIME = 1800  # Max wait time in seconds (30 minutes)

def trigger_pipeline(event, context):
    """Triggered when a new file is uploaded to GCS inside daily_sales_data/."""
    
    file_name = event["name"]

    # Ensure the uploaded file is inside the specified directory and is a CSV
    if not file_name.startswith(DATA_PREFIX) or not file_name.endswith(".csv"):
        print(f"Ignoring file {file_name}. Not a CSV or outside {DATA_PREFIX}")
        return "Ignored"

    file_path = f"gs://{BUCKET_NAME}/{file_name}"
    archive_path = f"gs://{BUCKET_NAME}/{ARCHIVE_PREFIX}{file_name[len(DATA_PREFIX):]}"  # Preserve subpath

    # Authenticate and create the Dataflow client
    credentials, _ = google.auth.default()
    dataflow = build("dataflow", "v1b3", credentials=credentials)

    # Generate a unique job name
    job_name = f"dataflow-job-{file_name.replace('/', '-').replace('.', '-')}"

    body = {
        "jobName": job_name,
        "parameters": {
            "input": file_path,
            "temp_location": f"gs://{BUCKET_NAME}/temp/",
        },
        "environment": {
            "tempLocation": f"gs://{BUCKET_NAME}/temp/",
            "workerRegion": REGION,
        }
    }

    try:
        response = dataflow.projects().locations().templates().launch(
            projectId=PROJECT_ID,
            location=REGION,
            gcsPath=DATAFLOW_TEMPLATE,
            body=body
        ).execute()
        
        job_id = response["job"]["id"]
        print(f"Successfully triggered Dataflow job '{job_name}' (ID: {job_id}) for {file_name}.")
    
    except Exception as e:
        print(f"Error triggering Dataflow: {str(e)}")
        return "Dataflow Trigger Failed"

    # Wait for Dataflow job to complete
    if wait_for_dataflow_completion(dataflow, job_id):
        # Move file to archive folder after successful Dataflow completion
        try:
            storage_client = storage.Client()
            bucket = storage_client.bucket(BUCKET_NAME)
            blob = bucket.blob(file_name)
            bucket.copy_blob(blob, bucket, new_name=archive_path)
            blob.delete()

            print(f"Moved {file_name} to archive: {archive_path}")
            return "Success"
        except Exception as e:
            print(f"Error moving file to archive: {str(e)}")
            return "File Move Failed"
    else:
        print(f"Dataflow job '{job_id}' did not complete successfully. File will not be archived.")
        return "Dataflow Failed"


def wait_for_dataflow_completion(dataflow, job_id):
    """Polls the Dataflow job status until it reaches a terminal state."""
    start_time = time.time()
    
    while time.time() - start_time < MAX_WAIT_TIME:
        try:
            job = dataflow.projects().locations().jobs().get(
                projectId=PROJECT_ID,
                location=REGION,
                jobId=job_id
            ).execute()
            
            job_state = job["currentState"]
            print(f"Dataflow job {job_id} status: {job_state}")

            if job_state in ["JOB_STATE_DONE"]:
                print(f"Dataflow job {job_id} completed successfully.")
                return True
            elif job_state in ["JOB_STATE_FAILED", "JOB_STATE_CANCELLED"]:
                print(f"Dataflow job {job_id} failed/cancelled.")
                return False

        except Exception as e:
            print(f"Error checking Dataflow job status: {str(e)}")

        time.sleep(POLL_INTERVAL)

    print(f"Dataflow job {job_id} did not complete within the timeout period.")
    return False
