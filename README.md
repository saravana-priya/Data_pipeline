# GCP Sales Data Pipeline

This repository demonstrates a GCP data pipeline that generates fake sales data, uploads it to Google Cloud Storage (GCS), and ingests it into BigQuery using Cloud Functions and Dataflow.

## Architecture

The pipeline consists of the following components:

1.  *Data Generation:* A Python script generates fake sales data and saves it as a CSV file.
2.  *Workflow Orchestration:* Apache Airflow in Cloud Composer schedules the data generation script to run daily at 10 AM IST.
3.  *Cloud Storage (GCS):* The generated CSV file is uploaded to a GCS bucket.
4.  *Cloud Functions:* A Cloud Function is triggered when a new CSV file is uploaded to the GCS bucket.
5.  *Dataflow:* The Cloud Function triggers a Dataflow job to ingest the data from the CSV file into a BigQuery table.
6.  *BigQuery:* The sales data is stored in a BigQuery table for analysis.

## Prerequisites

* A Google Cloud Platform (GCP) account.
* gcloud CLI installed and configured.
* Terraform installed (optional, for IaC).
* A github account.

## Setup

1.  *Clone the repository:*

    bash
    git clone <repository_url>
    cd <repository_name>
    

2.  *Create a GCS bucket:*

    bash
    gsutil mb gs://<your-bucket-name>
    

3.  *Create a BigQuery dataset:*

    bash
    bq mk <your-project-id>:<your-dataset-name>
    

4.  *Configure Airflow (Cloud Composer):*
    * Create a cloud composer environment.
    * Upload the airflow dag to the dag folder of the composer bucket.
    * Configure the variables within airflow to match your project.

5.  *Deploy the Cloud Function:*

    bash
    gcloud functions deploy <function-name> --runtime python3.9 --trigger-resource gs://<your-bucket-name> --trigger-event google.storage.object.finalize --set-env-vars BUCKET_NAME=<your-bucket-name>,DATASET_NAME=<your-dataset-name>
    

6.  *Deploy the Dataflow job:*
    * The cloud function will trigger the dataflow job.
    * The dataflow job code will need to be stored in a GCS bucket.

7.  *(Optional) Deploy infrastructure using Terraform:*
    * If using Terraform, configure the terraform.tfvars file and run terraform apply.

## Usage

* The Airflow DAG will automatically generate and upload the sales data daily at 10 AM.
* The Cloud Function will trigger the Dataflow job to ingest the data into BigQuery.
* You can query the BigQuery table to analyze the sales data.

## Enhancements

* Implement data validation checks.
* Add error handling and logging.
* Implement Infrastructure as Code (IaC).
* Optimize the Dataflow job.
* Implement CI/CD.

## Author

* [Saravana Priya]

