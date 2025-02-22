
import pandas as pd
import random
import uuid
from faker import Faker
from datetime import datetime, timedelta
from google.cloud import storage

def generate_data():
    # Initialize Faker
    fake = Faker()
    # Configurations
    NUM_RECORDS = 1_000_000  # Adjust to generate more/less data
    cur_date = datetime.now().date().strftime("%Y-%m-%d").replace("-","_")
    # Define possible values
    regions = ["North America", "Europe", "Asia", "South America", "Africa", "Australia"]
    currencies = {"North America": "USD", "Europe": "EUR", "Asia": "INR", "South America": "BRL", "Africa": "ZAR", "Australia": "AUD"}
    # Define GCS details
    bucket_name = "orbital-broker-440004-b3_data_south"
    destination_blob_name = f"daily_sales_data/{cur_date}sales_data.csv"

    # Function to generate a random date within the past 5 years
    def random_date():
        """Generate a random timestamp from yesterday."""
        # Get yesterday's date
        yesterday = datetime.now() - timedelta(days=1)
        # Generate a random time within yesterday
        random_time = yesterday.replace(
        hour=random.randint(0, 23), 
        minute=random.randint(0, 59), 
        second=random.randint(0, 59),
        microsecond=random.randint(0, 999999)
        )
    
        return random_time

    # Generate data
    sales_data = []
    for _ in range(NUM_RECORDS):
        id = str(uuid.uuid4())  # Unique Transaction ID
        transaction_date = random_date().strftime("%Y-%m-%d %H:%M:%S")  # Random date-time
        sku_id = f"SKU-{random.randint(1000, 9999)}"  # Random SKU
        customer_id = f"CUST-{random.randint(10000, 99999)}"  # Random Customer ID
        region = random.choice(regions)  # Random region
        currency = currencies[region]  # Currency based on region
        sales_quantity = random.randint(1, 20)  # Number of items sold
        sales_amount = round(sales_quantity * random.uniform(10, 500), 2)  # Random price calculation

        sales_data.append([id, transaction_date, sku_id, customer_id, region, currency, sales_amount, sales_quantity])

    # Convert to Pandas DataFrame
    df = pd.DataFrame(sales_data, columns=["id", "transaction_date", "sku_id", "customer_id", "region", "currency", "sales_amount", "sales_quantity"])


    # Save DataFrame as a temporary CSV file
    temp_csv = "/tmp/temp_sales_data.csv"
    df.to_csv(temp_csv, index=False)

    # Upload CSV to GCS
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(temp_csv)
