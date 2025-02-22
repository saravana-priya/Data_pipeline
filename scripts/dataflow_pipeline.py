import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import argparse

class FormatData(beam.DoFn):
    def process(self, element):
        import csv
        from io import StringIO
        
        reader = csv.reader(StringIO(element))
        for row in reader:
            yield {
                "id": row[0],
                "transaction_date": row[1],
                "sku_id": row[2],
                "customer_id": row[3],
                "region": row[4],
                "currency": row[5],
                "sales_amount": float(row[6]),
                "sales_quantity": int(row[7])
            }

def run(input_file, output_table, temp_location):
    pipeline_options = PipelineOptions(
        runner="DataflowRunner",
        project="orbital-broker-440004-b3",
        temp_location=temp_location,
        region="asia-south1",
        worker_region="asia-south1"
    )

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "Read CSV" >> beam.io.ReadFromText(input_file, skip_header_lines=1)
            | "Format Data" >> beam.ParDo(FormatData())
            | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                output_table,
                schema="id:STRING, transaction_date:TIMESTAMP, sku_id:STRING, customer_id:STRING, region:STRING, currency:STRING, sales_amount:FLOAT, sales_quantity:INTEGER",
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="GCS path to input CSV file")
    parser.add_argument("--output", required=True, help="BigQuery table in format project:dataset.table")
    parser.add_argument("--temp_location", required=True, help="GCS temp location")
    #args = parser.parse_args()

    # Parse known arguments, pass the rest to Apache Beam
    args, pipeline_args = parser.parse_known_args()

    # Define pipeline options (including Dataflow arguments)
    pipeline_options = PipelineOptions(pipeline_args)

    
    run(args.input, args.output, args.temp_location)
