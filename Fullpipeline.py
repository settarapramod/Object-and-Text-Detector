import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions

PROJECT_ID = 'your-project-id'
GCS_BUCKET = 'your-bucket-name'
AVRO_FILE_PATH = f'gs://{GCS_BUCKET}/path/to/your/data.avro'
BQ_STAGING_TABLE = 'your-project.your_dataset.staging_table'
BQ_REPORTING_TABLE = 'your-project.your_dataset.reporting_table'

class LoadToBigQuery(beam.DoFn):
    def process(self, row):
        yield {
            'field1': row['field1'],  # Replace with your actual fields
            'field2': row['field2'],
            # Add more fields as needed
        }

def run():
    # Set up pipeline options
    options = PipelineOptions()
    gcp_options = options.view_as(GoogleCloudOptions)
    gcp_options.project = PROJECT_ID
    gcp_options.region = 'us-central1'  # Set your region
    gcp_options.temp_location = f'gs://{GCS_BUCKET}/temp/'

    with beam.Pipeline(options=options) as pipeline:
        # Step 1: Read Avro from GCS
        avro_data = (
            pipeline
            | 'Read Avro' >> beam.io.ReadFromAvro(AVRO_FILE_PATH)
        )

        # Step 2: Transform and Write to Staging BigQuery Table
        (
            avro_data
            | 'Transform Data' >> beam.ParDo(LoadToBigQuery())
            | 'Write to Staging BQ' >> beam.io.WriteToBigQuery(
                BQ_STAGING_TABLE,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

    # Step 3: Load data from Staging to Reporting Table (outside Beam)
    from google.cloud import bigquery
    client = bigquery.Client()

    query = f"""
    INSERT INTO `{BQ_REPORTING_TABLE}` (field1, field2)
    SELECT field1, field2 FROM `{BQ_STAGING_TABLE}`
    """
    client.query(query).result()
    print("Data loaded from staging to reporting table successfully.")

if __name__ == '__main__':
    run()
