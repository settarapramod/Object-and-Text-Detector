import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from google.cloud import bigquery

PROJECT_ID = 'your-project-id'
GCS_BUCKET = 'your-bucket-name'
AVRO_FILE_PATH = f'gs://{GCS_BUCKET}/path/to/your/data.avro'
BQ_STAGING_TABLE = 'your-project.your_dataset.staging_table'
BQ_REPORTING_TABLE = 'your-project.your_dataset.reporting_table'

def run():
    # Set up pipeline options
    options = PipelineOptions()
    gcp_options = options.view_as(GoogleCloudOptions)
    gcp_options.project = PROJECT_ID
    gcp_options.region = 'us-central1'  # Set your region
    gcp_options.temp_location = f'gs://{GCS_BUCKET}/temp/'

    # Initialize BigQuery client to dynamically create the schema
    bq_client = bigquery.Client()

    # Step 1: Get the Avro schema and dynamically generate BigQuery schema
    def generate_bq_schema(avro_sample):
        """Generate BigQuery schema based on the first Avro row."""
        fields = [
            bigquery.SchemaField(key, _get_bq_type(value), mode='NULLABLE')
            for key, value in avro_sample.items()
        ]
        return fields

    def _get_bq_type(value):
        """Map Python types to BigQuery types."""
        if isinstance(value, int):
            return 'INTEGER'
        elif isinstance(value, float):
            return 'FLOAT'
        elif isinstance(value, bool):
            return 'BOOLEAN'
        elif isinstance(value, dict):
            return 'RECORD'
        else:
            return 'STRING'  # Default to STRING for unknown types

    with beam.Pipeline(options=options) as pipeline:
        # Step 2: Read Avro data from GCS
        avro_data = (
            pipeline
            | 'Read Avro' >> beam.io.ReadFromAvro(AVRO_FILE_PATH)
        )

        # Step 3: Use a sample record to create the BigQuery schema dynamically
        sample_row = next(avro_data)  # Read one sample row for schema inference
        bq_schema = generate_bq_schema(sample_row)

        # Step 4: Write data to the BigQuery staging table
        (
            avro_data
            | 'Write to Staging BQ' >> WriteToBigQuery(
                BQ_STAGING_TABLE,
                schema=bq_schema,
                write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

    # Step 5: Load data from staging to reporting table
    query = f"""
    INSERT INTO `{BQ_REPORTING_TABLE}` 
    SELECT * FROM `{BQ_STAGING_TABLE}`
    """
    bq_client.query(query).result()
    print("Data successfully loaded from staging to reporting table.")

if __name__ == '__main__':
    run()
